//! User sign-in, server-to-user events and the watchlist.

use std::fmt;
use std::sync::{Arc, Weak};

use parking_lot::Mutex;
use sonic_rs::{JsonContainerTrait, JsonValueTrait, Value};

use crate::auth::{UserAuthData, UserAuthRequest};
use crate::channel::subscribe;
use crate::client::Shared;
use crate::codec;
use crate::dispatcher::{BindingId, Dispatcher, EventStream};
use crate::error::{Error, Result};
use crate::event::{Event, Payload};

#[derive(Default)]
pub(crate) struct UserState {
    pub requested: bool,
    pub in_flight_epoch: Option<u64>,
    pub user_id: Option<String>,
    pub user_data: Option<Value>,
}

#[derive(Default)]
pub(crate) struct UserShared {
    pub dispatcher: Dispatcher,
    pub watchlist: Dispatcher,
    pub state: Mutex<UserState>,
}

impl UserShared {
    pub(crate) fn user_id(&self) -> Option<String> {
        self.state.lock().user_id.clone()
    }
}

fn server_channel_name(user_id: &str) -> String {
    format!("#server-to-user-{user_id}")
}

/// Start sign-in on the current connection if it was requested and is not
/// already running (connection task or `sign_in`).
pub(crate) fn attempt_sign_in(shared: &Arc<Shared>) {
    let Some(epoch) = shared.connected_epoch() else {
        return;
    };
    {
        let mut state = shared.user.state.lock();
        if !state.requested || state.in_flight_epoch == Some(epoch) {
            return;
        }
        state.in_flight_epoch = Some(epoch);
    }
    let shared = Arc::clone(shared);
    tokio::spawn(async move {
        let result = async {
            let socket_id = shared.socket_id().ok_or(Error::NotConnected)?;
            let auth = authenticate(&shared, socket_id).await?;
            let data = codec::to_json(&sonic_rs::json!({
                "auth": auth.auth,
                "user_data": auth.user_data,
            }))?;
            let message = shared.encode(&shared.prefix.event("signin"), None, &data)?;
            shared.send_frames(epoch, vec![message]).await
        }
        .await;
        if let Err(error) = result {
            {
                let mut state = shared.user.state.lock();
                state.in_flight_epoch = None;
                state.user_id = None;
                state.user_data = None;
            }
            shared.report_error(error);
        }
    });
}

async fn authenticate(shared: &Arc<Shared>, socket_id: String) -> Result<UserAuthData> {
    let request = UserAuthRequest { socket_id };
    if let Some(authenticator) = shared.config.user_authenticator.as_ref() {
        return authenticator.authenticate(request).await;
    }
    #[cfg(feature = "http")]
    if let Some(endpoint) = shared.config.user_auth_endpoint.as_ref() {
        return crate::http::authenticate_user(&shared.http, endpoint, &request).await;
    }
    Err(Error::auth(None, "no user authenticator configured"))
}

/// Handle `signin_success` (connection task).
pub(crate) fn on_signin_success(shared: &Arc<Shared>, event: &Arc<Event>) {
    let payload = event.value();
    let parsed = payload
        .get("user_data")
        .and_then(|raw| raw.as_str())
        .and_then(|raw| sonic_rs::from_str::<Value>(raw).ok());
    let user_id = parsed
        .as_ref()
        .and_then(|data| data.get("id"))
        .and_then(|id| id.as_str())
        .map(str::to_owned);
    let Some(user_id) = user_id else {
        {
            let mut state = shared.user.state.lock();
            state.user_id = None;
            state.user_data = None;
        }
        shared.report_error(Error::auth(None, "signin_success carried no user_data.id"));
        return;
    };
    {
        let mut state = shared.user.state.lock();
        state.user_id = Some(user_id.clone());
        state.user_data = parsed;
    }
    // Subscribe the user's server channel now that the server knows the
    // user (`subscribe` starts it immediately on a live connection).
    subscribe(shared, &server_channel_name(&user_id), None);
    shared.user.dispatcher.emit(event);
}

/// Handle `watchlist_events` (connection task): one event per entry, named
/// after the entry (`online` / `offline`).
pub(crate) fn on_watchlist_events(shared: &Shared, event: &Arc<Event>) {
    let payload = event.value();
    let Some(entries) = payload.get("events").and_then(|events| events.as_array()) else {
        return;
    };
    for entry in entries.iter() {
        let Some(name) = entry.get("name").and_then(|name| name.as_str()) else {
            continue;
        };
        let item = Event::new(name).with_data(Payload::Json(entry.to_string()));
        shared.user.watchlist.emit(&Arc::new(item));
    }
}

/// Reset the per-connection sign-in progress (keeps the request and the
/// last known identity so the next connection signs in again).
pub(crate) fn on_disconnect(shared: &Shared) {
    shared.user.state.lock().in_flight_epoch = None;
}

/// The signed-in user.
#[derive(Clone)]
pub struct User {
    shared: Arc<Shared>,
}

impl fmt::Debug for User {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("User")
            .field("user_id", &self.user_id())
            .finish()
    }
}

impl User {
    pub(crate) fn new(shared: Arc<Shared>) -> Self {
        Self { shared }
    }

    /// Request sign-in. Runs now when connected and again after every
    /// reconnect.
    pub fn sign_in(&self) -> Result<()> {
        if self.shared.config.user_authenticator.is_none()
            && self.shared.config.user_auth_endpoint.is_none()
        {
            return Err(Error::invalid(
                "configure user_auth_endpoint or user_authenticator",
            ));
        }
        self.shared.user.state.lock().requested = true;
        attempt_sign_in(&self.shared);
        Ok(())
    }

    /// The signed-in user id.
    pub fn user_id(&self) -> Option<String> {
        self.shared.user.user_id()
    }

    /// The signed-in user data.
    pub fn user_data(&self) -> Option<Value> {
        self.shared.user.state.lock().user_data.clone()
    }

    /// Bind to events sent to this user (`signin_success` included).
    pub fn bind(
        &self,
        event: &str,
        callback: impl Fn(&Event) + Send + Sync + 'static,
    ) -> BindingId {
        self.shared.user.dispatcher.bind(event, Arc::new(callback))
    }

    /// Bind to every event sent to this user.
    pub fn bind_global(&self, callback: impl Fn(&Event) + Send + Sync + 'static) -> BindingId {
        self.shared.user.dispatcher.bind_global(Arc::new(callback))
    }

    /// Remove a user binding.
    pub fn unbind(&self, id: BindingId) -> bool {
        self.shared.user.dispatcher.unbind(id)
    }

    /// Stream events sent to this user.
    pub fn stream(&self, capacity: usize) -> EventStream {
        stream_on(&self.shared, |user| &user.dispatcher, capacity)
    }

    /// Watchlist (online/offline) events.
    pub fn watchlist(&self) -> Watchlist {
        Watchlist {
            shared: Arc::clone(&self.shared),
        }
    }
}

/// Online/offline notifications for the signed-in user's watchlist.
#[derive(Clone)]
pub struct Watchlist {
    shared: Arc<Shared>,
}

impl fmt::Debug for Watchlist {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Watchlist")
    }
}

impl Watchlist {
    /// Bind to `online` or `offline`. The event data is the entry, e.g.
    /// `{"name":"online","user_ids":["1","2"]}`.
    pub fn bind(
        &self,
        event: &str,
        callback: impl Fn(&Event) + Send + Sync + 'static,
    ) -> BindingId {
        self.shared.user.watchlist.bind(event, Arc::new(callback))
    }

    /// Bind to every watchlist event.
    pub fn bind_global(&self, callback: impl Fn(&Event) + Send + Sync + 'static) -> BindingId {
        self.shared.user.watchlist.bind_global(Arc::new(callback))
    }

    /// Remove a watchlist binding.
    pub fn unbind(&self, id: BindingId) -> bool {
        self.shared.user.watchlist.unbind(id)
    }

    /// Stream watchlist events.
    pub fn stream(&self, capacity: usize) -> EventStream {
        stream_on(&self.shared, |user| &user.watchlist, capacity)
    }
}

fn stream_on(
    shared: &Arc<Shared>,
    pick: fn(&UserShared) -> &Dispatcher,
    capacity: usize,
) -> EventStream {
    let (id, receiver, dropped) = pick(&shared.user).bind_stream(None, capacity);
    let weak: Weak<Shared> = Arc::downgrade(shared);
    EventStream::new(
        id,
        receiver,
        dropped,
        Box::new(move |id| {
            if let Some(shared) = weak.upgrade() {
                pick(&shared.user).unbind(id);
            }
        }),
    )
}
