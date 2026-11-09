//! Channels and subscriptions.

use std::fmt;
use std::sync::{Arc, Weak};

use parking_lot::Mutex;
use serde::Serialize;
use sockudo_ws::Message;
use sonic_rs::{JsonValueTrait, Value};

use crate::auth::{ChannelAuthData, ChannelAuthRequest};
use crate::client::{Command, Shared};
use crate::codec;
use crate::crypto::SharedSecret;
use crate::delta::ChannelDeltaSettings;
use crate::dispatcher::{BindingId, Dispatcher, EventStream};
use crate::error::{Error, Result};
use crate::event::{Event, Payload};
use crate::filter::FilterNode;
use crate::presence::Members;
use crate::protocol::{ANNOTATION_SUBSCRIBE_MODE, ProtocolVersion, strip_internal, strip_platform};
use crate::recovery::Rewind;

/// The type of a channel, derived from its name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChannelKind {
    /// No authorization.
    Public,
    /// `private-*`: authorized by your backend.
    Private,
    /// `presence-*`: authorized, with membership.
    Presence,
    /// `private-encrypted-*`: authorized, payloads end-to-end encrypted.
    PrivateEncrypted,
    /// `#server-to-user-*`: the signed-in user's server channel.
    ServerToUser,
}

impl ChannelKind {
    /// Classify a channel name.
    pub fn from_name(name: &str) -> Self {
        if name.starts_with("private-encrypted-") {
            Self::PrivateEncrypted
        } else if name.starts_with("presence-") {
            Self::Presence
        } else if name.starts_with("private-") {
            Self::Private
        } else if name.starts_with("#server-to-user-") {
            Self::ServerToUser
        } else {
            Self::Public
        }
    }

    /// Whether subscribing needs a signature from your backend.
    pub const fn requires_auth(self) -> bool {
        matches!(
            self,
            Self::Private | Self::Presence | Self::PrivateEncrypted
        )
    }
}

/// A JMESPath delivery predicate evaluated against the bounded V2 message
/// projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilterExpression(pub String);

impl FilterExpression {
    /// A JMESPath expression, e.g. ``data.price >= `100` ``.
    pub fn jmespath(source: impl Into<String>) -> Self {
        Self(source.into())
    }
}

/// Protocol V2 subscription options.
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct SubscribeOptions {
    /// Tag filter evaluated server-side.
    pub filter: Option<FilterNode>,
    /// Deliver only these event names.
    pub events: Option<Vec<String>>,
    /// JMESPath predicate.
    pub expression: Option<FilterExpression>,
    /// Per-subscription delta negotiation.
    pub delta: Option<ChannelDeltaSettings>,
    /// Replay history before live delivery.
    pub rewind: Option<Rewind>,
    /// Receive raw annotation events (`annotation.create` / `annotation.delete`).
    pub annotation_subscribe: bool,
}

impl SubscribeOptions {
    /// Empty options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set a tag filter (builder style).
    pub fn filter(mut self, filter: FilterNode) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Restrict to event names (builder style).
    pub fn events<I, S>(mut self, events: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.events = Some(events.into_iter().map(Into::into).collect());
        self
    }

    /// Set a JMESPath predicate (builder style).
    pub fn expression(mut self, expression: impl Into<String>) -> Self {
        self.expression = Some(FilterExpression::jmespath(expression));
        self
    }

    /// Negotiate delta compression (builder style).
    pub fn delta(mut self, delta: ChannelDeltaSettings) -> Self {
        self.delta = Some(delta);
        self
    }

    /// Replay history on subscribe (builder style).
    pub fn rewind(mut self, rewind: Rewind) -> Self {
        self.rewind = Some(rewind);
        self
    }

    /// Receive raw annotation events (builder style).
    pub fn annotation_subscribe(mut self, enabled: bool) -> Self {
        self.annotation_subscribe = enabled;
        self
    }

    fn is_empty(&self) -> bool {
        self == &Self::default()
    }

    pub(crate) fn validate(&self, protocol: ProtocolVersion) -> Result<()> {
        if protocol == ProtocolVersion::V1 && !self.is_empty() {
            return Err(Error::invalid("subscription options require Protocol V2"));
        }
        if let Some(filter) = self.filter.as_ref() {
            filter
                .validate()
                .map_err(|error| Error::invalid(format!("invalid filter: {error}")))?;
        }
        if let Some(events) = self.events.as_ref()
            && events.iter().any(String::is_empty)
        {
            return Err(Error::invalid("event filter names must not be empty"));
        }
        if self
            .expression
            .as_ref()
            .is_some_and(|expression| expression.0.trim().is_empty())
        {
            return Err(Error::invalid("filter expression must not be empty"));
        }
        if matches!(self.rewind, Some(Rewind::Count(0) | Rewind::Seconds(0))) {
            return Err(Error::invalid("rewind must be greater than zero"));
        }
        Ok(())
    }

    fn write_into(&self, object: &mut sonic_rs::Object) -> Result<()> {
        fn to_value<T: Serialize + ?Sized>(value: &T) -> Result<Value> {
            sonic_rs::to_value(value).map_err(|error| Error::Encode(error.to_string()))
        }
        if self.events.is_some() || self.expression.is_some() {
            let mut filter = sonic_rs::Object::new();
            if let Some(events) = self.events.as_ref() {
                filter.insert("events", to_value(events)?);
            }
            if let Some(tags) = self.filter.as_ref() {
                filter.insert("tags", to_value(tags)?);
            }
            if let Some(expression) = self.expression.as_ref() {
                filter.insert("expression", Value::from(expression.0.as_str()));
            }
            object.insert("filter", Value::from(filter));
        } else if let Some(tags) = self.filter.as_ref() {
            object.insert("tags_filter", to_value(tags)?);
        }
        if let Some(delta) = self.delta.as_ref() {
            object.insert("delta", delta.subscription_value());
        }
        if let Some(rewind) = self.rewind {
            object.insert("rewind", rewind.subscription_value());
        }
        if self.annotation_subscribe {
            object.insert(
                "modes",
                sonic_rs::json!(["SUBSCRIBE", ANNOTATION_SUBSCRIBE_MODE]),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubState {
    Unsubscribed,
    Pending { epoch: u64, cancelled: bool },
    Subscribed,
}

pub(crate) struct ChannelState {
    pub sub: SubState,
    pub options: SubscribeOptions,
    pub subscription_count: Option<u64>,
    pub attach_serial: Option<u64>,
    pub members: Members,
    pub secret: Option<SharedSecret>,
    /// A key-rotation re-authorization is in flight (encrypted channels).
    pub reauthorizing: bool,
}

pub(crate) struct ChannelInner {
    pub name: String,
    pub kind: ChannelKind,
    pub shared: Weak<Shared>,
    pub dispatcher: Dispatcher,
    pub state: Mutex<ChannelState>,
}

impl fmt::Debug for ChannelInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Channel")
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("state", &self.state.lock().sub)
            .finish()
    }
}

/// A channel subscription handle. Cheap to clone.
#[derive(Clone, Debug)]
pub struct Channel {
    inner: Arc<ChannelInner>,
}

impl Channel {
    pub(crate) fn from_inner(inner: Arc<ChannelInner>) -> Self {
        Self { inner }
    }

    pub(crate) fn inner(&self) -> &Arc<ChannelInner> {
        &self.inner
    }

    pub(crate) fn shared(&self) -> Result<Arc<Shared>> {
        self.inner.shared.upgrade().ok_or(Error::Closed)
    }

    /// Channel name.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Channel type.
    pub fn kind(&self) -> ChannelKind {
        self.inner.kind
    }

    /// Whether `subscription_succeeded` has been received on this connection.
    pub fn is_subscribed(&self) -> bool {
        self.inner.state.lock().sub == SubState::Subscribed
    }

    /// Whether a subscribe request is in flight.
    pub fn is_pending(&self) -> bool {
        matches!(self.inner.state.lock().sub, SubState::Pending { .. })
    }

    /// Latest `subscription_count` reported by the server.
    pub fn subscription_count(&self) -> Option<u64> {
        self.inner.state.lock().subscription_count
    }

    /// Serial at which the subscription attached (for gap-free history).
    pub fn attach_serial(&self) -> Option<u64> {
        self.inner.state.lock().attach_serial
    }

    /// Current subscription options.
    pub fn options(&self) -> SubscribeOptions {
        self.inner.state.lock().options.clone()
    }

    /// Presence membership (presence channels only).
    pub fn members(&self) -> Option<Members> {
        (self.inner.kind == ChannelKind::Presence).then(|| self.inner.state.lock().members.clone())
    }

    /// Bind a callback to an event on this channel.
    pub fn bind(
        &self,
        event: &str,
        callback: impl Fn(&Event) + Send + Sync + 'static,
    ) -> BindingId {
        self.inner.dispatcher.bind(event, Arc::new(callback))
    }

    /// Bind a callback to every event on this channel.
    pub fn bind_global(&self, callback: impl Fn(&Event) + Send + Sync + 'static) -> BindingId {
        self.inner.dispatcher.bind_global(Arc::new(callback))
    }

    /// Remove one binding.
    pub fn unbind(&self, id: BindingId) -> bool {
        self.inner.dispatcher.unbind(id)
    }

    /// Remove every binding for an event name.
    pub fn unbind_event(&self, event: &str) {
        self.inner.dispatcher.unbind_event(event);
    }

    /// Remove every binding on this channel.
    pub fn unbind_all(&self) {
        self.inner.dispatcher.unbind_all();
    }

    /// Stream every event on this channel (bounded; see [`EventStream`]).
    pub fn stream(&self, capacity: usize) -> EventStream {
        self.make_stream(None, capacity)
    }

    /// Stream one event name on this channel.
    pub fn stream_event(&self, event: &str, capacity: usize) -> EventStream {
        self.make_stream(Some(event), capacity)
    }

    fn make_stream(&self, event: Option<&str>, capacity: usize) -> EventStream {
        let (id, receiver, dropped) = self.inner.dispatcher.bind_stream(event, capacity);
        let weak = Arc::downgrade(&self.inner);
        EventStream::new(
            id,
            receiver,
            dropped,
            Box::new(move |id| {
                if let Some(inner) = weak.upgrade() {
                    inner.dispatcher.unbind(id);
                }
            }),
        )
    }

    fn client_event_frame<T: Serialize + ?Sized>(
        &self,
        event: &str,
        data: &T,
    ) -> Result<(Arc<Shared>, u64, Message)> {
        if !event.starts_with("client-") {
            return Err(Error::BadEventName(event.to_owned()));
        }
        if self.inner.kind == ChannelKind::PrivateEncrypted {
            return Err(Error::unsupported(
                "client events are not supported on encrypted channels",
            ));
        }
        let shared = self.shared()?;
        let epoch = shared.connected_epoch().ok_or(Error::NotConnected)?;
        if !self.is_subscribed() {
            tracing::warn!(channel = %self.inner.name, "client event triggered before subscription_succeeded");
        }
        let message = shared.encode(event, Some(&self.inner.name), &codec::to_json(data)?)?;
        Ok((shared, epoch, message))
    }

    /// Send a client event (`client-*`). Requires client events to be enabled
    /// for the app and a private or presence channel.
    pub async fn trigger<T: Serialize + ?Sized>(&self, event: &str, data: &T) -> Result<()> {
        let (shared, epoch, message) = self.client_event_frame(event, data)?;
        shared.send_frames(epoch, vec![message]).await
    }

    /// Send a client event without waiting for queue space (usable from
    /// event callbacks).
    pub fn try_trigger<T: Serialize + ?Sized>(&self, event: &str, data: &T) -> Result<()> {
        let (shared, epoch, message) = self.client_event_frame(event, data)?;
        shared.try_send_frames(epoch, vec![message])
    }

    /// Replace this member's `user_info` (presence channels, Protocol V2).
    pub async fn update_presence<T: Serialize + ?Sized>(&self, info: &T) -> Result<()> {
        if self.inner.kind != ChannelKind::Presence {
            return Err(Error::unsupported(
                "presence_update requires a presence channel",
            ));
        }
        let shared = self.shared()?;
        if !shared.config.protocol.is_v2() {
            return Err(Error::unsupported("presence_update requires Protocol V2"));
        }
        let epoch = shared.connected_epoch().ok_or(Error::NotConnected)?;
        let info = sonic_rs::to_value(info).map_err(|error| Error::Encode(error.to_string()))?;
        let body = codec::to_json(&sonic_rs::json!({
            "channel": self.inner.name.as_str(),
            "data": info,
        }))?;
        // On the JSON wire an object `data` is deserialized server-side as
        // structured data, which lifts `channel` out of the payload the
        // presence handler reads. A JSON-encoded string reaches the handler
        // intact. Binary formats carry the object as the `json` kind.
        let data = if shared.config.wire_format.is_binary() {
            body
        } else {
            let text = String::from_utf8(body).map_err(|error| Error::Encode(error.to_string()))?;
            codec::to_json(&text)?
        };
        let message = shared.encode(&shared.prefix.event("presence_update"), None, &data)?;
        shared.send_frames(epoch, vec![message]).await
    }
}

impl ChannelInner {
    fn new(shared: &Arc<Shared>, name: &str) -> Self {
        Self {
            name: name.to_owned(),
            kind: ChannelKind::from_name(name),
            shared: Arc::downgrade(shared),
            dispatcher: Dispatcher::default(),
            state: Mutex::new(ChannelState {
                sub: SubState::Unsubscribed,
                options: SubscribeOptions::default(),
                subscription_count: None,
                attach_serial: None,
                members: Members::default(),
                secret: None,
                reauthorizing: false,
            }),
        }
    }

    fn is_pending_for(&self, epoch: u64) -> bool {
        matches!(self.state.lock().sub, SubState::Pending { epoch: e, .. } if e == epoch)
    }

    fn subscribe_frame(&self, shared: &Shared, auth: Option<&ChannelAuthData>) -> Result<Message> {
        let options = self.state.lock().options.clone();
        let mut object = sonic_rs::Object::new();
        object.insert(
            "auth",
            Value::from(auth.map_or("", |auth| auth.auth.as_str())),
        );
        object.insert("channel", Value::from(self.name.as_str()));
        if let Some(channel_data) = auth.and_then(|auth| auth.channel_data.as_deref()) {
            object.insert("channel_data", Value::from(channel_data));
        }
        options.write_into(&mut object)?;
        let data = codec::to_json(&object)?;
        shared.encode(&shared.prefix.event("subscribe"), None, &data)
    }

    /// Side effects of a successful authorization.
    fn apply_auth(&self, shared: &Shared, auth: &ChannelAuthData) -> Result<()> {
        match self.kind {
            ChannelKind::PrivateEncrypted => {
                let secret = auth.shared_secret.as_deref().ok_or_else(|| {
                    Error::auth(
                        None,
                        format!(
                            "no shared_secret in auth payload for encrypted channel {}",
                            self.name
                        ),
                    )
                })?;
                self.state.lock().secret = Some(SharedSecret::from_base64(secret)?);
            }
            ChannelKind::Presence => {
                let from_channel_data = auth
                    .channel_data
                    .as_deref()
                    .and_then(|raw| sonic_rs::from_str::<Value>(raw).ok())
                    .and_then(|value| {
                        let id = value.get("user_id")?;
                        id.as_str()
                            .map(str::to_owned)
                            .or_else(|| id.as_u64().map(|n| n.to_string()))
                    });
                let my_id = from_channel_data.or_else(|| shared.user.user_id());
                let Some(my_id) = my_id else {
                    return Err(Error::auth(
                        None,
                        format!("invalid auth response for presence channel '{}'", self.name),
                    ));
                };
                self.state.lock().members.set_my_id(Some(my_id));
            }
            _ => {}
        }
        Ok(())
    }

    fn fail_subscription(&self, shared: &Shared, epoch: u64, error: Error) {
        let remove = {
            let mut state = self.state.lock();
            match state.sub {
                SubState::Pending {
                    epoch: e,
                    cancelled,
                } if e == epoch => {
                    state.sub = SubState::Unsubscribed;
                    cancelled
                }
                _ => return,
            }
        };
        if remove {
            shared.channels.write().remove(&self.name);
            return;
        }
        tracing::warn!(channel = %self.name, %error, "subscription failed");
        let status = match &error {
            Error::Auth { status, .. } => *status,
            _ => None,
        };
        let mut data = sonic_rs::json!({"type": "AuthError", "error": error.to_string()});
        if let Some(status) = status {
            data["status"] = Value::from(status);
        }
        let event = Event::new(shared.prefix.event("subscription_error"))
            .with_channel(self.name.clone())
            .with_data(Payload::Json(data.to_string()));
        self.dispatcher.emit(&Arc::new(event));
        shared.report_error(error);
    }

    fn emit_renamed(&self, event: &Arc<Event>, name: String) {
        self.dispatcher.emit(&Arc::new(event.renamed(name)));
    }

    /// Handle an event routed to this channel (connection task).
    pub(crate) fn handle(&self, shared: &Arc<Shared>, event: &Arc<Event>, out: &mut Vec<Message>) {
        let prefix = shared.prefix;
        let name = event.event.as_str();
        if let Some(internal) = strip_internal(name) {
            match internal {
                "subscription_succeeded" => self.on_subscription_succeeded(shared, event, out),
                "subscription_count" => {
                    let count = event
                        .value()
                        .get("subscription_count")
                        .and_then(crate::event::value_to_u64);
                    if count.is_some() {
                        self.state.lock().subscription_count = count;
                    }
                    self.emit_renamed(event, prefix.event("subscription_count"));
                }
                "member_added" | "member_removed" | "presence_update"
                    if self.kind == ChannelKind::Presence =>
                {
                    let payload = event.value();
                    let applied = {
                        let mut state = self.state.lock();
                        match internal {
                            "member_added" => state.members.add(&payload),
                            "member_removed" => state.members.remove(&payload),
                            _ => state.members.update(&payload),
                        }
                    };
                    if applied.is_some() {
                        self.emit_renamed(event, prefix.event(internal));
                    }
                }
                "message" => {
                    let payload = event.value();
                    if payload.get("action").and_then(|v| v.as_str()) == Some("message.summary") {
                        self.emit_renamed(event, "message.summary".to_owned());
                    } else {
                        self.dispatcher.emit(event);
                    }
                }
                "annotation" => {
                    let payload = event.value();
                    match payload.get("action").and_then(|v| v.as_str()) {
                        Some(action) => self.emit_renamed(event, action.to_owned()),
                        None => self.dispatcher.emit(event),
                    }
                }
                _ => self.dispatcher.emit(event),
            }
            return;
        }
        if let Some(platform) = strip_platform(name) {
            if platform == "subscription_error" {
                let mut state = self.state.lock();
                if matches!(state.sub, SubState::Pending { .. }) {
                    state.sub = SubState::Unsubscribed;
                }
            }
            self.dispatcher.emit(event);
            return;
        }
        if self.kind == ChannelKind::PrivateEncrypted {
            self.handle_encrypted(shared, event);
            return;
        }
        self.dispatcher.emit(event);
        if self.kind == ChannelKind::ServerToUser {
            shared.user.dispatcher.emit(event);
        }
    }

    fn on_subscription_succeeded(
        &self,
        shared: &Arc<Shared>,
        event: &Arc<Event>,
        out: &mut Vec<Message>,
    ) {
        let payload = event.value();
        let cancelled = {
            let mut state = self.state.lock();
            let cancelled = matches!(
                state.sub,
                SubState::Pending {
                    cancelled: true,
                    ..
                }
            );
            state.sub = SubState::Subscribed;
            state.attach_serial = payload
                .get("attach_serial")
                .and_then(crate::event::value_to_u64);
            if self.kind == ChannelKind::Presence {
                state.members.apply_subscription(&payload);
            }
            cancelled
        };
        if cancelled {
            // Unsubscribed while pending: finish the unsubscribe now.
            shared.channels.write().remove(&self.name);
            if let Ok(message) = unsubscribe_frame(shared, &self.name) {
                out.push(message);
            }
            return;
        }
        self.emit_renamed(event, shared.prefix.event("subscription_succeeded"));
    }

    fn handle_encrypted(&self, shared: &Arc<Shared>, event: &Arc<Event>) {
        let secret = self.state.lock().secret.clone();
        let decrypted = secret
            .as_ref()
            .ok_or_else(|| Error::Decryption("no shared secret for channel".into()))
            .and_then(|secret| crate::crypto::decrypt(secret, &event.data));
        match decrypted {
            Ok(plaintext) => {
                let mut clear = (**event).clone();
                clear.data = Payload::Text(plaintext);
                self.dispatcher.emit(&Arc::new(clear));
            }
            Err(error) => {
                // The key may have rotated: re-authorize once and retry. Only
                // one re-authorization runs at a time so a burst of
                // undecryptable payloads cannot flood the auth endpoint.
                let Some(channel) = shared.channel(&self.name) else {
                    return;
                };
                {
                    let mut state = self.state.lock();
                    if state.reauthorizing {
                        drop(state);
                        shared.report_error(error);
                        return;
                    }
                    state.reauthorizing = true;
                }
                tracing::debug!(channel = %self.name, %error, "decryption failed; re-authorizing");
                let shared = Arc::clone(shared);
                let event = Arc::clone(event);
                tokio::spawn(async move {
                    let retried = async {
                        let socket_id = shared.socket_id().ok_or(Error::NotConnected)?;
                        let auth = authorize(&shared, &channel.name, socket_id).await?;
                        channel.apply_auth(&shared, &auth)?;
                        let secret = channel
                            .state
                            .lock()
                            .secret
                            .clone()
                            .ok_or_else(|| Error::Decryption("no shared secret".into()))?;
                        crate::crypto::decrypt(&secret, &event.data)
                    }
                    .await;
                    channel.state.lock().reauthorizing = false;
                    match retried {
                        Ok(plaintext) => {
                            let mut clear = (*event).clone();
                            clear.data = Payload::Text(plaintext);
                            channel.dispatcher.emit(&Arc::new(clear));
                        }
                        Err(error) => shared.report_error(error),
                    }
                });
            }
        }
    }
}

pub(crate) fn unsubscribe_frame(shared: &Shared, name: &str) -> Result<Message> {
    let data = codec::to_json(&sonic_rs::json!({ "channel": name }))?;
    shared.encode(&shared.prefix.event("unsubscribe"), None, &data)
}

/// Obtain a channel authorization from the configured handler or endpoint.
pub(crate) async fn authorize(
    shared: &Arc<Shared>,
    channel: &str,
    socket_id: String,
) -> Result<ChannelAuthData> {
    let request = ChannelAuthRequest {
        socket_id,
        channel_name: channel.to_owned(),
    };
    if let Some(authorizer) = shared.config.channel_authorizer.as_ref() {
        return authorizer.authorize(request).await;
    }
    #[cfg(feature = "http")]
    if let Some(endpoint) = shared.config.channel_auth_endpoint.as_ref() {
        return crate::http::authorize_channel(&shared.http, endpoint, &request).await;
    }
    Err(Error::auth(
        None,
        format!("no channel authorizer configured for '{channel}'"),
    ))
}

/// Register (or update) a channel and subscribe when connected.
pub(crate) fn subscribe(
    shared: &Arc<Shared>,
    name: &str,
    options: Option<SubscribeOptions>,
) -> Channel {
    let inner = {
        let mut channels = shared.channels.write();
        Arc::clone(
            channels
                .entry(name.to_owned())
                .or_insert_with(|| Arc::new(ChannelInner::new(shared, name))),
        )
    };
    let start = {
        let mut state = inner.state.lock();
        if let Some(options) = options {
            state.options = options;
        }
        match state.sub {
            SubState::Pending {
                epoch,
                cancelled: true,
            } => {
                state.sub = SubState::Pending {
                    epoch,
                    cancelled: false,
                };
                None
            }
            SubState::Pending { .. } | SubState::Subscribed => None,
            SubState::Unsubscribed => match shared.connected_epoch() {
                Some(epoch) => {
                    state.sub = SubState::Pending {
                        epoch,
                        cancelled: false,
                    };
                    Some(epoch)
                }
                None => None,
            },
        }
    };
    if let Some(epoch) = start {
        start_subscription(shared, &inner, epoch);
    }
    Channel::from_inner(inner)
}

/// Send the subscribe frame for a channel already marked pending.
pub(crate) fn start_subscription(shared: &Arc<Shared>, channel: &Arc<ChannelInner>, epoch: u64) {
    if !channel.kind.requires_auth() {
        match channel.subscribe_frame(shared, None) {
            Ok(message) => shared.enqueue(Command::Frames {
                epoch,
                messages: vec![message],
            }),
            Err(error) => channel.fail_subscription(shared, epoch, error),
        }
        return;
    }
    let shared = Arc::clone(shared);
    let channel = Arc::clone(channel);
    tokio::spawn(async move {
        if let Some(message) = authorized_frame(&shared, &channel, epoch).await {
            let _ = shared.send_frames(epoch, vec![message]).await;
        }
    });
}

/// Authorize a pending channel and build its subscribe frame.
pub(crate) async fn authorized_frame(
    shared: &Arc<Shared>,
    channel: &Arc<ChannelInner>,
    epoch: u64,
) -> Option<Message> {
    let socket_id = shared.socket_id()?;
    let result = async {
        let auth = authorize(shared, &channel.name, socket_id).await?;
        channel.apply_auth(shared, &auth)?;
        channel.subscribe_frame(shared, Some(&auth))
    }
    .await;
    match result {
        Ok(message) if channel.is_pending_for(epoch) => Some(message),
        Ok(_) => None,
        Err(error) => {
            channel.fail_subscription(shared, epoch, error);
            None
        }
    }
}

/// Mark every unsubscribed channel pending for `epoch`. Returns the frames of
/// channels that need no auth, and the channels that do.
pub(crate) fn prepare_resubscribe(
    shared: &Arc<Shared>,
    epoch: u64,
) -> (Vec<Message>, Vec<Arc<ChannelInner>>) {
    let channels: Vec<Arc<ChannelInner>> = shared.channels.read().values().cloned().collect();
    let mut frames = Vec::new();
    let mut needs_auth = Vec::new();
    for channel in channels {
        // The user's server channel is resubscribed after sign-in succeeds.
        if channel.kind == ChannelKind::ServerToUser {
            continue;
        }
        {
            let mut state = channel.state.lock();
            if state.sub != SubState::Unsubscribed {
                continue;
            }
            state.sub = SubState::Pending {
                epoch,
                cancelled: false,
            };
        }
        if channel.kind.requires_auth() {
            needs_auth.push(channel);
        } else {
            match channel.subscribe_frame(shared, None) {
                Ok(message) => frames.push(message),
                Err(error) => channel.fail_subscription(shared, epoch, error),
            }
        }
    }
    (frames, needs_auth)
}

/// Reset subscription state after the connection is lost.
pub(crate) fn disconnect_all(shared: &Shared) {
    let mut forget = Vec::new();
    for channel in shared.channels.read().values() {
        let mut state = channel.state.lock();
        if matches!(
            state.sub,
            SubState::Pending {
                cancelled: true,
                ..
            }
        ) {
            forget.push(channel.name.clone());
        }
        state.sub = SubState::Unsubscribed;
        state.members.reset();
        state.secret = None;
        state.attach_serial = None;
    }
    if !forget.is_empty() {
        let mut channels = shared.channels.write();
        for name in forget {
            channels.remove(&name);
        }
    }
}

/// Unsubscribe and forget a channel.
pub(crate) fn unsubscribe(shared: &Arc<Shared>, name: &str) {
    let Some(channel) = shared.channel(name) else {
        return;
    };
    enum Next {
        Keep,
        Forget,
        ForgetAndSend,
    }
    let next = {
        let mut state = channel.state.lock();
        match state.sub {
            SubState::Pending { epoch, .. } => {
                state.sub = SubState::Pending {
                    epoch,
                    cancelled: true,
                };
                Next::Keep
            }
            SubState::Subscribed => {
                state.sub = SubState::Unsubscribed;
                Next::ForgetAndSend
            }
            SubState::Unsubscribed => Next::Forget,
        }
    };
    match next {
        Next::Keep => {}
        Next::Forget => {
            shared.channels.write().remove(name);
        }
        Next::ForgetAndSend => {
            shared.channels.write().remove(name);
            if let Some(epoch) = shared.connected_epoch()
                && let Ok(message) = unsubscribe_frame(shared, name)
            {
                shared.enqueue(Command::Frames {
                    epoch,
                    messages: vec![message],
                });
            }
        }
    }
    shared.positions.lock().remove(name);
    if let Some(delta) = shared.delta.as_ref() {
        delta.lock().clear_channel(name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::DeltaAlgorithm;
    use crate::filter::Filter;

    #[test]
    fn classifies_channel_names() {
        assert_eq!(ChannelKind::from_name("orders"), ChannelKind::Public);
        assert_eq!(
            ChannelKind::from_name("private-orders"),
            ChannelKind::Private
        );
        assert_eq!(
            ChannelKind::from_name("presence-room"),
            ChannelKind::Presence
        );
        assert_eq!(
            ChannelKind::from_name("private-encrypted-docs"),
            ChannelKind::PrivateEncrypted
        );
        assert_eq!(
            ChannelKind::from_name("#server-to-user-1"),
            ChannelKind::ServerToUser
        );
        assert!(ChannelKind::Presence.requires_auth());
        assert!(!ChannelKind::ServerToUser.requires_auth());
    }

    fn options_json(options: &SubscribeOptions) -> Value {
        let mut object = sonic_rs::Object::new();
        options.write_into(&mut object).unwrap();
        Value::from(object)
    }

    #[test]
    fn writes_subscription_options_like_other_sdks() {
        let tags_only = SubscribeOptions::new().filter(Filter::eq("market", "spot"));
        assert_eq!(
            options_json(&tags_only),
            sonic_rs::json!({"tags_filter": {"key": "market", "cmp": "eq", "val": "spot"}})
        );
        let compound = SubscribeOptions::new()
            .filter(Filter::eq("market", "spot"))
            .events(["price.updated"])
            .expression("data.price >= `100`")
            .delta(ChannelDeltaSettings::with_algorithm(
                DeltaAlgorithm::Xdelta3,
            ))
            .rewind(Rewind::Seconds(30))
            .annotation_subscribe(true);
        assert_eq!(
            options_json(&compound),
            sonic_rs::json!({
                "filter": {
                    "events": ["price.updated"],
                    "tags": {"key": "market", "cmp": "eq", "val": "spot"},
                    "expression": "data.price >= `100`"
                },
                "delta": {"enabled": true, "algorithm": "xdelta3"},
                "rewind": {"seconds": 30},
                "modes": ["SUBSCRIBE", "ANNOTATION_SUBSCRIBE"]
            })
        );
    }

    #[test]
    fn validates_subscription_options() {
        assert!(
            SubscribeOptions::new()
                .validate(ProtocolVersion::V1)
                .is_ok()
        );
        assert!(
            SubscribeOptions::new()
                .rewind(Rewind::Count(5))
                .validate(ProtocolVersion::V1)
                .is_err()
        );
        assert!(
            SubscribeOptions::new()
                .filter(Filter::eq("a", ""))
                .validate(ProtocolVersion::V2)
                .is_err()
        );
        assert!(
            SubscribeOptions::new()
                .events([""])
                .validate(ProtocolVersion::V2)
                .is_err()
        );
        assert!(
            SubscribeOptions::new()
                .rewind(Rewind::Count(0))
                .validate(ProtocolVersion::V2)
                .is_err()
        );
    }
}
