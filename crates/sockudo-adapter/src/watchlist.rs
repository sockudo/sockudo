// src/watchlist/manager.rs
use dashmap::DashMap;
use sockudo_core::error::Result;
use sockudo_core::websocket::SocketId;
use sockudo_protocol::messages::PusherMessage;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct WatchlistEntry {
    pub user_id: String,
    pub watchers: HashSet<String>, // Users watching this user
    pub watching: HashSet<String>, // Users this user is watching
}

#[derive(Default)]
struct AppWatchlistState {
    watchlists: HashMap<String, WatchlistEntry>,
    online_users: HashMap<String, HashSet<SocketId>>,
}

pub struct WatchlistManager {
    // Keep app-level sharding concurrent while serializing the multi-map updates
    // required to maintain watchlist relationships within a single app.
    apps: Arc<DashMap<String, Arc<Mutex<AppWatchlistState>>>>,
}

impl Default for WatchlistManager {
    fn default() -> Self {
        Self::new()
    }
}

impl WatchlistManager {
    pub fn new() -> Self {
        Self {
            apps: Arc::new(DashMap::new()),
        }
    }

    fn app_state(&self, app_id: &str) -> Arc<Mutex<AppWatchlistState>> {
        self.apps
            .entry(app_id.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(AppWatchlistState::default())))
            .clone()
    }

    /// Add a user with their watchlist
    pub async fn add_user_with_watchlist(
        &self,
        app_id: &str,
        user_id: &str,
        socket_id: SocketId,
        watchlist: Option<Vec<String>>,
    ) -> Result<Vec<PusherMessage>> {
        let mut events_to_send = Vec::new();
        let app_state = self.app_state(app_id);
        let mut state = app_state.lock().unwrap();

        let was_offline = {
            let user_sockets = state.online_users.entry(user_id.to_string()).or_default();
            let was_empty = user_sockets.is_empty();
            user_sockets.insert(socket_id);
            was_empty
        };

        if let Some(watchlist_vec) = watchlist {
            let watching_set: HashSet<String> = watchlist_vec.into_iter().collect();
            let previous_watching = state
                .watchlists
                .get(user_id)
                .map(|entry| entry.watching.clone())
                .unwrap_or_default();

            for removed_user_id in previous_watching.difference(&watching_set) {
                if let Some(removed_entry) = state.watchlists.get_mut(removed_user_id) {
                    removed_entry.watchers.remove(user_id);
                }
            }

            let user_watchers = {
                let user_entry = state
                    .watchlists
                    .entry(user_id.to_string())
                    .or_insert_with(|| WatchlistEntry {
                        user_id: user_id.to_string(),
                        watchers: HashSet::new(),
                        watching: watching_set.clone(),
                    });
                user_entry.watching = watching_set.clone();
                user_entry.watchers.clone()
            };

            for watched_user_id in &watching_set {
                let watched_entry = state
                    .watchlists
                    .entry(watched_user_id.clone())
                    .or_insert_with(|| WatchlistEntry {
                        user_id: watched_user_id.clone(),
                        watchers: HashSet::new(),
                        watching: HashSet::new(),
                    });
                watched_entry.watchers.insert(user_id.to_string());
            }

            if was_offline {
                for _ in &user_watchers {
                    events_to_send.push(PusherMessage::watchlist_online_event(vec![
                        user_id.to_string(),
                    ]));
                }
            }

            let mut online_watched_users = Vec::new();
            let mut offline_watched_users = Vec::new();

            for watched_user_id in &watching_set {
                let is_online = state
                    .online_users
                    .get(watched_user_id)
                    .is_some_and(|sockets| !sockets.is_empty());
                if is_online {
                    online_watched_users.push(watched_user_id.clone());
                } else {
                    offline_watched_users.push(watched_user_id.clone());
                }
            }

            if !online_watched_users.is_empty() {
                events_to_send.push(PusherMessage::watchlist_online_event(online_watched_users));
            }
            if !offline_watched_users.is_empty() {
                events_to_send.push(PusherMessage::watchlist_offline_event(
                    offline_watched_users,
                ));
            }
        }

        Ok(events_to_send)
    }

    /// Remove a user connection
    pub async fn remove_user_connection(
        &self,
        app_id: &str,
        user_id: &str,
        socket_id: &SocketId,
    ) -> Result<Vec<PusherMessage>> {
        let mut events_to_send = Vec::new();
        let Some(app_state) = self.apps.get(app_id).map(|entry| entry.value().clone()) else {
            return Ok(events_to_send);
        };
        let mut state = app_state.lock().unwrap();

        let mut should_cleanup_user = false;
        if let Some(user_sockets) = state.online_users.get_mut(user_id) {
            user_sockets.remove(socket_id);
            if user_sockets.is_empty() {
                should_cleanup_user = true;
                if let Some(user_entry) = state.watchlists.get(user_id) {
                    for _ in &user_entry.watchers {
                        events_to_send.push(PusherMessage::watchlist_offline_event(vec![
                            user_id.to_string(),
                        ]));
                    }
                }
            }
        }

        if should_cleanup_user {
            state.online_users.remove(user_id);

            if let Some(user_entry) = state.watchlists.remove(user_id) {
                for watched_user_id in &user_entry.watching {
                    if let Some(watched_entry) = state.watchlists.get_mut(watched_user_id) {
                        watched_entry.watchers.remove(user_id);
                    }
                }
            }
        }

        Ok(events_to_send)
    }

    /// Get online status for a user's watchlist
    pub async fn get_watchlist_status(
        &self,
        app_id: &str,
        user_id: &str,
    ) -> Result<(Vec<String>, Vec<String>)> {
        let mut online_users = Vec::new();
        let mut offline_users = Vec::new();

        if let Some(app_state) = self.apps.get(app_id).map(|entry| entry.value().clone()) {
            let state = app_state.lock().unwrap();
            if let Some(user_entry) = state.watchlists.get(user_id) {
                for watched_user_id in &user_entry.watching {
                    let is_online = state
                        .online_users
                        .get(watched_user_id)
                        .is_some_and(|sockets| !sockets.is_empty());
                    if is_online {
                        online_users.push(watched_user_id.clone());
                    } else {
                        offline_users.push(watched_user_id.clone());
                    }
                }
            }
        }

        Ok((online_users, offline_users))
    }

    /// Clean up when app is removed
    pub async fn cleanup_app(&self, app_id: &str) {
        self.apps.remove(app_id);
    }

    pub async fn get_watchers_for_user(&self, app_id: &str, user_id: &str) -> Result<Vec<String>> {
        let mut watchers = Vec::new();

        if let Some(app_state) = self.apps.get(app_id).map(|entry| entry.value().clone()) {
            let state = app_state.lock().unwrap();
            if let Some(user_entry) = state.watchlists.get(user_id) {
                watchers.extend(user_entry.watchers.iter().cloned());
            }
        }

        Ok(watchers)
    }
}
