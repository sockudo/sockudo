// src/watchlist/manager.rs
use dashmap::DashMap;
use parking_lot::Mutex;
use sockudo_core::error::Result;
use sockudo_core::websocket::SocketId;
use sockudo_protocol::messages::PusherMessage;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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

    fn is_user_online(state: &AppWatchlistState, user_id: &str) -> bool {
        state
            .online_users
            .get(user_id)
            .is_some_and(|sockets| !sockets.is_empty())
    }

    fn prune_watchlist_entry_if_empty(state: &mut AppWatchlistState, user_id: &str) {
        let should_remove = state.watchlists.get(user_id).is_some_and(|entry| {
            entry.watchers.is_empty()
                && entry.watching.is_empty()
                && !Self::is_user_online(state, user_id)
        });

        if should_remove {
            state.watchlists.remove(user_id);
        }
    }

    fn remove_watching_edge(
        state: &mut AppWatchlistState,
        watcher_id: &str,
        watched_user_id: &str,
    ) {
        if let Some(watched_entry) = state.watchlists.get_mut(watched_user_id) {
            watched_entry.watchers.remove(watcher_id);
        }
        Self::prune_watchlist_entry_if_empty(state, watched_user_id);
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
        let mut state = app_state.lock();

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
                Self::remove_watching_edge(&mut state, user_id, removed_user_id);
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
        let mut state = app_state.lock();

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

            if let Some(previous_watching) = state
                .watchlists
                .get(user_id)
                .map(|entry| entry.watching.iter().cloned().collect::<Vec<_>>())
            {
                if let Some(user_entry) = state.watchlists.get_mut(user_id) {
                    user_entry.watching.clear();
                }

                for watched_user_id in previous_watching {
                    Self::remove_watching_edge(&mut state, user_id, &watched_user_id);
                }
            }

            Self::prune_watchlist_entry_if_empty(&mut state, user_id);
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
            let state = app_state.lock();
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
            let state = app_state.lock();
            if let Some(user_entry) = state.watchlists.get(user_id) {
                watchers.extend(user_entry.watchers.iter().cloned());
            }
        }

        Ok(watchers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sockudo_core::websocket::SocketId;

    #[tokio::test]
    async fn removes_empty_placeholder_entries_when_watchlist_changes() {
        let manager = WatchlistManager::new();
        let socket_id = SocketId::new();

        manager
            .add_user_with_watchlist(
                "app",
                "alice",
                socket_id,
                Some(vec!["bob".to_string(), "carol".to_string()]),
            )
            .await
            .unwrap();
        manager
            .add_user_with_watchlist("app", "alice", socket_id, Some(vec!["bob".to_string()]))
            .await
            .unwrap();

        let app_state = manager.apps.get("app").unwrap().value().clone();
        let state = app_state.lock();
        assert!(state.watchlists.contains_key("bob"));
        assert!(!state.watchlists.contains_key("carol"));
    }

    #[tokio::test]
    async fn preserves_watcher_placeholders_when_user_goes_offline() {
        let manager = WatchlistManager::new();
        let watcher_socket = SocketId::new();
        let watched_socket = SocketId::new();

        manager
            .add_user_with_watchlist(
                "app",
                "watcher",
                watcher_socket,
                Some(vec!["watched".to_string()]),
            )
            .await
            .unwrap();
        manager
            .add_user_with_watchlist("app", "watched", watched_socket, Some(Vec::new()))
            .await
            .unwrap();

        manager
            .remove_user_connection("app", "watched", &watched_socket)
            .await
            .unwrap();

        let watchers = manager
            .get_watchers_for_user("app", "watched")
            .await
            .unwrap();
        assert_eq!(watchers, vec!["watcher".to_string()]);

        let app_state = manager.apps.get("app").unwrap().value().clone();
        let state = app_state.lock();
        let watched_entry = state.watchlists.get("watched").unwrap();
        assert!(watched_entry.watching.is_empty());
        assert_eq!(watched_entry.watchers.len(), 1);
    }
}
