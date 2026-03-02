use crate::error::Result;
use dashmap::DashMap;
use sockudo_protocol::messages::PusherMessage;
use sockudo_types::socket::SocketId;
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct WatchlistEntry {
    pub user_id: String,
    pub watchers: HashSet<String>,
    pub watching: HashSet<String>,
}

pub struct WatchlistManager {
    watchlists: Arc<DashMap<String, DashMap<String, WatchlistEntry>>>,
    online_users: Arc<DashMap<String, DashMap<String, HashSet<SocketId>>>>,
}

impl Default for WatchlistManager {
    fn default() -> Self {
        Self::new()
    }
}

impl WatchlistManager {
    pub fn new() -> Self {
        Self {
            watchlists: Arc::new(DashMap::new()),
            online_users: Arc::new(DashMap::new()),
        }
    }

    pub async fn add_user_with_watchlist(
        &self,
        app_id: &str,
        user_id: &str,
        socket_id: SocketId,
        watchlist: Option<Vec<String>>,
    ) -> Result<Vec<PusherMessage>> {
        let mut events_to_send = Vec::new();

        if !self.watchlists.contains_key(app_id) {
            self.watchlists.insert(app_id.to_string(), DashMap::new());
        }
        if !self.online_users.contains_key(app_id) {
            self.online_users.insert(app_id.to_string(), DashMap::new());
        }

        let app_watchlists = self.watchlists.get(app_id).unwrap();
        let app_online_users = self.online_users.get(app_id).unwrap();

        let was_offline = {
            let mut user_sockets = app_online_users.entry(user_id.to_string()).or_default();
            let was_empty = user_sockets.is_empty();
            user_sockets.insert(socket_id);
            was_empty
        };

        if let Some(watchlist_vec) = watchlist {
            let watching_set: HashSet<String> = watchlist_vec.into_iter().collect();

            let mut user_entry = app_watchlists
                .entry(user_id.to_string())
                .or_insert_with(|| WatchlistEntry {
                    user_id: user_id.to_string(),
                    watchers: HashSet::new(),
                    watching: watching_set.clone(),
                });
            user_entry.watching = watching_set.clone();

            for watched_user_id in &watching_set {
                let mut watched_entry = app_watchlists
                    .entry(watched_user_id.clone())
                    .or_insert_with(|| WatchlistEntry {
                        user_id: watched_user_id.clone(),
                        watchers: HashSet::new(),
                        watching: HashSet::new(),
                    });
                watched_entry.watchers.insert(user_id.to_string());
            }

            if was_offline && let Some(user_entry) = app_watchlists.get(user_id) {
                for _ in &user_entry.watchers {
                    events_to_send.push(PusherMessage::watchlist_online_event(vec![
                        user_id.to_string(),
                    ]));
                }
            }

            let mut online_watched_users = Vec::new();
            let mut offline_watched_users = Vec::new();

            for watched_user_id in &watching_set {
                if app_online_users.contains_key(watched_user_id)
                    && !app_online_users.get(watched_user_id).unwrap().is_empty()
                {
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

    pub async fn remove_user_connection(
        &self,
        app_id: &str,
        user_id: &str,
        socket_id: &SocketId,
    ) -> Result<Vec<PusherMessage>> {
        let mut events_to_send = Vec::new();
        let mut should_cleanup_user = false;

        if let Some(app_online_users) = self.online_users.get(app_id)
            && let Some(mut user_sockets) = app_online_users.get_mut(user_id)
        {
            user_sockets.remove(socket_id);

            if user_sockets.is_empty() {
                should_cleanup_user = true;

                if let Some(app_watchlists) = self.watchlists.get(app_id)
                    && let Some(user_entry) = app_watchlists.get(user_id)
                {
                    for _ in &user_entry.watchers {
                        events_to_send.push(PusherMessage::watchlist_offline_event(vec![
                            user_id.to_string(),
                        ]));
                    }
                }
            }
        }

        if should_cleanup_user {
            if let Some(app_online_users) = self.online_users.get(app_id) {
                app_online_users.remove(user_id);
            }

            if let Some(app_watchlists) = self.watchlists.get(app_id) {
                let watching_list: Vec<String> = app_watchlists
                    .get(user_id)
                    .map(|entry| entry.watching.iter().cloned().collect())
                    .unwrap_or_default();

                for watched_user_id in &watching_list {
                    if let Some(mut watched_entry) = app_watchlists.get_mut(watched_user_id) {
                        watched_entry.watchers.remove(user_id);
                    }
                }

                app_watchlists.remove(user_id);
            }
        }

        Ok(events_to_send)
    }

    pub async fn get_watchlist_status(
        &self,
        app_id: &str,
        user_id: &str,
    ) -> Result<(Vec<String>, Vec<String>)> {
        let mut online_users = Vec::new();
        let mut offline_users = Vec::new();

        if let Some(app_watchlists) = self.watchlists.get(app_id)
            && let Some(user_entry) = app_watchlists.get(user_id)
            && let Some(app_online_users) = self.online_users.get(app_id)
        {
            for watched_user_id in &user_entry.watching {
                if app_online_users.contains_key(watched_user_id)
                    && !app_online_users.get(watched_user_id).unwrap().is_empty()
                {
                    online_users.push(watched_user_id.clone());
                } else {
                    offline_users.push(watched_user_id.clone());
                }
            }
        }

        Ok((online_users, offline_users))
    }

    pub async fn cleanup_app(&self, app_id: &str) {
        self.watchlists.remove(app_id);
        self.online_users.remove(app_id);
    }

    pub async fn get_watchers_for_user(&self, app_id: &str, user_id: &str) -> Result<Vec<String>> {
        let mut watchers = Vec::new();

        if let Some(app_watchlists) = self.watchlists.get(app_id)
            && let Some(user_entry) = app_watchlists.get(user_id)
        {
            watchers.extend(user_entry.watchers.iter().cloned());
        }

        Ok(watchers)
    }
}
