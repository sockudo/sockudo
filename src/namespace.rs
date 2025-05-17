use crate::app::manager::AppManager;
use crate::channel::PresenceMemberInfo;
use crate::error::{Error, Result};
use crate::log::Log;
use crate::protocol::messages::PusherMessage;
use crate::websocket::{ConnectionState, SocketId, WebSocket, WebSocketRef}; // Assuming WebSocketRef is defined (e.g., pub struct WebSocketRef(Arc<Mutex<WebSocket>>);)
use dashmap::{DashMap, DashSet};
use fastwebsockets::{Frame, Payload, WebSocketWrite};
use futures::future::join_all;
// use futures::stream::{self, StreamExt}; // StreamExt seems unused in the provided snippet
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use std::collections::HashMap;
// use std::collections::HashSet; // HashSet seems unused
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::io::WriteHalf;
use tokio::sync::{mpsc, Mutex};
// use tokio::sync::Semaphore; // Semaphore seems unused

// Represents a namespace, typically tied to a specific application ID.
// Manages WebSocket connections, channel subscriptions, and user presence within that app.
pub struct Namespace {
    pub app_id: String,
    // Stores all active WebSocket connections, keyed by their unique SocketId.
    // Arc<Mutex<WebSocket>> allows shared, mutable access to connection state.
    pub sockets: DashMap<SocketId, Arc<Mutex<WebSocket>>>,
    // Maps channel names (String) to a set of SocketIds subscribed to that channel.
    pub channels: DashMap<String, DashSet<SocketId>>,
    // Maps user IDs (String) to a set of WebSocket references associated with that user.
    // WebSocketRef likely wraps Arc<Mutex<WebSocket>> for reference counting and access.
    pub users: DashMap<String, DashSet<WebSocketRef>>,
}

impl Namespace {
    // Creates a new Namespace for a given application ID.
    pub fn new(app_id: String) -> Self {
        Self {
            app_id,
            sockets: DashMap::new(),
            channels: DashMap::new(),
            users: DashMap::new(),
        }
    }

    // Adds a new WebSocket connection to the namespace.
    pub async fn add_socket(
        &self,
        socket_id: SocketId,
        socket: WebSocketWrite<WriteHalf<TokioIo<Upgraded>>>,
        app_manager: &Arc<dyn AppManager + Send + Sync>,
    ) {
        // Create a channel for sending outgoing messages to this specific WebSocket client.
        let (tx, mut rx) = mpsc::unbounded_channel();

        // Initialize the WebSocket state.
        let mut connection_state = ConnectionState::new();
        connection_state.socket_id = socket_id.clone(); // Store socket_id in the state

        // Fetch and associate the application configuration.
        // TODO: Consider returning a Result from this function to signal failure.
        match app_manager.find_by_id(&self.app_id).await {
            Ok(Some(app)) => {
                connection_state.app = Some(app);
            }
            Ok(None) => {
                // App not found, log error and potentially close connection early.
                Log::error(format!(
                    "App not found for app_id: {}. Cannot fully initialize socket: {}",
                    self.app_id, socket_id
                ));
                // Decide on error handling: maybe send an error frame and close?
                // For now, it proceeds without app config.
                // Consider sending a close frame immediately:
                // let _ = socket.write_frame(Frame::close(1011, b"Internal server error")).await;
                // return;
            }
            Err(e) => {
                Log::error(format!(
                    "Failed to get app {} for socket {}: {}",
                    self.app_id, socket_id, e
                ));
                // Decide on error handling.
                // let _ = socket.write_frame(Frame::close(1011, b"Internal server error")).await;
                // return;
            }
        }

        // Create the WebSocket wrapper.
        let mut connection = WebSocket {
            state: connection_state,
            socket: Some(socket), // The actual write half of the WebSocket.
            message_sender: tx,   // Sender part of the message channel.
        };

        // Wrap the connection in Arc<Mutex> for shared ownership and interior mutability.
        let connection_arc = Arc::new(Mutex::new(connection));

        // Store the connection in the central map.
        self.sockets
            .insert(socket_id.clone(), connection_arc.clone());

        // Spawn a dedicated task to handle sending messages from the channel to the WebSocket client.
        // This decouples message sending logic from the rest of the application.
        let connection_clone = connection_arc.clone(); // Clone Arc for the task.
        tokio::spawn(async move {
            while let Some(frame) = rx.recv().await {
                // Lock the connection to access the socket's write half.
                let mut connection_guard = connection_clone.lock().await;
                if let Some(socket) = &mut connection_guard.socket {
                    // Attempt to send the frame over the WebSocket.
                    if let Err(e) = socket.write_frame(frame).await {
                        Log::error(format!(
                            "Failed to send frame to socket {}: {}. Closing send loop.",
                            socket_id, e
                        ));
                        // Error likely means the connection is broken or closed.
                        // Remove the socket write half to prevent further writes.
                        connection_guard.socket.take();
                        // Break the loop to terminate this sender task.
                        break;
                    }
                } else {
                    // If connection_guard.socket is None, it means the socket was already closed/taken elsewhere.
                    Log::info(format!(
                        "Send loop for socket {} stopping: WebSocket already closed.",
                        socket_id
                    ));
                    break;
                }
                // Drop the lock guard before waiting for the next message.
                drop(connection_guard);
            }
            // Optional: Log when the send loop terminates naturally (e.g., channel closed).
            // Log::info(format!("Send loop for socket {} finished.", socket_id));
        });
    }

    // Retrieves a connection Arc by SocketId.
    pub fn get_connection(&self, socket_id: &SocketId) -> Option<Arc<Mutex<WebSocket>>> {
        // `DashMap::get` returns a Ref, clone the value (Arc) out of it.
        self.sockets
            .get(socket_id)
            .map(|conn_ref| conn_ref.value().clone())
    }

    // Retrieves a connection Arc if it exists and is subscribed to the specified channel.
    pub fn get_connection_from_channel(
        &self,
        channel: &str,
        socket_id: &SocketId,
    ) -> Option<Arc<Mutex<WebSocket>>> {
        // Check if the channel exists and the socket is subscribed.
        if let Some(channel_sockets) = self.channels.get(channel) {
            if channel_sockets.contains(socket_id) {
                // If subscribed, retrieve the connection.
                return self.get_connection(socket_id);
            }
        }
        None
    }

    // Sends a single PusherMessage to a specific SocketId.
    pub async fn send_message(&self, socket_id: &SocketId, message: PusherMessage) -> Result<()> {
        if let Some(connection) = self.get_connection(socket_id) {
            // Serialize the message to JSON. Propagate serialization errors.
            let message_payload = serde_json::to_string(&message)?;
            // Create a WebSocket text frame.
            let frame = Frame::text(Payload::from(message_payload.into_bytes()));

            // Lock the connection briefly to access the message sender channel.
            // Note: Locking is necessary because the sender is inside the Mutex-protected WebSocket.
            let conn_guard = connection.lock().await;
            conn_guard.message_sender.send(frame).map_err(|e| {
                // If send fails, the receiver task likely terminated (connection closed).
                Log::warning(format!("Failed to queue message for {}: {}", socket_id, e));
                Error::ConnectionError(format!(
                    "Failed to send message: receiver closed for {}",
                    socket_id
                ))
            })?;
        } else {
            // Log if the target socket doesn't exist (it might have disconnected).
            Log::warning(format!(
                "Attempted to send message to non-existent socket: {}",
                socket_id
            ));
            // Depending on requirements, this could return an error:
            // return Err(Error::NotFound(format!("Socket {} not found", socket_id)));
        }
        Ok(())
    }

    // Broadcasts a PusherMessage to all sockets subscribed to a channel, optionally excluding one.
    pub async fn broadcast(
        &self,
        channel: &str,
        message: PusherMessage,
        except: Option<&SocketId>,
    ) -> Result<()> {
        // Serialize the message once to avoid repeated serialization overhead.
        // Wrap in Arc for cheap cloning per subscriber.
        let payload = Arc::new(serde_json::to_string(&message)?);

        // Retrieve the set of socket IDs subscribed to the channel.
        // Use `get` instead of `get_channel` to avoid accidental channel creation.
        if let Some(socket_ids_ref) = self.channels.get(channel) {
            // Iterate over a clone of the socket IDs to avoid holding the DashSet lock during async operations.
            // Note: This iterates over a snapshot; sockets added/removed concurrently might be missed/included.
            let socket_ids = socket_ids_ref.clone();
            drop(socket_ids_ref); // Drop the DashMap RefGuard

            for socket_id in socket_ids.iter() {
                // Check if this socket should be excluded.
                if except.is_none_or(|excluded_id| excluded_id != &*socket_id) {
                    // Get the connection for the current socket ID.
                    if let Some(connection) = self.get_connection(&*socket_id) {
                        // Clone the Arc'd payload for this specific send operation.
                        let current_payload = payload.clone();
                        // Create the WebSocket text frame.
                        let frame = Frame::text(Payload::from(current_payload.as_bytes().to_vec()));

                        // Lock the connection briefly to access the message sender.
                        let conn_guard = connection.lock().await;
                        if let Err(e) = conn_guard.message_sender.send(frame) {
                            // Log send errors but continue broadcasting to others.
                            Log::warning(format!(
                                "Failed to queue broadcast message for socket {:?}: {:?}",
                                socket_id.0, e
                            ));
                            // Don't propagate this error, as broadcast should be best-effort.
                        }
                        // Drop the lock guard.
                        drop(conn_guard);
                    }
                    // If get_connection returns None, the socket likely disconnected between
                    // fetching the list and trying to send; this is usually acceptable.
                }
            }
        } else {
            // Log if the channel doesn't exist or has no subscribers.
            Log::info(format!(
                "Broadcast attempted on non-existent or empty channel: {}",
                channel
            ));
        }

        Ok(())
    }

    // Retrieves presence information for all members in a presence channel.
    pub async fn get_channel_members(
        &self,
        channel: &str,
    ) -> Result<HashMap<String, PresenceMemberInfo>> {
        let mut presence_members = HashMap::new();

        // Use `get` to avoid side effects of channel creation.
        if let Some(socket_ids_ref) = self.channels.get(channel) {
            // Clone the set to avoid holding the lock during async operations.
            let socket_ids = socket_ids_ref.clone();
            drop(socket_ids_ref); // Drop the DashMap RefGuard

            for socket_id in socket_ids.iter() {
                if let Some(connection) = self.get_connection(&socket_id) {
                    // Lock the connection state briefly to clone presence data.
                    let presence_data = {
                        // Scoped lock
                        let conn_guard = connection.lock().await;
                        // Clone only the presence map for this specific channel, if it exists.
                        conn_guard
                            .state
                            .presence
                            .as_ref()
                            .and_then(|p_map| p_map.get(channel).cloned())
                    }; // Lock guard dropped here

                    // Process the cloned data without holding the lock.
                    if let Some(presence_info) = presence_data {
                        // Assuming user_id is unique per user across sockets in a presence channel.
                        // Later entries for the same user_id might overwrite earlier ones if a user
                        // has multiple sockets in the channel (behavior might need clarification).
                        presence_members.insert(presence_info.user_id.clone(), presence_info);
                    }
                }
            }
        } else {
            // Channel doesn't exist, return empty map.
            Log::info(format!(
                "get_channel_members called on non-existent channel: {}",
                channel
            ));
        }

        Ok(presence_members)
    }

    // Retrieves all connection Arcs for sockets subscribed to a specific channel.
    // Returns a new DashMap containing a snapshot of the connections.
    pub fn get_channel_sockets(&self, channel: &str) -> DashMap<SocketId, Arc<Mutex<WebSocket>>> {
        let sockets = DashMap::new();
        if let Some(channel_sockets_ref) = self.channels.get(channel) {
            // Iterate over a clone to avoid holding the lock.
            let channel_sockets = channel_sockets_ref.clone();
            drop(channel_sockets_ref); // Drop DashMap RefGuard

            for socket_id in channel_sockets.iter() {
                if let Some(connection) = self.get_connection(&*socket_id) {
                    sockets.insert(socket_id.clone(), connection);
                }
                // If connection is None here, it means the socket was removed between
                // getting the channel list and getting the connection.
            }
        }
        sockets
    }

    // Retrieves references to WebSockets associated with a specific user ID.
    // Optimization: Avoids redundant connection lookups compared to the original version.
    pub async fn get_user_sockets(&self, user_id: &str) -> Result<DashSet<WebSocketRef>> {
        // Directly return a clone of the DashSet associated with the user_id if it exists.
        // This avoids iterating and re-fetching connections.
        // Assumes WebSocketRef correctly implements Clone, Eq, Hash for DashSet usage.
        // WebSocketRef likely wraps Arc<Mutex<WebSocket>>.
        match self.users.get(user_id) {
            Some(user_sockets_ref) => {
                // Clone the entire DashSet for the user.
                let user_sockets = user_sockets_ref.clone();
                Ok(user_sockets)
            }
            None => {
                // User not found, return an empty set.
                Ok(DashSet::new())
            }
        }
    }

    // Cleans up a WebSocket connection: sends disconnect messages and removes from internal state.
    pub async fn cleanup_connection(&self, ws_ref: WebSocketRef) {
        // Assuming WebSocketRef allows access to the inner Arc<Mutex<WebSocket>>.
        // Let's assume WebSocketRef is defined as:
        // #[derive(Clone, Eq, PartialEq, Hash)]
        // pub struct WebSocketRef(Arc<Mutex<WebSocket>>);

        let socket_id = {
            // Lock briefly to get the socket ID.
            let ws_guard = ws_ref.0.lock().await;
            ws_guard.state.socket_id.clone()
        };

        // Log the cleanup attempt.
        Log::info(format!("Cleaning up connection for socket: {}", socket_id));

        // Send disconnect message (best effort).
        let disconnect_message = PusherMessage::error(
            4009, // Example error code
            "Connection closed by server".to_string(),
            None,
        );
        let close_frame = Frame::close(1000, b"Closing"); // Standard close frame

        // Serialize error message safely.
        let error_payload = match serde_json::to_string(&disconnect_message) {
            Ok(payload) => Some(payload),
            Err(e) => {
                Log::error(format!(
                    "Failed to serialize disconnect message for {}: {}",
                    socket_id, e
                ));
                None
            }
        };

        // Send frames using the message sender channel (best effort).
        {
            let ws_guard = ws_ref.0.lock().await;
            if let Some(payload) = error_payload {
                let error_frame = Frame::text(Payload::from(payload.into_bytes()));
                // Ignore send errors during cleanup, as the connection might already be dead.
                let _ = ws_guard.message_sender.send(error_frame);
            }
            let _ = ws_guard.message_sender.send(close_frame);

            // Optional: Mark the socket as closed within the state if needed.
            // ws_guard.socket.take(); // Or similar state update.
        } // Lock guard dropped here.

        // Remove socket from all channels it was subscribed to.
        // Iterate through channels and remove the socket ID.
        // `retain` is efficient for removing empty sets.
        self.channels.retain(|_channel_name, socket_set| {
            socket_set.remove(&socket_id);
            !socket_set.is_empty() // Keep the channel entry if other sockets remain.
        });

        // Remove socket reference from user tracking.
        // Need the user ID associated with this socket.
        let user_id_option = {
            let ws_guard = ws_ref.0.lock().await;
            // Safely get user ID string if user data exists.
            ws_guard
                .state
                .user
                .as_ref()
                .and_then(|u| u.get("id"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()) // Clone the user ID string
        };

        if let Some(user_id) = user_id_option {
            // Get the mutable reference to the user's socket set.
            if let Some(mut user_sockets_ref) = self.users.get_mut(&user_id) {
                // Remove the specific WebSocketRef from the set.
                user_sockets_ref.remove(&ws_ref);
                // Check if the set is now empty *after* removal.
                let is_empty = user_sockets_ref.is_empty();
                // Drop the mutable reference *before* potentially removing the user entry.
                drop(user_sockets_ref);

                // If the user's socket set is now empty, remove the user entry entirely.
                if is_empty {
                    self.users.remove(&user_id);
                    Log::info(format!("Removed empty user entry for: {}", user_id));
                }
            }
        }

        // Finally, remove the socket from the main sockets map.
        if self.sockets.remove(&socket_id).is_some() {
            Log::info(format!("Removed socket {} from main map.", socket_id));
        } else {
            Log::warning(format!(
                "Socket {} already removed from main map during cleanup.",
                socket_id
            ));
        }
    }

    // Terminates all connections associated with a specific user ID.
    pub async fn terminate_user_connections(&self, user_id: &str) -> Result<()> {
        // Get the set of WebSocketRefs for the user.
        // Use the optimized get_user_sockets which clones the set.
        let connections_to_terminate = match self.users.get(user_id) {
            Some(user_sockets_ref) => user_sockets_ref.clone(),
            None => DashSet::new(), // No connections for this user.
        };

        if connections_to_terminate.is_empty() {
            Log::info(format!(
                "No active connections found to terminate for user: {}",
                user_id
            ));
            return Ok(());
        }

        Log::info(format!(
            "Terminating {} connections for user: {}",
            connections_to_terminate.len(),
            user_id
        ));

        // Create futures for cleaning up each connection concurrently.
        let mut cleanup_futures = Vec::new();
        for connection_ref in connections_to_terminate.iter() {
            // cleanup_connection takes ownership of the WebSocketRef clone.
            cleanup_futures.push(self.cleanup_connection(connection_ref.clone()));
        }

        // Wait for all cleanup tasks to complete.
        join_all(cleanup_futures).await;

        Log::info(format!(
            "Finished terminating connections for user: {}",
            user_id
        ));

        // Note: The user entry in `self.users` might still exist briefly if cleanup_connection
        // hasn't removed it yet, but it should be removed shortly after by those tasks.
        // Alternatively, explicitly remove the user entry here after cleanup:
        // self.users.remove(user_id);

        Ok(())
    }

    // Subscribes a socket to a channel. Returns true if the socket was newly added.
    pub fn add_channel_to_socket(&self, channel: &str, socket_id: &SocketId) -> bool {
        // `entry().or_default()` ensures the DashSet exists for the channel.
        // `insert` returns true if the value was not already present.
        self.channels
            .entry(channel.to_string())
            .or_default()
            .insert(socket_id.clone())
    }

    // Unsubscribes a socket from a channel. Returns true (consider changing return type).
    pub fn remove_channel_from_socket(&self, channel: &str, socket_id: &SocketId) -> bool {
        // Use `get_mut` to attempt removal.
        if let Some(mut channel_sockets_ref) = self.channels.get_mut(channel) {
            let removed = channel_sockets_ref.remove(socket_id);
            let is_empty = channel_sockets_ref.is_empty();
            // Drop the mutable reference before potentially removing the channel entry.
            drop(channel_sockets_ref);

            if is_empty {
                // Remove the channel entry itself if no sockets remain.
                self.channels.remove(channel);
                Log::info(format!("Removed empty channel entry: {}", channel));
            }
            return removed.is_some(); // Return whether the socket was actually in the set.
        } else {
            // Channel didn't exist, so socket wasn't in it.
            false
        }
    }

    // Removes a connection entirely from the main socket map.
    // Typically called after cleanup_connection.
    pub fn remove_connection(&self, socket_id: &SocketId) {
        if self.sockets.remove(socket_id).is_some() {
            Log::info(format!("Explicitly removed socket: {}", socket_id));
        }
    }

    // Retrieves the set of socket IDs for a channel.
    // WARNING: This method CREATES the channel entry if it doesn't exist due to `or_default`.
    // Consider using `get_channel_subscribers` for a read-only version.
    pub fn get_channel(&self, channel: &str) -> Result<DashSet<SocketId>> {
        // `entry().or_default()` gets or creates the channel entry.
        let channel_data = self.channels.entry(channel.to_string()).or_default();
        // Clone the set of socket IDs.
        Ok(channel_data.value().clone())
    }

    // Read-only alternative to get_channel. Returns None if channel doesn't exist.
    pub fn get_channel_subscribers(&self, channel: &str) -> Option<DashSet<SocketId>> {
        self.channels.get(channel).map(|set_ref| set_ref.clone())
    }

    // Removes a channel entry entirely, regardless of subscribers.
    pub fn remove_channel(&self, channel: &str) {
        self.channels.remove(channel);
        Log::info(format!("Removed channel entry: {}", channel));
    }

    // Checks if a specific socket is subscribed to a specific channel.
    // Uses read-only access, avoiding the side effect of `get_channel`.
    pub fn is_in_channel(&self, channel: &str, socket_id: &SocketId) -> bool {
        self.channels
            .get(channel) // Get read-only reference to the set.
            .map_or(false, |channel_sockets| channel_sockets.contains(socket_id))
        // Check containment.
    }

    // Retrieves presence information for a specific socket within a channel.
    pub async fn get_presence_member(
        &self,
        channel: &str,
        socket_id: &SocketId,
    ) -> Option<PresenceMemberInfo> {
        // Get the connection, handling potential absence.
        if let Some(connection) = self.get_connection(socket_id) {
            // Lock the state briefly.
            let conn_guard = connection.lock().await;
            // Safely access presence data, clone if found for the specific channel.
            conn_guard
                .state
                .presence
                .as_ref() // Option<&HashMap>
                .and_then(|presence_map| presence_map.get(channel)) // Option<&PresenceMemberInfo>
                .cloned() // Option<PresenceMemberInfo>
        } else {
            // Socket doesn't exist.
            None
        }
    }

    // Associates an authenticated user with a WebSocket connection.
    // Safely handles JSON access.
    pub async fn add_user(&self, ws: Arc<Mutex<WebSocket>>) -> Result<()> {
        // Clone the user data (Option<serde_json::Value>) from the WebSocket state.
        let user_json_option = {
            // Scoped lock
            let ws_guard = ws.lock().await;
            ws_guard.state.user.clone()
        };

        if let Some(user_val) = user_json_option {
            // Safely extract the user ID as a string.
            if let Some(user_id_str) = user_val.get("id").and_then(|v| v.as_str()) {
                let user_id = user_id_str.to_string(); // Clone the string slice
                                                       // Get or create the DashSet for this user ID.
                let mut user_sockets_ref = self.users.entry(user_id.clone()).or_default();
                // Add the WebSocket reference (assuming WebSocketRef wraps the Arc).
                // Requires WebSocketRef to implement Clone, Eq, Hash.
                user_sockets_ref.insert(WebSocketRef(ws.clone())); // Assuming WebSocketRef(Arc<Mutex<WebSocket>>)
                Log::info(format!(
                    "Added socket {} to user {}",
                    ws.lock().await.state.socket_id,
                    user_id
                ));
            } else {
                // Log if user data is present but lacks a valid 'id' field.
                Log::warning(format!(
                    "User data found for socket {} but missing 'id' string field.",
                    ws.lock().await.state.socket_id // Re-lock is okay for logging
                ));
                // Potentially return an error here depending on requirements.
                // return Err(Error::InvalidData("User data missing 'id'".to_string()));
            }
        }
        // If user_json_option is None, do nothing (connection is anonymous).
        Ok(())
    }

    // Disassociates a user from a WebSocket connection upon disconnect or logout.
    // Safely handles JSON access.
    pub async fn remove_user(&self, ws: Arc<Mutex<WebSocket>>) -> Result<()> {
        // Clone user data and get socket ID within a single lock if possible.
        let (user_json_option, socket_id) = {
            // Scoped lock
            let ws_guard = ws.lock().await;
            (
                ws_guard.state.user.clone(),
                ws_guard.state.socket_id.clone(),
            )
        };

        if let Some(user_val) = user_json_option {
            // Safely extract the user ID string.
            if let Some(user_id_str) = user_val.get("id").and_then(|v| v.as_str()) {
                // Get a mutable reference to the user's socket set.
                if let Some(user_sockets_ref) = self.users.get_mut(user_id_str) {
                    // Remove the specific WebSocketRef.
                    let removed = user_sockets_ref.remove(&WebSocketRef(ws.clone()));
                    let is_empty = user_sockets_ref.is_empty();
                    // Drop the mutable ref before potentially removing the user entry.
                    drop(user_sockets_ref);

                    if removed.is_some() {
                        Log::info(format!(
                            "Removed socket {} from user {}",
                            socket_id, user_id_str
                        ));
                    }

                    // If the set becomes empty, remove the user entry from the map.
                    if is_empty {
                        self.users.remove(user_id_str);
                        Log::info(format!("Removed empty user entry for: {}", user_id_str));
                    }
                }
                // If user_sockets_ref is None, the user entry might have already been removed.
            } else {
                Log::warning(format!(
                    "User data found for socket {} during removal but missing 'id' string field.",
                    socket_id
                ));
            }
        }
        // If user_json_option is None, nothing to remove.
        Ok(())
    }

    // Retrieves a map of channel names to their current subscriber counts.
    pub async fn get_channels_with_socket_count(&self) -> Result<DashMap<String, usize>> {
        // Create a new DashMap to store the results.
        let channels_with_count: DashMap<String, usize> = DashMap::new();
        // Iterate over the channels map. `iter` provides read-only access.
        for channel_ref in self.channels.iter() {
            let channel_name = channel_ref.key().clone();
            let socket_count = channel_ref.value().len(); // Get the size of the DashSet.
            channels_with_count.insert(channel_name, socket_count);
        }
        Ok(channels_with_count)
    }
}
