// Make sure App is in scope
use crate::app::manager::AppManager;
use crate::channel::PresenceMemberInfo;
use crate::error::{Error, Result}; // Error should be in scope

use crate::protocol::messages::PusherMessage;
use crate::websocket::{ConnectionState, SocketId, WebSocket, WebSocketRef};
use dashmap::{DashMap, DashSet};
use fastwebsockets::{Frame, OpCode, Payload, WebSocketWrite};
use futures::future::join_all;
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use std::collections::HashMap;
// use std::collections::HashSet; // HashSet seems unused
use std::sync::atomic::{AtomicU32, Ordering}; // Added AtomicU32 and Ordering
use std::sync::Arc;
use tokio::io::WriteHalf;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, error, info, warn};
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
    // Now returns a Result to indicate success or failure (e.g., connection limit).
    pub async fn add_socket(
        &self,
        socket_id: SocketId,
        socket_writer: WebSocketWrite<WriteHalf<TokioIo<Upgraded>>>, // Renamed for clarity
        app_manager: &Arc<dyn AppManager + Send + Sync>,
    ) -> Result<Arc<Mutex<WebSocket>>> {
        // Return the connection Arc on success
        // Fetch the application configuration first.
        let app_config = match app_manager.find_by_id(&self.app_id).await {
            Ok(Some(app)) => app,
            Ok(None) => {
                error!(
                    "App not found for app_id: {}. Cannot initialize socket: {}",
                    self.app_id, socket_id
                );
                return Err(Error::ApplicationNotFound);
            }
            Err(e) => {
                error!(
                    "Failed to get app {} for socket {}: {}",
                    self.app_id, socket_id, e
                );
                return Err(Error::InternalError(format!(
                    "Failed to retrieve app config: {}",
                    e
                )));
            }
        };

        // Create a channel for sending outgoing messages to this specific WebSocket client.
        let (tx, mut rx) = mpsc::unbounded_channel();

        let mut connection_state = ConnectionState::new();
        connection_state.socket_id = socket_id.clone();
        connection_state.app = Some(app_config); // Store the fetched app config

        let connection = WebSocket {
            state: connection_state,
            socket: Some(socket_writer), // The actual write half of the WebSocket.
            message_sender: tx,          // Sender part of the message channel.
        };

        let connection_arc = Arc::new(Mutex::new(connection));

        // Store the connection in the central map.
        self.sockets
            .insert(socket_id.clone(), connection_arc.clone());

        // Spawn a dedicated task to handle sending messages from the channel to the WebSocket client.
        let task_connection_arc = connection_arc.clone(); // Clone Arc for the task.
        let task_socket_id = socket_id.clone(); // Clone socket_id for the task
        tokio::spawn(async move {
            while let Some(frame) = rx.recv().await {
                // Log details about the frame BEFORE attempting to send it
                debug!(
                    socket_id = %task_socket_id,
                    opcode = ?frame.opcode,
                    payload_len = frame.payload.len(),
                    fin = frame.fin,
                    "Attempting to send frame"
                );

                // For text frames, log a snippet of the payload (be mindful of large payloads)
                if frame.opcode == OpCode::Text {
                    let payload_str_snippet = String::from_utf8_lossy(
                        &frame.payload[..std::cmp::min(frame.payload.len(), 200)], // Log first 200 bytes max
                    );
                    debug!(socket_id = %task_socket_id, "Frame text payload snippet: '{}'", payload_str_snippet);
                }

                let mut connection_guard = task_connection_arc.lock().await;
                if let Some(socket) = &mut connection_guard.socket {
                    // Attempt to send the frame. The `frame` is consumed here.
                    if let Err(e) = socket.write_frame(frame).await {
                        // Log the error, which will include the "Broken pipe" details if that's the cause
                        error!(
                            socket_id = %task_socket_id,
                            error = %e,
                            "Failed to send frame. Closing send loop."
                        );
                        connection_guard.socket.take();
                        break;
                    }
                } else {
                    info!(
                        socket_id = %task_socket_id,
                        "Send loop stopping: WebSocket already closed (socket writer was None)."
                    );
                    break;
                }
                drop(connection_guard);
            }
            // This log helps confirm the send loop for a specific socket has ended.
            info!(socket_id = %task_socket_id, "Send loop terminated.");
        });
        Ok(connection_arc)
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

            let conn_guard = connection.lock().await;
            conn_guard.message_sender.send(frame).map_err(|e| {
                warn!("Failed to queue message for {}: {}", socket_id, e);
                Error::ConnectionError(format!(
                    "Failed to send message: receiver closed for {}",
                    socket_id
                ))
            })?;
        } else {
            warn!(
                "Attempted to send message to non-existent socket: {}",
                socket_id
            );
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
        let payload = Arc::new(serde_json::to_string(&message)?);

        if let Some(socket_ids_ref) = self.channels.get(channel) {
            let socket_ids_snapshot = socket_ids_ref.clone(); // Clone the DashSet for iteration
            drop(socket_ids_ref); // Drop the DashMap RefGuard

            for socket_id_entry in socket_ids_snapshot.iter() {
                // iter() on DashSet gives &SocketId
                let current_socket_id = socket_id_entry.key(); // Get the SocketId itself
                if except.is_none_or(|excluded_id| excluded_id != current_socket_id) {
                    if let Some(connection) = self.get_connection(current_socket_id) {
                        let current_payload_clone = payload.clone();
                        let frame =
                            Frame::text(Payload::from(current_payload_clone.as_bytes().to_vec()));

                        let conn_guard = connection.lock().await;
                        if let Err(e) = conn_guard.message_sender.send(frame) {
                            warn!(
                                "Failed to queue broadcast message for socket {:?}: {:?}",
                                current_socket_id.0,
                                e // Accessing the String inside SocketId for logging
                            );
                        }
                        drop(conn_guard);
                    }
                }
            }
        } else {
            info!(
                "Broadcast attempted on non-existent or empty channel: {}",
                channel
            );
        }
        Ok(())
    }

    // Retrieves presence information for all members in a presence channel.
    pub async fn get_channel_members(
        &self,
        channel: &str,
    ) -> Result<HashMap<String, PresenceMemberInfo>> {
        let mut presence_members = HashMap::new();

        if let Some(socket_ids_ref) = self.channels.get(channel) {
            let socket_ids_snapshot = socket_ids_ref.clone();
            drop(socket_ids_ref);

            for socket_id_entry in socket_ids_snapshot.iter() {
                let socket_id = socket_id_entry.key();
                if let Some(connection) = self.get_connection(socket_id) {
                    let presence_data = {
                        let conn_guard = connection.lock().await;
                        conn_guard
                            .state
                            .presence
                            .as_ref()
                            .and_then(|p_map| p_map.get(channel).cloned())
                    };
                    if let Some(presence_info) = presence_data {
                        presence_members.insert(presence_info.user_id.clone(), presence_info);
                    }
                }
            }
        } else {
            info!(
                "get_channel_members called on non-existent channel: {}",
                channel
            );
        }
        Ok(presence_members)
    }

    // Retrieves all connection Arcs for sockets subscribed to a specific channel.
    pub fn get_channel_sockets(&self, channel: &str) -> DashMap<SocketId, Arc<Mutex<WebSocket>>> {
        let sockets_in_channel = DashMap::new();
        if let Some(channel_sockets_ref) = self.channels.get(channel) {
            let channel_sockets_snapshot = channel_sockets_ref.clone();
            drop(channel_sockets_ref);

            for socket_id_entry in channel_sockets_snapshot.iter() {
                let socket_id = socket_id_entry.key();
                if let Some(connection) = self.get_connection(socket_id) {
                    sockets_in_channel.insert(socket_id.clone(), connection);
                }
            }
        }
        sockets_in_channel
    }

    // Retrieves references to WebSockets associated with a specific user ID.
    pub async fn get_user_sockets(&self, user_id: &str) -> Result<DashSet<WebSocketRef>> {
        match self.users.get(user_id) {
            Some(user_sockets_ref) => {
                let user_sockets = user_sockets_ref.clone();
                Ok(user_sockets)
            }
            None => Ok(DashSet::new()),
        }
    }

    // Cleans up a WebSocket connection: sends disconnect messages and removes from internal state.
    pub async fn cleanup_connection(&self, ws_ref: WebSocketRef) {
        let socket_id = {
            let ws_guard = ws_ref.0.lock().await;
            ws_guard.state.socket_id.clone()
        };

        let disconnect_message = PusherMessage::error(
            4009, // Example error code for server-initiated close
            "Connection closed by server".to_string(),
            None,
        );
        let close_frame = Frame::close(1000, b"Closing"); // Standard close frame

        let error_payload = match serde_json::to_string(&disconnect_message) {
            Ok(payload) => Some(payload),
            Err(e) => {
                error!(
                    "Failed to serialize disconnect message for {}: {}",
                    socket_id, e
                );
                None
            }
        };

        // Send frames using the message sender channel (best effort).
        {
            let ws_guard = ws_ref.0.lock().await;
            if let Some(payload_str) = error_payload {
                // Renamed to avoid conflict
                let error_frame = Frame::text(Payload::from(payload_str.into_bytes()));
                // Ignore send errors during cleanup, as the connection might already be dead.
                ws_guard.message_sender.send(error_frame);
            }
        } // Lock guard dropped here.

        // Remove socket from all channels it was subscribed to.
        self.channels.retain(|_channel_name, socket_set| {
            socket_set.remove(&socket_id);
            !socket_set.is_empty() // Keep the channel entry if other sockets remain.
        });

        // Remove socket reference from user tracking.
        let user_id_option = {
            let ws_guard = ws_ref.0.lock().await;
            ws_guard
                .state
                .user
                .as_ref()
                .and_then(|u| u.get("id"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()) // Clone the user ID string
        };

        if let Some(user_id_str_val) = user_id_option {
            // Renamed to avoid conflict
            if let Some(user_sockets_ref) = self.users.get_mut(&user_id_str_val) {
                user_sockets_ref.remove(&ws_ref);
                let is_empty = user_sockets_ref.is_empty();
                drop(user_sockets_ref); // Drop mutable ref before potentially removing user entry
                if is_empty {
                    self.users.remove(&user_id_str_val);
                    info!("Removed empty user entry for: {}", user_id_str_val);
                }
            }
        }

        // Finally, remove the socket from the main sockets map.
        if self.sockets.remove(&socket_id).is_some() {
            info!("Removed socket {} from main map.", socket_id);
        } else {
            warn!(
                "Socket {} already removed from main map during cleanup.",
                socket_id
            );
        }
    }

    // Terminates all connections associated with a specific user ID.
    pub async fn terminate_user_connections(&self, user_id: &str) -> Result<()> {
        if let Some(user_sockets_ref) = self.users.get(user_id) {
            let user_sockets_snapshot = user_sockets_ref.clone();
            drop(user_sockets_ref); // Drop the DashMap RefGuard
            let cleanup_tasks: Vec<_> = user_sockets_snapshot
                .iter()
                .map(async |ws_ref| {
                    let mut ws = ws_ref.0.lock().await;
                    ws.close(
                        4009,
                        "You got disconnected by the app.".to_string(),
                    ).await;
                })
                .collect();

            // Wait for all cleanup tasks to complete.
            join_all(cleanup_tasks).await;
        }
        Ok(())
    }

    // Subscribes a socket to a channel. Returns true if the socket was newly added.
    pub fn add_channel_to_socket(&self, channel: &str, socket_id: &SocketId) -> bool {
        self.channels
            .entry(channel.to_string())
            .or_default()
            .insert(socket_id.clone())
    }

    // Unsubscribes a socket from a channel.
    pub fn remove_channel_from_socket(&self, channel: &str, socket_id: &SocketId) -> bool {
        if let Some(channel_sockets_ref) = self.channels.get_mut(channel) {
            let removed = channel_sockets_ref.remove(socket_id);
            let is_empty = channel_sockets_ref.is_empty();
            drop(channel_sockets_ref); // Drop mutable ref before potentially removing channel entry
            if is_empty {
                self.channels.remove(channel);
                info!("Removed empty channel entry: {}", channel);
            }
            return removed.is_some(); // Return whether the socket was actually in the set.
        }
        false // Channel didn't exist, so socket wasn't in it.
    }

    // Removes a connection entirely from the main socket map.
    pub fn remove_connection(&self, socket_id: &SocketId) {
        // This method should primarily be used if cleanup_connection might not have run.
        // The active_connections decrement should be centralized in cleanup_connection.
        if self.sockets.remove(socket_id).is_some() {
            info!("Explicitly removed socket: {}", socket_id);
            // Avoid double-decrementing active_connections.
            // If this is called after cleanup_connection, it's fine.
            // If called instead of, the counter would be off.
        }
    }

    // Retrieves the set of socket IDs for a channel.
    // WARNING: This method CREATES the channel entry if it doesn't exist due to `or_default`.
    pub fn get_channel(&self, channel: &str) -> Result<DashSet<SocketId>> {
        let channel_data = self.channels.entry(channel.to_string()).or_default();
        Ok(channel_data.value().clone())
    }

    // Read-only alternative to get_channel. Returns None if channel doesn't exist.
    pub fn get_channel_subscribers(&self, channel: &str) -> Option<DashSet<SocketId>> {
        self.channels.get(channel).map(|set_ref| set_ref.clone())
    }

    // Removes a channel entry entirely, regardless of subscribers.
    pub fn remove_channel(&self, channel: &str) {
        self.channels.remove(channel);
        info!("Removed channel entry: {}", channel);
    }

    // Checks if a specific socket is subscribed to a specific channel.
    pub fn is_in_channel(&self, channel: &str, socket_id: &SocketId) -> bool {
        self.channels
            .get(channel)
            .is_some_and(|channel_sockets| channel_sockets.contains(socket_id))
    }

    // Retrieves presence information for a specific socket within a channel.
    pub async fn get_presence_member(
        &self,
        channel: &str,
        socket_id: &SocketId,
    ) -> Option<PresenceMemberInfo> {
        if let Some(connection) = self.get_connection(socket_id) {
            let conn_guard = connection.lock().await;
            conn_guard
                .state
                .presence
                .as_ref()
                .and_then(|presence_map| presence_map.get(channel))
                .cloned()
        } else {
            None
        }
    }

    // Associates an authenticated user with a WebSocket connection.
    pub async fn add_user(&self, ws: Arc<Mutex<WebSocket>>) -> Result<()> {
        let user_json_option = {
            let ws_guard = ws.lock().await;
            ws_guard.state.user.clone()
        };
        if let Some(user_val) = user_json_option {
            if let Some(user_id_str) = user_val.get("id").and_then(|v| v.as_str()) {
                let user_id = user_id_str.to_string();
                let user_sockets_ref = self.users.entry(user_id.clone()).or_default();
                user_sockets_ref.insert(WebSocketRef(ws.clone()));
                info!(
                    "Added socket {} to user {}",
                    ws.lock().await.state.socket_id, // Re-lock OK for logging
                    user_id
                );
            } else {
                warn!(
                    "User data found for socket {} but missing 'id' string field.",
                    ws.lock().await.state.socket_id // Re-lock OK for logging
                );
            }
        }
        Ok(())
    }

    // Disassociates a user from a WebSocket connection upon disconnect or logout.
    pub async fn remove_user(&self, ws: Arc<Mutex<WebSocket>>) -> Result<()> {
        let (user_json_option, socket_id) = {
            let ws_guard = ws.lock().await;
            (
                ws_guard.state.user.clone(),
                ws_guard.state.socket_id.clone(),
            )
        };
        if let Some(user_val) = user_json_option {
            if let Some(user_id_str) = user_val.get("id").and_then(|v| v.as_str()) {
                if let Some(user_sockets_ref) = self.users.get_mut(user_id_str) {
                    let removed = user_sockets_ref.remove(&WebSocketRef(ws.clone()));
                    let is_empty = user_sockets_ref.is_empty();
                    drop(user_sockets_ref); // Drop mutable ref before potentially removing user entry
                    if removed.is_some() {
                        info!("Removed socket {} from user {}", socket_id, user_id_str);
                    }
                    if is_empty {
                        self.users.remove(user_id_str);
                        info!("Removed empty user entry for: {}", user_id_str);
                    }
                }
            } else {
                warn!(
                    "User data found for socket {} during removal but missing 'id' string field.",
                    socket_id
                );
            }
        }
        Ok(())
    }

    // Retrieves a map of channel names to their current subscriber counts.
    pub async fn get_channels_with_socket_count(&self) -> Result<DashMap<String, usize>> {
        let channels_with_count: DashMap<String, usize> = DashMap::new();
        for channel_ref in self.channels.iter() {
            let channel_name = channel_ref.key().clone();
            let socket_count = channel_ref.value().len();
            channels_with_count.insert(channel_name, socket_count);
        }
        Ok(channels_with_count)
    }
    pub async fn get_sockets(&self) -> Result<DashMap<SocketId, Arc<Mutex<WebSocket>>>> {
        let sockets = self.sockets.clone();
        Ok(sockets)
    }
}
