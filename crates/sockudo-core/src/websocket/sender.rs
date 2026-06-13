use super::buffer::{ByteCounter, MessageSenderHandle, SizedMessageReceiverHandle};
use crate::error::{Error, Result};
use crossfire::{TrySendError, mpsc};
use sockudo_ws::Message;
use sockudo_ws::axum_integration::WebSocketWriter;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

// Message sender for async message handling
#[derive(Debug)]
pub struct MessageSender {
    sender: MessageSenderHandle,
    receiver_handle: Option<JoinHandle<()>>,
}

impl Drop for MessageSender {
    fn drop(&mut self) {
        if let Some(handle) = self.receiver_handle.take() {
            handle.abort();
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum SocketOperation {
    WriteFrame,
    SendCloseFrame,
}

impl std::fmt::Display for SocketOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SocketOperation::WriteFrame => write!(f, "write message to WebSocket"),
            SocketOperation::SendCloseFrame => write!(f, "send close message"),
        }
    }
}

impl SocketOperation {
    fn is_close_operation(&self) -> bool {
        matches!(self, SocketOperation::SendCloseFrame)
    }
}

impl MessageSender {
    pub fn new_with_broadcast(
        mut socket: WebSocketWriter,
        broadcast_rx: SizedMessageReceiverHandle,
        buffer_capacity: usize,
        byte_counter: Option<Arc<ByteCounter>>,
        shutdown_token: CancellationToken,
    ) -> Self {
        let (sender, receiver) = mpsc::bounded_async::<Message>(buffer_capacity);

        let receiver_handle = tokio::spawn(async move {
            let mut msg_count = 0;
            let mut is_shutting_down = false;
            let mut broadcast_closed = false;
            let mut receiver_closed = false;

            loop {
                tokio::select! {
                    biased;

                    _ = shutdown_token.cancelled() => {
                        debug!("Receiver task shutting down via cancellation token");
                        break;
                    }
                    recv_result = broadcast_rx.recv(), if !broadcast_closed => {
                        match recv_result {
                            Ok(sized_msg) => {
                                msg_count += 1;
                                let msg_size = sized_msg.size;
                                let msg = Message::Text(sized_msg.bytes);

                                if let Err(e) = socket.send(msg).await {
                                    Self::log_connection_error(
                                        &e,
                                        SocketOperation::WriteFrame,
                                        msg_count,
                                        is_shutting_down,
                                    );
                                    break;
                                }

                                if let Some(ref counter) = byte_counter {
                                    counter.sub(msg_size);
                                }
                            }
                            Err(_) => {
                                broadcast_closed = true;
                            }
                        }
                    }
                    recv_result = receiver.recv(), if !receiver_closed => {
                        match recv_result {
                            Ok(message) => {
                                msg_count += 1;

                                if matches!(message, Message::Close(_)) {
                                    is_shutting_down = true;
                                }

                                if let Err(e) = socket.send(message).await {
                                    Self::log_connection_error(
                                        &e,
                                        SocketOperation::WriteFrame,
                                        msg_count,
                                        is_shutting_down,
                                    );
                                    break;
                                }
                            }
                            Err(_) => {
                                receiver_closed = true;
                            }
                        }
                    }
                    else => break,
                }
            }

            if let Err(e) = socket.close(1000, "Normal closure").await {
                Self::log_connection_error(&e, SocketOperation::SendCloseFrame, msg_count, true);
            }
        });

        Self {
            sender,
            receiver_handle: Some(receiver_handle),
        }
    }

    fn is_connection_error(error: &sockudo_ws::Error) -> bool {
        matches!(
            error,
            sockudo_ws::Error::ConnectionClosed
                | sockudo_ws::Error::ConnectionReset
                | sockudo_ws::Error::Closed(_)
                | sockudo_ws::Error::Io(_)
        )
    }

    fn log_connection_error(
        error: &sockudo_ws::Error,
        operation: SocketOperation,
        msg_count: usize,
        is_shutting_down: bool,
    ) {
        let is_conn_err = Self::is_connection_error(error);

        if is_conn_err && is_shutting_down {
            debug!("{} failed during shutdown (expected): {}", operation, error);
        } else if is_conn_err && msg_count <= 2 {
            warn!(
                "Early connection {} failed (after {} messages): {}",
                operation, msg_count, error
            );
        } else if is_conn_err {
            warn!(
                "Connection {} failed during operation (after {} messages): {}",
                operation, msg_count, error
            );
        } else if operation.is_close_operation() {
            warn!("Failed to {}: {}", operation, error);
        } else {
            error!("Failed to {}: {}", operation, error);
        }
    }

    pub fn new(mut socket: WebSocketWriter, buffer_capacity: usize) -> Self {
        let (sender, receiver) = mpsc::bounded_async::<Message>(buffer_capacity);

        let receiver_handle = tokio::spawn(async move {
            let mut msg_count = 0;
            let mut is_shutting_down = false;

            while let Ok(message) = receiver.recv().await {
                msg_count += 1;

                if matches!(message, Message::Close(_)) {
                    is_shutting_down = true;
                }

                if let Err(e) = socket.send(message).await {
                    Self::log_connection_error(
                        &e,
                        SocketOperation::WriteFrame,
                        msg_count,
                        is_shutting_down,
                    );
                    break;
                }
            }

            if let Err(e) = socket.close(1000, "Normal closure").await {
                Self::log_connection_error(&e, SocketOperation::SendCloseFrame, msg_count, true);
            }
        });

        Self {
            sender,
            receiver_handle: Some(receiver_handle),
        }
    }

    pub fn try_send(&self, message: Message) -> std::result::Result<(), TrySendError<Message>> {
        self.sender.try_send(message)
    }

    pub(crate) fn sender_handle(&self) -> MessageSenderHandle {
        self.sender.clone()
    }

    pub fn send(&self, message: Message) -> Result<()> {
        self.sender.try_send(message).map_err(|e| match e {
            TrySendError::Full(_) => Error::BufferFull("Message buffer is full".into()),
            TrySendError::Disconnected(_) => {
                Error::ConnectionClosed("Message channel closed".into())
            }
        })
    }

    pub fn send_json<T: serde::Serialize>(&self, message: &T) -> Result<()> {
        let payload = sonic_rs::to_string(message)
            .map_err(|e| Error::InvalidMessageFormat(format!("Serialization failed: {e}")))?;

        self.send(Message::text(payload))
    }

    pub fn send_text(&self, text: String) -> Result<()> {
        self.send(Message::text(text))
    }

    pub fn send_close(&self, code: u16, reason: &str) -> Result<()> {
        self.send(Message::Close(Some(sockudo_ws::error::CloseReason::new(
            code, reason,
        ))))
    }
}
