use super::buffer::{ByteCounter, MessageSenderHandle, SizedMessageReceiverHandle};
use crate::error::{Error, Result};
use crossfire::{TrySendError, mpsc};
use sockudo_ws::Message;
use sockudo_ws::axum_integration::WebSocketWriter;
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

// Message sender for async message handling
#[derive(Debug)]
pub struct MessageSender {
    sender: MessageSenderHandle,
    close_flushed: Arc<Notify>,
    shutdown_token: Option<CancellationToken>,
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
        let close_flushed = Arc::new(Notify::new());
        let writer_close_flushed = Arc::clone(&close_flushed);
        let close_shutdown_token = shutdown_token.clone();

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

                                let is_close = matches!(message, Message::Close(_));
                                if is_close {
                                    is_shutting_down = true;
                                }

                                let send_result = socket.send(message).await;
                                if is_close {
                                    writer_close_flushed.notify_one();
                                }
                                if let Err(e) = send_result {
                                    Self::log_connection_error(
                                        &e,
                                        SocketOperation::WriteFrame,
                                        msg_count,
                                        is_shutting_down,
                                    );
                                    break;
                                }
                                if is_close {
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

            if !is_shutting_down && let Err(e) = socket.close(1000, "Normal closure").await {
                Self::log_connection_error(&e, SocketOperation::SendCloseFrame, msg_count, true);
            }
        });

        Self {
            sender,
            close_flushed,
            shutdown_token: Some(close_shutdown_token),
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
            debug!(
                operation = %operation,
                error = %error,
                "connection error during shutdown (expected)"
            );
        } else if is_conn_err && msg_count <= 2 {
            warn!(
                operation = %operation,
                msg_count = msg_count,
                error = %error,
                "early connection error"
            );
        } else if is_conn_err {
            warn!(
                operation = %operation,
                msg_count = msg_count,
                error = %error,
                "connection error during operation"
            );
        } else if operation.is_close_operation() {
            warn!(operation = %operation, error = %error, "close frame send failed");
        } else {
            error!(operation = %operation, error = %error, "websocket write failed");
        }
    }

    pub fn new(mut socket: WebSocketWriter, buffer_capacity: usize) -> Self {
        let (sender, receiver) = mpsc::bounded_async::<Message>(buffer_capacity);
        let close_flushed = Arc::new(Notify::new());
        let writer_close_flushed = Arc::clone(&close_flushed);

        let receiver_handle = tokio::spawn(async move {
            let mut msg_count = 0;
            let mut is_shutting_down = false;

            while let Ok(message) = receiver.recv().await {
                msg_count += 1;

                let is_close = matches!(message, Message::Close(_));
                if is_close {
                    is_shutting_down = true;
                }

                let send_result = socket.send(message).await;
                if is_close {
                    writer_close_flushed.notify_one();
                }
                if let Err(e) = send_result {
                    Self::log_connection_error(
                        &e,
                        SocketOperation::WriteFrame,
                        msg_count,
                        is_shutting_down,
                    );
                    break;
                }
                if is_close {
                    break;
                }
            }

            if !is_shutting_down && let Err(e) = socket.close(1000, "Normal closure").await {
                Self::log_connection_error(&e, SocketOperation::SendCloseFrame, msg_count, true);
            }
        });

        Self {
            sender,
            close_flushed,
            shutdown_token: None,
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

    pub async fn send_close(&self, code: u16, reason: &str) -> Result<()> {
        let flushed = self.close_flushed.notified();
        self.send(Message::Close(Some(sockudo_ws::error::CloseReason::new(
            code, reason,
        ))))?;

        if let Some(shutdown_token) = &self.shutdown_token {
            tokio::select! {
                biased;
                _ = flushed => Ok(()),
                _ = shutdown_token.cancelled() => Err(Error::ConnectionClosed(
                    "WebSocket writer stopped before flushing close frame".into(),
                )),
            }
        } else {
            flushed.await;
            Ok(())
        }
    }
}
