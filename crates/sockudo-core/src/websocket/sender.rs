use super::buffer::{ByteCounter, MessageSenderHandle, OutboundFrame, SizedMessageSenderHandle};
use crate::error::{Error, Result};
use crossfire::{TrySendError, mpsc};
use sockudo_ws::Message;
use sockudo_ws::axum_integration::WebSocketWriter;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

// Message sender for async message handling
#[derive(Debug)]
pub struct MessageSender {
    sender: MessageSenderHandle,
    broadcast_sender: SizedMessageSenderHandle,
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
        buffer_capacity: usize,
        byte_counter: Option<Arc<ByteCounter>>,
        shutdown_token: CancellationToken,
    ) -> Self {
        let combined_capacity = buffer_capacity.saturating_mul(2);
        let (outbound_sender, receiver) = mpsc::bounded_async::<OutboundFrame>(combined_capacity);
        let direct_pending = Arc::new(AtomicUsize::new(0));
        let broadcast_pending = Arc::new(AtomicUsize::new(0));
        let sender = MessageSenderHandle::new(
            outbound_sender.clone(),
            Arc::clone(&direct_pending),
            buffer_capacity,
        );
        let broadcast_sender = SizedMessageSenderHandle::new(
            outbound_sender,
            Arc::clone(&broadcast_pending),
            buffer_capacity,
        );
        let close_flushed = Arc::new(Notify::new());
        let writer_close_flushed = Arc::clone(&close_flushed);
        let close_shutdown_token = shutdown_token.clone();

        let receiver_handle = tokio::spawn(async move {
            let mut msg_count = 0;
            let mut is_shutting_down = false;

            loop {
                tokio::select! {
                    biased;

                    _ = shutdown_token.cancelled() => {
                        debug!("Receiver task shutting down via cancellation token");
                        break;
                    }
                    recv_result = receiver.recv() => {
                        match recv_result {
                            Ok(OutboundFrame::Direct(message)) => {
                                direct_pending.fetch_sub(1, Ordering::Release);
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
                            Ok(OutboundFrame::Broadcast(sized_msg)) => {
                                broadcast_pending.fetch_sub(1, Ordering::Release);
                                msg_count += 1;
                                let msg_size = sized_msg.size;
                                let msg = sized_msg.into_frame();

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
                            Err(_) => break,
                        }
                    }
                }
            }

            if !is_shutting_down && let Err(e) = socket.close(1000, "Normal closure").await {
                Self::log_connection_error(&e, SocketOperation::SendCloseFrame, msg_count, true);
            }
        });

        Self {
            sender,
            broadcast_sender,
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
                | sockudo_ws::Error::HeartbeatTimeout
                | sockudo_ws::Error::IdleTimeout
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
        let combined_capacity = buffer_capacity.saturating_mul(2);
        let (outbound_sender, receiver) = mpsc::bounded_async::<OutboundFrame>(combined_capacity);
        let direct_pending = Arc::new(AtomicUsize::new(0));
        let broadcast_pending = Arc::new(AtomicUsize::new(0));
        let sender = MessageSenderHandle::new(
            outbound_sender.clone(),
            Arc::clone(&direct_pending),
            buffer_capacity,
        );
        let broadcast_sender = SizedMessageSenderHandle::new(
            outbound_sender,
            Arc::clone(&broadcast_pending),
            buffer_capacity,
        );
        let close_flushed = Arc::new(Notify::new());
        let writer_close_flushed = Arc::clone(&close_flushed);

        let receiver_handle = tokio::spawn(async move {
            let mut msg_count = 0;
            let mut is_shutting_down = false;

            while let Ok(frame) = receiver.recv().await {
                match frame {
                    OutboundFrame::Direct(message) => {
                        direct_pending.fetch_sub(1, Ordering::Release);
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
                    OutboundFrame::Broadcast(sized_msg) => {
                        broadcast_pending.fetch_sub(1, Ordering::Release);
                        msg_count += 1;
                        if let Err(e) = socket.send(sized_msg.into_frame()).await {
                            Self::log_connection_error(
                                &e,
                                SocketOperation::WriteFrame,
                                msg_count,
                                is_shutting_down,
                            );
                            break;
                        }
                    }
                }
            }

            if !is_shutting_down && let Err(e) = socket.close(1000, "Normal closure").await {
                Self::log_connection_error(&e, SocketOperation::SendCloseFrame, msg_count, true);
            }
        });

        Self {
            sender,
            broadcast_sender,
            close_flushed,
            shutdown_token: None,
            receiver_handle: Some(receiver_handle),
        }
    }

    pub fn try_send(&self, message: Message) -> std::result::Result<(), TrySendError<Message>> {
        self.sender.try_send(message)
    }

    pub(super) fn sender_handle(&self) -> MessageSenderHandle {
        self.sender.clone()
    }

    pub(crate) fn broadcast_sender_handle(&self) -> SizedMessageSenderHandle {
        self.broadcast_sender.clone()
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
