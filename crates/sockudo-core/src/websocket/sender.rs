use super::buffer::{
    ByteCounter, CloseFrame, MessageSenderHandle, OutboundFrame, SizedMessageSenderHandle,
};
use super::state::{DisconnectCause, SocketTermination};
use crate::error::{Error, Result};
use crossfire::{TrySendError, mpsc};
use sockudo_ws::Message;
use sockudo_ws::axum_integration::WebSocketWriter;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

// Message sender for async message handling
#[derive(Debug)]
pub struct MessageSender {
    sender: MessageSenderHandle,
    broadcast_sender: SizedMessageSenderHandle,
    termination: Arc<SocketTermination>,
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
        socket: WebSocketWriter,
        buffer_capacity: usize,
        byte_counter: Option<Arc<ByteCounter>>,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self::spawn(socket, buffer_capacity, byte_counter, Some(shutdown_token))
    }

    pub fn new(socket: WebSocketWriter, buffer_capacity: usize) -> Self {
        Self::spawn(socket, buffer_capacity, None, None)
    }

    fn spawn(
        mut socket: WebSocketWriter,
        buffer_capacity: usize,
        byte_counter: Option<Arc<ByteCounter>>,
        shutdown_token: Option<CancellationToken>,
    ) -> Self {
        let (outbound_sender, receiver) = mpsc::unbounded_async::<OutboundFrame>();
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
        let termination = Arc::new(SocketTermination::new(shutdown_token));
        let writer_termination = Arc::clone(&termination);
        let exit_guard = termination.writer_stopped.clone().drop_guard();

        let receiver_handle = tokio::spawn(async move {
            let shutdown = writer_termination.shutdown_token();
            let mut msg_count = 0;
            let mut sent_close = false;

            loop {
                let frame = match shutdown {
                    Some(token) => tokio::select! {
                        biased;
                        _ = token.cancelled() => break,
                        frame = receiver.recv() => frame,
                    },
                    None => receiver.recv().await,
                };
                let (message, size) = match frame {
                    Ok(OutboundFrame::Direct(message)) => {
                        direct_pending.fetch_sub(1, Ordering::Release);
                        (message, None)
                    }
                    Ok(OutboundFrame::Broadcast(message)) => {
                        broadcast_pending.fetch_sub(1, Ordering::Release);
                        let frame = if shutdown.is_some() && message.wire_format.is_binary() {
                            Message::Binary(message.bytes)
                        } else {
                            Message::Text(message.bytes)
                        };
                        (frame, Some(message.size))
                    }
                    Ok(OutboundFrame::Close(close)) => {
                        let CloseFrame {
                            message,
                            claimed,
                            written,
                        } = *close;
                        direct_pending.fetch_sub(1, Ordering::Release);
                        if shutdown.is_some_and(CancellationToken::is_cancelled)
                            || written.is_closed()
                            || claimed.swap(true, Ordering::AcqRel)
                        {
                            let _ = written.send(Err(Self::cancelled_close()));
                            continue;
                        }
                        sent_close = true;
                        Self::write_close(&mut socket, message, written, &writer_termination).await;
                        break;
                    }
                    Err(_) => break,
                };

                msg_count += 1;
                sent_close = matches!(message, Message::Close(_));
                if let Err(error) = socket.send(message).await {
                    let cause = writer_termination.record_writer_failure(&error);
                    let expected_close_race = cause.is_peer_or_transport_gone()
                        && writer_termination.close_requested.load(Ordering::Acquire);
                    Self::log_connection_error(
                        &error,
                        SocketOperation::WriteFrame,
                        msg_count,
                        sent_close || expected_close_race,
                    );
                    break;
                }
                if sent_close {
                    break;
                }
                if let (Some(size), Some(counter)) = (size, &byte_counter) {
                    counter.sub(size);
                }
            }

            // A queued Close cannot be written after this point. The queue may
            // retain its acknowledgment sender until all handles are dropped.
            drop(receiver);
            drop(exit_guard);
            if sent_close {
                return;
            }
            if let Some(token) = shutdown {
                if writer_termination.writer_failure().is_some_and(|cause| {
                    cause == DisconnectCause::TransportClosed || cause.is_native_timeout()
                }) && writer_termination.reader_attached.load(Ordering::Acquire)
                {
                    // A peer Close (or a read error) can be pending in the
                    // split reader's control handoff. Dropping the writer now
                    // cancels that driver and prevents the reader seeing it.
                    token.cancelled().await;
                    drop(socket);
                    return;
                }
                token.cancel();
            }
            if let Err(error) = socket.close(1000, "Normal closure").await {
                Self::log_connection_error(
                    &error,
                    SocketOperation::SendCloseFrame,
                    msg_count,
                    true,
                );
            }
        });

        Self {
            sender,
            broadcast_sender,
            termination,
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

        if !is_shutting_down
            && DisconnectCause::from_websocket_error(error) == DisconnectCause::TransportError
        {
            if let sockudo_ws::Error::Io(io) = error {
                error!(
                    operation = %operation,
                    reason = error.metric_name(),
                    io_kind = ?io.kind(),
                    os_error = io.raw_os_error(),
                    "websocket write failed"
                );
            } else {
                error!(operation = %operation, reason = error.metric_name(), "websocket write failed");
            }
        } else if is_conn_err && is_shutting_down {
            debug!(
                operation = %operation,
                reason = error.metric_name(),
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

    pub fn try_send(&self, message: Message) -> std::result::Result<(), TrySendError<Message>> {
        self.sender.try_send(message)
    }

    /// Abort the writer task without waiting for a queued frame to flush.
    /// Shutdown uses this when a close cannot finish within its budget.
    pub fn abort_writer(&mut self) {
        if let Some(handle) = self.receiver_handle.take() {
            handle.abort();
        }
    }

    pub(super) fn sender_handle(&self) -> MessageSenderHandle {
        self.sender.clone()
    }

    pub(super) fn termination(&self) -> &Arc<SocketTermination> {
        &self.termination
    }

    fn close_unavailable_error(&self) -> Option<Error> {
        self.termination.writer_close_error().or_else(|| {
            self.termination
                .writer_has_stopped()
                .then(|| Error::ConnectionClosed("WebSocket writer already stopped".into()))
        })
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
        if let Some(error) = self.close_unavailable_error() {
            return Err(error);
        }

        let (written, mut result) = oneshot::channel();
        let claimed = Arc::new(AtomicBool::new(false));
        let close = Message::Close(Some(sockudo_ws::error::CloseReason::new(code, reason)));
        if let Err(error) = self
            .sender
            .enqueue_close(close, Arc::clone(&claimed), written)
        {
            return Err(self.termination.writer_close_error().unwrap_or(error));
        }

        Self::wait_for_close(&self.termination, &claimed, &mut result).await
    }

    pub(super) async fn wait_for_close(
        termination: &SocketTermination,
        claimed: &AtomicBool,
        result: &mut oneshot::Receiver<Result<()>>,
    ) -> Result<()> {
        let shutdown = termination.shutdown_token();
        let (written, started) = tokio::select! {
            biased;
            written = &mut *result => (written.ok(), claimed.load(Ordering::Acquire)),
            _ = termination.wait_for_writer() => (result.try_recv().ok(), claimed.load(Ordering::Acquire)),
            _ = async {
                match shutdown {
                    Some(token) => token.cancelled().await,
                    None => std::future::pending::<()>().await,
                }
            } => {
                if claimed.swap(true, Ordering::AcqRel) {
                    // The writer owns the request now. EOF only stops dequeueing;
                    // it must not turn an in-flight flush into a failed Close.
                    (result.await.ok(), true)
                } else {
                    (result.try_recv().ok(), false)
                }
            }
        };
        written.unwrap_or_else(|| {
            Err(termination.writer_close_error().unwrap_or_else(|| {
                if !started && shutdown.is_some_and(CancellationToken::is_cancelled) {
                    Self::cancelled_close()
                } else {
                    Error::Connection("WebSocket writer stopped without acknowledging Close".into())
                }
            }))
        })
    }

    fn cancelled_close() -> Error {
        Error::ConnectionClosed("WebSocket cancelled before writing Close".into())
    }

    async fn write_close(
        socket: &mut WebSocketWriter,
        message: Message,
        written: oneshot::Sender<Result<()>>,
        termination: &SocketTermination,
    ) {
        let already_closed = socket.is_closed();
        let result = match socket.send(message).await {
            Err(sockudo_ws::Error::ConnectionClosed) if already_closed => Err(
                Error::ConnectionClosed("WebSocket transport already closed".into()),
            ),
            Err(error) => {
                termination.record_close_failure(&error);
                Err(Error::WebSocket(error))
            }
            Ok(()) => Ok(()),
        };
        if let Err(Err(Error::WebSocket(error))) = written.send(result)
            && !DisconnectCause::from_websocket_error(&error).is_confirmed_peer_loss()
        {
            warn!(
                reason = error.metric_name(),
                "unacknowledged websocket close write failed"
            );
        }
    }
}
