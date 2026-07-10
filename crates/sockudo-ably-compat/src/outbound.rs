//! Bounded, priority-aware outbound delivery for Ably WebSocket sessions.
//!
//! The adapter fanout path owns delivery eligibility and continuity.  This
//! module only owns the last hop: encoded bytes and the bounded queue for one
//! compatibility session.  Control frames are kept separate from data so a
//! slow data consumer cannot starve ACK, ERROR, or close frames.

use bytes::Bytes;
use std::sync::{
    Arc, Weak,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::sync::mpsc;

use crate::codec::encode_protocol_bytes;
use crate::protocol::{ACTION_ERROR, AblyFormat, AblyProtocolMessage, empty_protocol_message};

const DEFAULT_CONTROL_MESSAGES: usize = 32;
const DEFAULT_DATA_MESSAGES: usize = 256;
const DEFAULT_CONTROL_BYTES: usize = 64 * 1024;
const DEFAULT_DATA_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
pub(crate) struct OutboundLimits {
    pub(crate) control_messages: usize,
    pub(crate) data_messages: usize,
    pub(crate) control_bytes: usize,
    pub(crate) data_bytes: usize,
}

impl Default for OutboundLimits {
    fn default() -> Self {
        Self {
            control_messages: DEFAULT_CONTROL_MESSAGES,
            data_messages: DEFAULT_DATA_MESSAGES,
            control_bytes: DEFAULT_CONTROL_BYTES,
            data_bytes: DEFAULT_DATA_BYTES,
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct OutboundMetrics {
    pub(crate) queued_messages: AtomicUsize,
    pub(crate) queued_bytes: AtomicUsize,
    pub(crate) overflow: AtomicUsize,
    pub(crate) continuity_lost: AtomicUsize,
    pub(crate) encoded: AtomicUsize,
    pub(crate) fanout: AtomicUsize,
    pub(crate) replay_source: AtomicUsize,
    pub(crate) expiry: AtomicUsize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboundMetricsSnapshot {
    pub queued_messages: usize,
    pub queued_bytes: usize,
    pub overflow: usize,
    pub continuity_lost: usize,
    pub encoded: usize,
    pub fanout: usize,
    pub replay_source: usize,
    pub expiry: usize,
}

impl OutboundMetrics {
    pub(crate) fn snapshot(&self) -> OutboundMetricsSnapshot {
        OutboundMetricsSnapshot {
            queued_messages: self.queued_messages.load(Ordering::Acquire),
            queued_bytes: self.queued_bytes.load(Ordering::Acquire),
            overflow: self.overflow.load(Ordering::Acquire),
            continuity_lost: self.continuity_lost.load(Ordering::Acquire),
            encoded: self.encoded.load(Ordering::Acquire),
            fanout: self.fanout.load(Ordering::Acquire),
            replay_source: self.replay_source.load(Ordering::Acquire),
            expiry: self.expiry.load(Ordering::Acquire),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OutboundPriority {
    Control,
    Data,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OutboundSendError {
    Closed,
    ControlOverflow,
    DataOverflow,
    ContinuityLost,
}

impl std::fmt::Display for OutboundSendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Closed => "outbound queue closed",
            Self::ControlOverflow => "control queue overflow",
            Self::DataOverflow => "data queue overflow",
            Self::ContinuityLost => "outbound continuity lost",
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct OutboundFrame {
    pub(crate) bytes: Arc<Bytes>,
    pub(crate) priority: OutboundPriority,
}

#[derive(Debug)]
pub(crate) struct AblyOutbound {
    format: AblyFormat,
    control_tx: mpsc::Sender<OutboundFrame>,
    data_tx: mpsc::Sender<OutboundFrame>,
    control_messages: AtomicUsize,
    data_messages: AtomicUsize,
    control_bytes: AtomicUsize,
    data_bytes: AtomicUsize,
    limits: OutboundLimits,
    continuity_lost: AtomicBool,
    metrics: Arc<OutboundMetrics>,
}

#[derive(Debug)]
pub(crate) struct AblyOutboundReceiver {
    control_rx: mpsc::Receiver<OutboundFrame>,
    data_rx: mpsc::Receiver<OutboundFrame>,
    outbound: Weak<AblyOutbound>,
}

pub(crate) type AblySender = Arc<AblyOutbound>;

impl AblyOutbound {
    pub(crate) fn channel(
        format: AblyFormat,
        limits: OutboundLimits,
        metrics: Arc<OutboundMetrics>,
    ) -> (Arc<Self>, AblyOutboundReceiver) {
        let (control_tx, control_rx) = mpsc::channel(limits.control_messages.max(1));
        let (data_tx, data_rx) = mpsc::channel(limits.data_messages.max(1));
        let outbound = Arc::new(Self {
            format,
            control_tx,
            data_tx,
            control_messages: AtomicUsize::new(0),
            data_messages: AtomicUsize::new(0),
            control_bytes: AtomicUsize::new(0),
            data_bytes: AtomicUsize::new(0),
            limits,
            continuity_lost: AtomicBool::new(false),
            metrics,
        });
        let receiver = AblyOutboundReceiver {
            control_rx,
            data_rx,
            outbound: Arc::downgrade(&outbound),
        };
        (outbound, receiver)
    }

    pub(crate) fn format(&self) -> AblyFormat {
        self.format
    }

    pub(crate) fn send_protocol(
        &self,
        message: &AblyProtocolMessage,
        priority: OutboundPriority,
    ) -> Result<(), OutboundSendError> {
        let bytes =
            encode_protocol_bytes(message, self.format).map_err(|_| OutboundSendError::Closed)?;
        self.metrics.encoded.fetch_add(1, Ordering::Relaxed);
        self.try_send(Arc::new(bytes), priority)
    }

    pub(crate) fn send_data(&self, bytes: Arc<Bytes>) -> Result<(), OutboundSendError> {
        self.try_send(bytes, OutboundPriority::Data)
    }

    pub(crate) fn continuity_lost(&self) -> bool {
        self.continuity_lost.load(Ordering::Acquire)
    }

    fn try_send(
        &self,
        bytes: Arc<Bytes>,
        priority: OutboundPriority,
    ) -> Result<(), OutboundSendError> {
        if self.continuity_lost() && priority == OutboundPriority::Data {
            return Err(OutboundSendError::ContinuityLost);
        }

        let size = bytes.len();
        let (messages, queued_bytes, message_limit, byte_limit, tx) = match priority {
            OutboundPriority::Control => (
                &self.control_messages,
                &self.control_bytes,
                self.limits.control_messages,
                self.limits.control_bytes,
                &self.control_tx,
            ),
            OutboundPriority::Data => (
                &self.data_messages,
                &self.data_bytes,
                self.limits.data_messages,
                self.limits.data_bytes,
                &self.data_tx,
            ),
        };
        if message_limit == 0 || byte_limit < size {
            return self.overflow(priority);
        }
        if !reserve(messages, message_limit) {
            return self.overflow(priority);
        }
        if !reserve_bytes(queued_bytes, byte_limit, size) {
            messages.fetch_sub(1, Ordering::AcqRel);
            return self.overflow(priority);
        }

        let frame = OutboundFrame { bytes, priority };
        if tx.try_send(frame).is_err() {
            messages.fetch_sub(1, Ordering::AcqRel);
            queued_bytes.fetch_sub(size, Ordering::AcqRel);
            return Err(OutboundSendError::Closed);
        }
        self.metrics.queued_messages.fetch_add(1, Ordering::Relaxed);
        self.metrics.queued_bytes.fetch_add(size, Ordering::Relaxed);
        Ok(())
    }

    fn overflow(&self, priority: OutboundPriority) -> Result<(), OutboundSendError> {
        if priority == OutboundPriority::Data {
            self.continuity_lost.store(true, Ordering::Release);
            self.metrics.overflow.fetch_add(1, Ordering::Relaxed);
            self.metrics.continuity_lost.fetch_add(1, Ordering::Relaxed);
            let error = AblyProtocolMessage {
                action: ACTION_ERROR,
                error: Some(crate::protocol::AblyErrorInfo {
                    message: "outbound queue overflow; recover the channel from canonical history"
                        .to_string(),
                    code: 90003,
                    status_code: 400,
                }),
                ..empty_protocol_message(ACTION_ERROR)
            };
            let _ = self.send_protocol(&error, OutboundPriority::Control);
            Err(OutboundSendError::DataOverflow)
        } else {
            self.metrics.overflow.fetch_add(1, Ordering::Relaxed);
            Err(OutboundSendError::ControlOverflow)
        }
    }
}

impl AblyOutboundReceiver {
    pub(crate) async fn recv(&mut self) -> Option<OutboundFrame> {
        loop {
            tokio::select! {
                biased;
                frame = self.control_rx.recv() => {
                    if let Some(frame) = frame {
                        self.release(&frame);
                        return Some(frame);
                    }
                }
                frame = self.data_rx.recv() => {
                    if let Some(frame) = frame {
                        self.release(&frame);
                        return Some(frame);
                    }
                }
                else => return None,
            }
            if self.control_rx.is_closed() && self.data_rx.is_closed() {
                return None;
            }
        }
    }

    fn release(&self, frame: &OutboundFrame) {
        let Some(outbound) = self.outbound.upgrade() else {
            return;
        };
        let (messages, bytes) = match frame.priority {
            OutboundPriority::Control => (&outbound.control_messages, &outbound.control_bytes),
            OutboundPriority::Data => (&outbound.data_messages, &outbound.data_bytes),
        };
        messages.fetch_sub(1, Ordering::AcqRel);
        bytes.fetch_sub(frame.bytes.len(), Ordering::AcqRel);
        outbound
            .metrics
            .queued_messages
            .fetch_sub(1, Ordering::Relaxed);
        outbound
            .metrics
            .queued_bytes
            .fetch_sub(frame.bytes.len(), Ordering::Relaxed);
    }
}

fn reserve(counter: &AtomicUsize, limit: usize) -> bool {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            (current < limit).then_some(current + 1)
        })
        .is_ok()
}

fn reserve_bytes(counter: &AtomicUsize, limit: usize, size: usize) -> bool {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            (current.checked_add(size).is_some_and(|next| next <= limit)).then_some(current + size)
        })
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{AblyFormat, AblyProtocolMessage, empty_protocol_message};

    fn message(action: u8) -> AblyProtocolMessage {
        AblyProtocolMessage {
            action,
            ..empty_protocol_message(action)
        }
    }

    #[tokio::test]
    async fn control_frames_are_prioritized_over_data() {
        let (outbound, mut receiver) = AblyOutbound::channel(
            AblyFormat::Json,
            OutboundLimits {
                data_messages: 2,
                data_bytes: 1024,
                ..Default::default()
            },
            Arc::new(OutboundMetrics::default()),
        );
        let data = encode_protocol_bytes(&message(15), AblyFormat::Json).unwrap();
        outbound.send_data(Arc::new(data)).unwrap();
        outbound
            .send_protocol(&message(1), OutboundPriority::Control)
            .unwrap();

        let first = receiver.recv().await.unwrap();
        assert_eq!(first.priority, OutboundPriority::Control);
    }

    #[tokio::test]
    async fn data_overflow_marks_continuity_and_emits_recoverable_error() {
        let (outbound, mut receiver) = AblyOutbound::channel(
            AblyFormat::Json,
            OutboundLimits {
                data_messages: 1,
                data_bytes: 64,
                ..Default::default()
            },
            Arc::new(OutboundMetrics::default()),
        );
        let data = Arc::new(Bytes::from_static(b"0123456789"));
        outbound.send_data(data.clone()).unwrap();
        assert_eq!(
            outbound.send_data(data),
            Err(OutboundSendError::DataOverflow)
        );
        assert!(outbound.continuity_lost());

        let frame = receiver.recv().await.unwrap();
        assert_eq!(frame.priority, OutboundPriority::Control);
        let decoded: AblyProtocolMessage = sonic_rs::from_slice(frame.bytes.as_ref()).unwrap();
        assert_eq!(decoded.action, ACTION_ERROR);
        assert_eq!(decoded.error.as_ref().map(|error| error.code), Some(90003));
    }

    #[test]
    fn reserve_bytes_is_atomic_at_the_limit() {
        let counter = AtomicUsize::new(0);
        assert!(reserve_bytes(&counter, 10, 10));
        assert!(!reserve_bytes(&counter, 10, 1));
        assert_eq!(counter.load(Ordering::Acquire), 10);
    }
}
