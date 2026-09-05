//! Count- and byte-bounded, ordered admission for broker callbacks.
use crate::horizontal_transport::BoxFuture;
use sockudo_core::error::{Error, Result};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinHandle;

pub(crate) const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
const MAX_PENDING_BYTES: usize = 64 * 1024 * 1024;
const RECORDS_PER_SHARD: usize = 64;

struct Work {
    future: BoxFuture<'static, ()>,
    _bytes: OwnedSemaphorePermit,
}

pub(crate) struct OrderedDispatcher {
    shards: Vec<mpsc::Sender<Work>>,
    bytes: Arc<Semaphore>,
    workers: Vec<JoinHandle<()>>,
}

impl OrderedDispatcher {
    pub(crate) fn new(shards: usize) -> Self {
        let mut senders = Vec::with_capacity(shards);
        let mut workers = Vec::with_capacity(shards);
        for _ in 0..shards {
            let (sender, mut receiver) = mpsc::channel::<Work>(RECORDS_PER_SHARD);
            senders.push(sender);
            workers.push(tokio::spawn(async move {
                while let Some(work) = receiver.recv().await {
                    work.future.await;
                    // Keep the byte permit until the handler and its payload finish.
                }
            }));
        }
        Self {
            shards: senders,
            bytes: Arc::new(Semaphore::new(MAX_PENDING_BYTES)),
            workers,
        }
    }

    pub(crate) async fn dispatch(
        &self,
        key: u64,
        bytes: usize,
        future: BoxFuture<'static, ()>,
    ) -> Result<()> {
        validate_frame_size(bytes)?;
        let permit = Arc::clone(&self.bytes)
            .acquire_many_owned(bytes.max(1) as u32)
            .await
            .map_err(|error| Error::Internal(format!("Ingress byte admission closed: {error}")))?;
        self.shards[key as usize % self.shards.len()]
            .send(Work {
                future,
                _bytes: permit,
            })
            .await
            .map_err(|_| Error::ConnectionClosed("Ingress worker closed".into()))
    }

    // Pub/Sub has no upstream acknowledgement. Never block its multiplexed
    // receive loop behind data work: responses must retain a path to admission.
    #[cfg(feature = "redis")]
    pub(crate) fn try_dispatch(
        &self,
        key: u64,
        bytes: usize,
        future: BoxFuture<'static, ()>,
    ) -> Result<()> {
        validate_frame_size(bytes)?;
        let permit = Arc::clone(&self.bytes)
            .try_acquire_many_owned(bytes.max(1) as u32)
            .map_err(|_| Error::Connection("Ingress byte capacity exhausted".into()))?;
        self.shards[key as usize % self.shards.len()]
            .try_send(Work {
                future,
                _bytes: permit,
            })
            .map_err(|_| Error::Connection("Ingress record capacity exhausted".into()))
    }

    pub(crate) async fn drain(mut self) {
        self.shards.clear();
        for worker in self.workers {
            if let Err(error) = worker.await {
                tracing::error!(error = %error, "ingress worker failed during drain");
            }
        }
    }
}

pub(crate) fn validate_frame_size(bytes: usize) -> Result<()> {
    if bytes > MAX_FRAME_BYTES {
        return Err(Error::InvalidMessageFormat(
            "Horizontal frame exceeds 16 MiB ingress limit".into(),
        ));
    }
    Ok(())
}

/// Length-separated stable app/channel identity. It is only a routing choice;
/// the handler still performs its usual full validation and authorization.
#[cfg(any(feature = "redis", feature = "nats", test))]
pub(crate) fn routing_key(payload: &[u8]) -> u64 {
    #[derive(serde::Deserialize)]
    struct Route<'a> {
        #[serde(borrow)]
        app_id: Option<&'a str>,
        #[serde(borrow)]
        channel: Option<&'a str>,
    }
    let route = sonic_rs::from_slice::<Route<'_>>(payload).ok();
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for part in [
        route.as_ref().and_then(|v| v.app_id).unwrap_or(""),
        route.as_ref().and_then(|v| v.channel).unwrap_or(""),
    ] {
        for byte in (part.len() as u64)
            .to_le_bytes()
            .iter()
            .chain(part.as_bytes())
        {
            hash = (hash ^ u64::from(*byte)).wrapping_mul(0x100_0000_01b3);
        }
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn preserves_channel_order_and_drains_accepted_work() {
        let dispatcher = OrderedDispatcher::new(4);
        let seen = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        for serial in 0..100 {
            let seen = Arc::clone(&seen);
            dispatcher
                .dispatch(
                    7,
                    10,
                    Box::pin(async move {
                        seen.lock().await.push(serial);
                    }),
                )
                .await
                .unwrap();
        }
        dispatcher.drain().await;
        assert_eq!(*seen.lock().await, (0..100).collect::<Vec<_>>());
    }
    #[tokio::test]
    async fn byte_admission_waits_until_handler_releases_payload() {
        let dispatcher = OrderedDispatcher::new(4);
        let gate = Arc::new(Semaphore::new(0));
        for key in 0..4 {
            let gate = Arc::clone(&gate);
            dispatcher
                .dispatch(
                    key,
                    MAX_FRAME_BYTES,
                    Box::pin(async move {
                        gate.acquire().await.unwrap().forget();
                    }),
                )
                .await
                .unwrap();
        }
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(10),
                dispatcher.dispatch(9, 1, Box::pin(async {}))
            )
            .await
            .is_err()
        );
        gate.add_permits(4);
        dispatcher.dispatch(9, 1, Box::pin(async {})).await.unwrap();
        dispatcher.drain().await;
    }
    #[test]
    fn route_separates_app_and_channel_boundaries() {
        assert_ne!(
            routing_key(br#"{"app_id":"ab","channel":"c"}"#),
            routing_key(br#"{"app_id":"a","channel":"bc"}"#)
        );
    }
}

#[cfg(any(feature = "redis", feature = "nats"))]
pub(crate) fn ingress_scope(payload: &[u8]) -> (Option<String>, Option<String>) {
    #[derive(serde::Deserialize)]
    struct Scope {
        app_id: Option<String>,
        channel: Option<String>,
    }
    sonic_rs::from_slice::<Scope>(payload)
        .map(|scope| (scope.app_id, scope.channel))
        .unwrap_or_default()
}

#[cfg(feature = "redis")]
pub(crate) fn report_ingress_gap(
    handler: &crate::horizontal_transport::IngressGapHandler,
    payload: Option<&[u8]>,
) {
    #[derive(serde::Deserialize)]
    struct Scope<'a> {
        #[serde(borrow)]
        app_id: Option<&'a str>,
        #[serde(borrow)]
        channel: Option<&'a str>,
    }
    let scope = payload.and_then(|bytes| sonic_rs::from_slice::<Scope<'_>>(bytes).ok());
    handler(
        scope.as_ref().and_then(|scope| scope.app_id),
        scope.as_ref().and_then(|scope| scope.channel),
    );
}

#[cfg(feature = "redis")]
pub(crate) struct RedisPush {
    pub message: redis::PushInfo,
    _permit: OwnedSemaphorePermit,
}

#[cfg(feature = "redis")]
pub(crate) fn bounded_redis_pushes(
    gap: crate::horizontal_transport::IngressGapHandler,
) -> (impl redis::aio::AsyncPushSender, mpsc::Receiver<RedisPush>) {
    let (sender, receiver) = mpsc::channel(256);
    let budget = Arc::new(Semaphore::new(MAX_PENDING_BYTES));
    let callback =
        move |message: redis::PushInfo| -> std::result::Result<(), redis::aio::SendError> {
            let payload = message.data.get(1).and_then(redis_value_bytes);
            let size = payload.map_or(128, |bytes| bytes.len().saturating_add(128));
            let permit = u32::try_from(size)
                .ok()
                .and_then(|size| Arc::clone(&budget).try_acquire_many_owned(size).ok());
            let Some(permit) = permit else {
                report_ingress_gap(&gap, payload);
                tracing::error!(
                    adapter = "redis",
                    "redis push byte admission failed continuity invalidated"
                );
                return Err(redis::aio::SendError);
            };
            match sender.try_send(RedisPush {
                message,
                _permit: permit,
            }) {
                Ok(()) => Ok(()),
                Err(error) => {
                    let rejected = error.into_inner();
                    report_ingress_gap(
                        &gap,
                        rejected.message.data.get(1).and_then(redis_value_bytes),
                    );
                    tracing::error!(
                        adapter = "redis",
                        "redis push queue admission failed continuity invalidated"
                    );
                    Err(redis::aio::SendError)
                }
            }
        };
    (callback, receiver)
}

#[cfg(feature = "redis")]
pub(crate) fn redis_value_bytes(value: &redis::Value) -> Option<&[u8]> {
    match value {
        redis::Value::BulkString(value) => Some(value),
        redis::Value::SimpleString(value) => Some(value.as_bytes()),
        _ => None,
    }
}

#[cfg(all(test, feature = "redis"))]
mod redis_admission_tests {
    use super::*;
    use redis::aio::AsyncPushSender;
    use std::sync::Mutex;

    fn push() -> redis::PushInfo {
        redis::PushInfo {
            kind: redis::PushKind::Message,
            data: vec![
                redis::Value::BulkString(b"test:#broadcast".to_vec()),
                redis::Value::BulkString(br#"{"app_id":"app","channel":"channel"}"#.to_vec()),
            ],
        }
    }

    #[tokio::test]
    async fn callback_bounds_records_and_invalidates_only_rejected_scope() {
        let gaps = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&gaps);
        let (sender, mut receiver) = bounded_redis_pushes(Arc::new(move |app, channel| {
            observed
                .lock()
                .unwrap()
                .push((app.map(str::to_owned), channel.map(str::to_owned)));
        }));
        for _ in 0..256 {
            assert!(sender.send(push()).is_ok());
        }
        assert!(sender.send(push()).is_err());
        assert_eq!(
            *gaps.lock().unwrap(),
            vec![(Some("app".into()), Some("channel".into()))]
        );
        for _ in 0..256 {
            assert!(receiver.recv().await.is_some());
        }
        assert!(sender.send(push()).is_ok());
        assert!(receiver.recv().await.is_some());
    }

    #[tokio::test]
    async fn full_data_lane_does_not_block_control_or_discard_accepted_records() {
        let data = OrderedDispatcher::new(1);
        let control = OrderedDispatcher::new(1);
        let gate = Arc::new(Semaphore::new(0));
        let completed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        for _ in 0..64 {
            let gate = Arc::clone(&gate);
            let completed = Arc::clone(&completed);
            data.try_dispatch(
                0,
                16,
                Box::pin(async move {
                    gate.acquire().await.unwrap().forget();
                    completed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }),
            )
            .unwrap();
        }
        assert!(data.try_dispatch(0, 16, Box::pin(async {})).is_err());
        let (sender, received) = tokio::sync::oneshot::channel();
        control
            .try_dispatch(
                0,
                16,
                Box::pin(async move {
                    sender.send(()).unwrap();
                }),
            )
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_millis(100), received)
            .await
            .unwrap()
            .unwrap();
        gate.add_permits(64);
        data.drain().await;
        control.drain().await;
        assert_eq!(completed.load(std::sync::atomic::Ordering::SeqCst), 64);
    }
}
