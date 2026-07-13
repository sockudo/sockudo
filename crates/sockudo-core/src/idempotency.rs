//! Cross-node idempotency claims and durable publish receipts.

use crate::{
    cache::CacheManager,
    error::{Error, Result},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::Duration;
use uuid::Uuid;

const WAIT_ATTEMPTS: usize = 40;
const WAIT_INTERVAL: Duration = Duration::from_millis(25);
const RECOVERY_CAS_ATTEMPTS: usize = 8;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IdempotencyReceipt {
    pub acknowledgement_id: String,
    pub message_serial: Option<String>,
    pub history_serial: Option<u64>,
    pub delivery_serial: Option<u64>,
    pub version_serial: Option<String>,
}

#[derive(Debug, Clone)]
pub struct IdempotencyClaim {
    cache_key: String,
    pending_value: String,
    fingerprint: String,
    ttl_seconds: u64,
}

#[derive(Debug, Clone)]
pub enum IdempotencyStart {
    Acquired(IdempotencyClaim),
    Replay(IdempotencyReceipt),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum IdempotencyRecord {
    Pending {
        fingerprint: String,
        owner: String,
    },
    Committed {
        fingerprint: String,
        receipt: IdempotencyReceipt,
    },
}

pub fn publish_fingerprint(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

pub fn publish_idempotency_cache_key(app_id: &str, channel: &str, message_id: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(app_id.as_bytes());
    digest.update([0]);
    digest.update(channel.as_bytes());
    digest.update([0]);
    digest.update(message_id.as_bytes());
    format!("idempotency:publish:v1:{}", hex::encode(digest.finalize()))
}

pub async fn begin_publish(
    cache: &dyn CacheManager,
    cache_key: String,
    fingerprint: String,
    ttl_seconds: u64,
) -> Result<IdempotencyStart> {
    let pending = IdempotencyRecord::Pending {
        fingerprint: fingerprint.clone(),
        owner: Uuid::new_v4().to_string(),
    };
    let pending_value = sonic_rs::to_string(&pending)?;
    let ttl_seconds = ttl_seconds.max(1);

    if cache
        .set_if_not_exists(&cache_key, &pending_value, ttl_seconds)
        .await?
    {
        return Ok(IdempotencyStart::Acquired(IdempotencyClaim {
            cache_key,
            pending_value,
            fingerprint,
            ttl_seconds,
        }));
    }

    for _ in 0..WAIT_ATTEMPTS {
        let Some(value) = cache.get(&cache_key).await? else {
            if cache
                .set_if_not_exists(&cache_key, &pending_value, ttl_seconds)
                .await?
            {
                return Ok(IdempotencyStart::Acquired(IdempotencyClaim {
                    cache_key,
                    pending_value,
                    fingerprint,
                    ttl_seconds,
                }));
            }
            tokio::time::sleep(WAIT_INTERVAL).await;
            continue;
        };
        let record: IdempotencyRecord = sonic_rs::from_str(&value)
            .map_err(|error| Error::Cache(format!("invalid idempotency record: {error}")))?;
        match record {
            IdempotencyRecord::Committed {
                fingerprint: existing,
                receipt,
            } if existing == fingerprint => return Ok(IdempotencyStart::Replay(receipt)),
            IdempotencyRecord::Pending {
                fingerprint: existing,
                ..
            } if existing == fingerprint => tokio::time::sleep(WAIT_INTERVAL).await,
            IdempotencyRecord::Committed {
                fingerprint: existing,
                ..
            }
            | IdempotencyRecord::Pending {
                fingerprint: existing,
                ..
            } => {
                tracing::debug!(
                    existing_fingerprint = %existing,
                    incoming_fingerprint = %fingerprint,
                    "Idempotency key payload conflict"
                );
                return Err(Error::IdempotencyConflict);
            }
        }
    }

    Err(Error::IdempotencyInProgress)
}

pub async fn commit_publish(
    cache: &dyn CacheManager,
    claim: &IdempotencyClaim,
    receipt: &IdempotencyReceipt,
) -> Result<()> {
    let committed = IdempotencyRecord::Committed {
        fingerprint: claim.fingerprint.clone(),
        receipt: receipt.clone(),
    };
    let committed_value = sonic_rs::to_string(&committed)?;
    if cache
        .compare_and_swap(
            &claim.cache_key,
            &claim.pending_value,
            &committed_value,
            claim.ttl_seconds,
        )
        .await?
    {
        return Ok(());
    }

    match cache.get(&claim.cache_key).await? {
        Some(value) => {
            let existing: IdempotencyRecord = sonic_rs::from_str(&value)
                .map_err(|error| Error::Cache(format!("invalid idempotency record: {error}")))?;
            match existing {
                IdempotencyRecord::Committed {
                    fingerprint,
                    receipt: existing_receipt,
                } if fingerprint == claim.fingerprint && existing_receipt == *receipt => Ok(()),
                IdempotencyRecord::Pending { fingerprint, .. }
                    if fingerprint == claim.fingerprint =>
                {
                    Err(Error::IdempotencyInProgress)
                }
                _ => Err(Error::IdempotencyConflict),
            }
        }
        None => Err(Error::IdempotencyInProgress),
    }
}

/// Finish an idempotency record from an authoritative durable publish receipt.
///
/// This operation is safe to invoke while the original owner is still alive:
/// callers may supply a receipt only after reading the matching payload
/// fingerprint from durable history or the version store. The compare-and-swap
/// then makes that durable receipt the single cross-node acknowledgement.
pub async fn commit_recovered_publish(
    cache: &dyn CacheManager,
    cache_key: &str,
    fingerprint: &str,
    receipt: &IdempotencyReceipt,
    ttl_seconds: u64,
) -> Result<IdempotencyReceipt> {
    let ttl_seconds = ttl_seconds.max(1);
    let committed = IdempotencyRecord::Committed {
        fingerprint: fingerprint.to_string(),
        receipt: receipt.clone(),
    };
    let committed_value = sonic_rs::to_string(&committed)?;

    for _ in 0..RECOVERY_CAS_ATTEMPTS {
        let Some(value) = cache.get(cache_key).await? else {
            if cache
                .set_if_not_exists(cache_key, &committed_value, ttl_seconds)
                .await?
            {
                return Ok(receipt.clone());
            }
            continue;
        };
        let record: IdempotencyRecord = sonic_rs::from_str(&value)
            .map_err(|error| Error::Cache(format!("invalid idempotency record: {error}")))?;
        match record {
            IdempotencyRecord::Committed {
                fingerprint: existing,
                receipt: existing_receipt,
            } if existing == fingerprint && existing_receipt == *receipt => {
                return Ok(existing_receipt);
            }
            IdempotencyRecord::Pending {
                fingerprint: existing,
                ..
            } if existing == fingerprint => {
                if cache
                    .compare_and_swap(cache_key, &value, &committed_value, ttl_seconds)
                    .await?
                {
                    return Ok(receipt.clone());
                }
            }
            IdempotencyRecord::Committed { .. } | IdempotencyRecord::Pending { .. } => {
                return Err(Error::IdempotencyConflict);
            }
        }
    }

    Err(Error::IdempotencyInProgress)
}

pub async fn abort_publish(cache: &dyn CacheManager, claim: &IdempotencyClaim) -> Result<()> {
    let _ = cache
        .compare_and_remove(&claim.cache_key, &claim.pending_value)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
        time::Duration,
    };

    #[derive(Default)]
    struct TestCache(Mutex<HashMap<String, String>>);

    #[async_trait]
    impl CacheManager for TestCache {
        async fn has(&self, key: &str) -> Result<bool> {
            Ok(self.0.lock().unwrap().contains_key(key))
        }
        async fn get(&self, key: &str) -> Result<Option<String>> {
            Ok(self.0.lock().unwrap().get(key).cloned())
        }
        async fn set(&self, key: &str, value: &str, _: u64) -> Result<()> {
            self.0
                .lock()
                .unwrap()
                .insert(key.to_string(), value.to_string());
            Ok(())
        }
        async fn remove(&self, key: &str) -> Result<()> {
            self.0.lock().unwrap().remove(key);
            Ok(())
        }
        async fn disconnect(&self) -> Result<()> {
            Ok(())
        }
        async fn ttl(&self, _: &str) -> Result<Option<Duration>> {
            Ok(None)
        }
        async fn set_if_not_exists(&self, key: &str, value: &str, _: u64) -> Result<bool> {
            let mut values = self.0.lock().unwrap();
            if values.contains_key(key) {
                return Ok(false);
            }
            values.insert(key.to_string(), value.to_string());
            Ok(true)
        }
        async fn compare_and_swap(
            &self,
            key: &str,
            expected: &str,
            value: &str,
            _: u64,
        ) -> Result<bool> {
            let mut values = self.0.lock().unwrap();
            if values.get(key).map(String::as_str) != Some(expected) {
                return Ok(false);
            }
            values.insert(key.to_string(), value.to_string());
            Ok(true)
        }
        async fn compare_and_remove(&self, key: &str, expected: &str) -> Result<bool> {
            let mut values = self.0.lock().unwrap();
            if values.get(key).map(String::as_str) != Some(expected) {
                return Ok(false);
            }
            values.remove(key);
            Ok(true)
        }
    }

    fn receipt() -> IdempotencyReceipt {
        IdempotencyReceipt {
            acknowledgement_id: "ack-7".to_string(),
            message_serial: Some("message-7".to_string()),
            history_serial: Some(7),
            delivery_serial: Some(8),
            version_serial: Some("version-7".to_string()),
        }
    }

    #[tokio::test]
    async fn identical_retry_observes_the_original_receipt() {
        let cache = Arc::new(TestCache::default());
        let key = publish_idempotency_cache_key("app", "channel", "message");
        let IdempotencyStart::Acquired(claim) =
            begin_publish(cache.as_ref(), key.clone(), "same".into(), 60)
                .await
                .unwrap()
        else {
            panic!("first caller must own claim")
        };
        let retry_cache = Arc::clone(&cache);
        let retry = tokio::spawn(async move {
            begin_publish(retry_cache.as_ref(), key, "same".into(), 60).await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        commit_publish(cache.as_ref(), &claim, &receipt())
            .await
            .unwrap();

        let IdempotencyStart::Replay(replayed) = retry.await.unwrap().unwrap() else {
            panic!("retry must replay")
        };
        assert_eq!(replayed, receipt());
    }

    #[tokio::test]
    async fn same_id_with_different_payload_is_a_conflict() {
        let cache = TestCache::default();
        let key = publish_idempotency_cache_key("app", "channel", "message");
        let IdempotencyStart::Acquired(claim) =
            begin_publish(&cache, key.clone(), "one".into(), 60)
                .await
                .unwrap()
        else {
            panic!("first caller must own claim")
        };
        commit_publish(&cache, &claim, &receipt()).await.unwrap();

        assert!(matches!(
            begin_publish(&cache, key, "two".into(), 60).await,
            Err(Error::IdempotencyConflict)
        ));
    }

    #[tokio::test]
    async fn durable_receipt_recovers_an_abandoned_pending_claim() {
        let cache = TestCache::default();
        let key = publish_idempotency_cache_key("app", "channel", "message");
        let IdempotencyStart::Acquired(_claim) =
            begin_publish(&cache, key.clone(), "same".into(), 60)
                .await
                .unwrap()
        else {
            panic!("first caller must own claim")
        };

        let recovered = commit_recovered_publish(&cache, &key, "same", &receipt(), 60)
            .await
            .unwrap();
        assert_eq!(recovered, receipt());

        let IdempotencyStart::Replay(replayed) =
            begin_publish(&cache, key, "same".into(), 60).await.unwrap()
        else {
            panic!("recovered claim must replay")
        };
        assert_eq!(replayed, receipt());
    }
}
