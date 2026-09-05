#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AblyRevocationRecord {
    target_type: String,
    target_value: String,
    issued_before: i64,
    applies_at: i64,
}

struct StoredAblyRevocation {
    app_id: String,
    record: AblyRevocationRecord,
    expires_at_ms: i64,
    bytes: usize,
}

struct AblyRevocationStore {
    entries: HashMap<String, StoredAblyRevocation>,
    bytes: usize,
    max_entries: usize,
    max_bytes: usize,
}

impl AblyRevocationStore {
    fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            entries: HashMap::new(),
            bytes: 0,
            max_entries,
            max_bytes,
        }
    }

    fn prune_expired(&mut self, now_ms: i64) {
        let mut removed_bytes = 0usize;
        self.entries.retain(|_, stored| {
            let retain = stored.expires_at_ms > now_ms;
            if !retain {
                removed_bytes = removed_bytes.saturating_add(stored.bytes);
            }
            retain
        });
        self.bytes = self.bytes.saturating_sub(removed_bytes);
    }

    fn insert(
        &mut self,
        app_id: &str,
        key: String,
        record: AblyRevocationRecord,
        expires_at_ms: i64,
        now_ms: i64,
    ) -> Result<(), AblyAuthError> {
        self.prune_expired(now_ms);
        let bytes = std::mem::size_of::<StoredAblyRevocation>()
            .checked_add(app_id.len())
            .and_then(|bytes| bytes.checked_add(key.len()))
            .and_then(|bytes| bytes.checked_add(record.target_type.len()))
            .and_then(|bytes| bytes.checked_add(record.target_value.len()))
            .ok_or_else(AblyAuthError::internal)?;
        let replaced_bytes = self.entries.get(&key).map_or(0, |stored| stored.bytes);
        let next_bytes = self
            .bytes
            .checked_sub(replaced_bytes)
            .and_then(|current| current.checked_add(bytes))
            .ok_or_else(AblyAuthError::internal)?;
        let next_entries = self.entries.len() + usize::from(!self.entries.contains_key(&key));
        if next_entries > self.max_entries || next_bytes > self.max_bytes {
            return Err(AblyAuthError::revocation_capacity());
        }
        self.entries.insert(
            key,
            StoredAblyRevocation {
                app_id: app_id.to_string(),
                record,
                expires_at_ms,
                bytes,
            },
        );
        self.bytes = next_bytes;
        Ok(())
    }

    fn get(&mut self, key: &str, now_ms: i64) -> Option<AblyRevocationRecord> {
        self.prune_expired(now_ms);
        self.entries.get(key).map(|stored| stored.record.clone())
    }

    fn records(&mut self, app_id: &str, now_ms: i64) -> Vec<AblyRevocationRecord> {
        self.prune_expired(now_ms);
        self.entries
            .values()
            .filter(|stored| stored.app_id == app_id)
            .map(|stored| stored.record.clone())
            .collect()
    }
}

impl Default for AblyRevocationStore {
    fn default() -> Self {
        Self::new(ABLY_REVOCATION_MAX_ENTRIES, ABLY_REVOCATION_MAX_BYTES)
    }
}

fn lock_revocations(
    revocations: &Mutex<AblyRevocationStore>,
) -> std::sync::MutexGuard<'_, AblyRevocationStore> {
    revocations
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn revocation_cache_key(app_id: &str, target_type: &str, target_value: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(target_type.as_bytes());
    digest.update([0]);
    digest.update(target_value.as_bytes());
    format!(
        "ably-compat:revocation:{app_id}:{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest.finalize())
    )
}