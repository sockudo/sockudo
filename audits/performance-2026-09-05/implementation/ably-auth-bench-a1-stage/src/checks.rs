impl Hub {
    async fn revocation(
        &self,
        app_id: &str,
        target_type: &str,
        target_value: &str,
    ) -> Result<Option<AblyRevocationRecord>, AblyAuthError> {
        let key = revocation_cache_key(app_id, target_type, target_value);
        if let Some(cache) = &self.cache {
            let Some(encoded) = cache
                .get(&key)
                .await
                .map_err(|_| auth_backend_failure("revocation cache read"))?
            else {
                return Ok(None);
            };
            let record = serde_json::from_str::<AblyRevocationRecord>(&encoded)
                .map_err(|_| auth_backend_failure("revocation cache decode"))?;
            return Ok(Some(record));
        }
        Ok(lock_revocations(&self.revocations).get(&key, now_ms()))
    }

    async fn authorization_is_revoked(
        &self,
        app_id: &str,
        authorization: &ConnectionAuthorization,
        attached_channels: &HashMap<String, AblyConnectionAttachment>,
    ) -> bool {
        if !authorization.revocable {
            return false;
        }
        let now = now_ms();
        let mut targets = Vec::with_capacity(attached_channels.len().saturating_add(2));
        if let Some(client_id) = authorization.client_id.as_deref() {
            targets.push(("clientId", client_id));
        }
        if let Some(revocation_key) = authorization.revocation_key.as_deref() {
            targets.push(("revocationKey", revocation_key));
        }
        for channel in attached_channels.keys() {
            targets.push(("channel", channel.as_str()));
        }
        for (target_type, target_value) in targets {
            match self.revocation(app_id, target_type, target_value).await {
                Ok(Some(record))
                    if authorization.issued_ms < record.issued_before
                        && now >= record.applies_at =>
                {
                    return true;
                }
                Ok(_) => {}
                Err(error) => {
                    self.metrics.mark_backend_failure();
                    warn!(
                        protocol = "ably",
                        app_id = %app_id,
                        error = %error.message,
                        "revocation backend read failed; authorization rejected"
                    );
                    return true;
                }
            }
        }
        if let Some(cache) = &self.cache {
            return match self
                .cached_channel_revocation_applies(cache.as_ref(), app_id, authorization, now)
                .await
            {
                Ok(revoked) => revoked,
                Err(error) => {
                    self.metrics.mark_backend_failure();
                    warn!(
                        protocol = "ably",
                        app_id = %app_id,
                        error = %error.message,
                        "revocation backend scan failed; authorization rejected"
                    );
                    true
                }
            };
        }
        lock_revocations(&self.revocations)
            .records(app_id, now)
            .iter()
            .any(|record| channel_revocation_applies(record, authorization, now))
    }

    async fn cached_channel_revocation_applies(
        &self,
        cache: &dyn CacheManager,
        app_id: &str,
        authorization: &ConnectionAuthorization,
        now: i64,
    ) -> Result<bool, AblyAuthError> {
        const PAGE_SIZE: usize = 512;
        const MAX_PAGES: usize = ABLY_REVOCATION_MAX_ENTRIES.div_ceil(PAGE_SIZE) + 1;

        let prefix = format!("ably-compat:revocation:{app_id}:");
        let mut cursor = None;
        let mut entries_seen = 0usize;
        let mut bytes_seen = 0usize;
        for _ in 0..MAX_PAGES {
            let started=std::time::Instant::now();
            let page = cache
                .scan_prefix_page(&prefix, cursor.clone(), PAGE_SIZE)
                .await
                .map_err(|_| auth_backend_failure("revocation cache scan"))?;
            if std::env::var_os("DIAGNOSTIC").is_some() {eprintln!("snapshot page {} entries in {:?}",page.entries.len(),started.elapsed());}
            if page.entries.len() > PAGE_SIZE {
                return Err(auth_backend_failure("revocation cache page bound"));
            }
            entries_seen = entries_seen
                .checked_add(page.entries.len())
                .filter(|count| *count <= ABLY_REVOCATION_MAX_ENTRIES)
                .ok_or_else(AblyAuthError::revocation_capacity)?;
            for (key, encoded) in page.entries {
                bytes_seen = bytes_seen
                    .checked_add(key.len())
                    .and_then(|bytes| bytes.checked_add(encoded.len()))
                    .filter(|bytes| *bytes <= ABLY_REVOCATION_MAX_BYTES)
                    .ok_or_else(AblyAuthError::revocation_capacity)?;
                let record = serde_json::from_str::<AblyRevocationRecord>(&encoded)
                    .map_err(|_| auth_backend_failure("revocation cache decode"))?;
                if channel_revocation_applies(&record, authorization, now) {
                    return Ok(true);
                }
            }
            match page.next_cursor {
                Some(next) if !next.is_empty() && cursor.as_deref() != Some(next.as_str()) => {
                    cursor = Some(next);
                }
                Some(_) => return Err(auth_backend_failure("revocation cache cursor")),
                None => return Ok(false),
            }
        }
        Err(AblyAuthError::revocation_capacity())
    }

    fn invalidate_revocation_snapshot(&self, app_id: &str) {
        self.revocation_snapshots
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(app_id);
    }

    async fn authorization_is_revoked_from_snapshot(
        &self,
        app_id: &str,
        authorization: &ConnectionAuthorization,
        attached_channels: &HashMap<String, AblyConnectionAttachment>,
    ) -> bool {
        if !authorization.revocable {
            return false;
        }
        let slot = {
            let mut slots = self
                .revocation_snapshots
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(slot) = slots.get(app_id) {
                Arc::clone(slot)
            } else {
                // Cache only a bounded number of app snapshots. Eviction affects
                // performance only; every replacement must perform a fresh read.
                if slots.len() >= REVOCATION_SNAPSHOT_APP_LIMIT {
                    if let Some(key) = slots.keys().next().cloned() {
                        slots.remove(&key);
                    }
                }
                let slot = Arc::new(AsyncMutex::new(None));
                slots.insert(app_id.to_owned(), Arc::clone(&slot));
                slot
            }
        };
        let mut snapshot = slot.lock().await;
        let outcome = if let Some(current) = snapshot
            .as_ref()
            .filter(|snapshot| snapshot.started.elapsed() < REVOCATION_SNAPSHOT_FRESHNESS)
        {
            current.outcome.clone()
        } else {
            let started = TokioInstant::now();
            let outcome = match tokio::time::timeout(
                REVOCATION_SNAPSHOT_FRESHNESS,
                self.read_revocation_snapshot(app_id),
            )
            .await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    eprintln!("revocation probe refresh timed out: {error}");
                    warn!(app_id = %app_id, error = %error, "revocation snapshot refresh timed out");
                    Err(AblyAuthError::backend_unavailable())
                }
            };
            match outcome {
                Ok((records, bytes)) => {
                    let records = Arc::new(records);
                    if bytes <= REVOCATION_SNAPSHOT_CACHE_BYTES {
                        *snapshot = Some(RevocationSnapshot {
                            started,
                            outcome: Ok(Arc::clone(&records)),
                        });
                    } else {
                        *snapshot = None;
                    }
                    Ok(records)
                }
                Err(error) => {
                    *snapshot = Some(RevocationSnapshot {
                        started: TokioInstant::now(),
                        outcome: Err(error.clone()),
                    });
                    Err(error)
                }
            }
        };
        drop(snapshot);
        match outcome {
            Ok(records) => {
                let now = now_ms();
                records.iter().any(|record| {
                    if authorization.issued_ms >= record.issued_before || now < record.applies_at {
                        return false;
                    }
                    match record.target_type.as_str() {
                        "clientId" => {
                            authorization.client_id.as_deref() == Some(record.target_value.as_str())
                        }
                        "revocationKey" => {
                            authorization.revocation_key.as_deref()
                                == Some(record.target_value.as_str())
                        }
                        "channel" => {
                            attached_channels.contains_key(&record.target_value)
                                || channel_revocation_applies(record, authorization, now)
                        }
                        _ => false,
                    }
                })
            }
            Err(error) => {
                eprintln!("revocation probe unavailable: {}",error.message);
                self.metrics.mark_backend_failure();
                warn!(protocol = "ably", app_id = %app_id, error = %error.message, "revocation snapshot unavailable; authorization rejected");
                true
            }
        }
    }

    async fn read_revocation_snapshot(
        &self,
        app_id: &str,
    ) -> Result<(Vec<AblyRevocationRecord>, usize), AblyAuthError> {
        static SCANS: OnceLock<tokio::sync::Semaphore> = OnceLock::new();
        let _scan = SCANS
            .get_or_init(|| tokio::sync::Semaphore::new(4))
            .acquire()
            .await
            .map_err(|_| AblyAuthError::backend_unavailable())?;
        let Some(cache) = &self.cache else {
            let records = lock_revocations(&self.revocations).records(app_id, now_ms());
            let bytes = records
                .iter()
                .map(|record| {
                    record.target_type.len()
                        + record.target_value.len()
                        + std::mem::size_of::<AblyRevocationRecord>()
                })
                .sum();
            return Ok((records, bytes));
        };
        const PAGE_SIZE: usize = 512;
        const MAX_PAGES: usize = ABLY_REVOCATION_MAX_ENTRIES.div_ceil(PAGE_SIZE) + 1;
        let prefix = format!("ably-compat:revocation:{app_id}:");
        let mut cursor = None;
        let mut records = Vec::new();
        let mut bytes = 0usize;
        for _ in 0..MAX_PAGES {
            let started=std::time::Instant::now();
            let page = cache
                .scan_prefix_page(&prefix, cursor.clone(), PAGE_SIZE)
                .await
                .map_err(|_| auth_backend_failure("revocation cache scan"))?;
            if std::env::var_os("DIAGNOSTIC").is_some() {eprintln!("snapshot page {} entries in {:?}",page.entries.len(),started.elapsed());}
            if page.entries.len() > PAGE_SIZE {
                return Err(auth_backend_failure("revocation cache page bound"));
            }
            if records.len().saturating_add(page.entries.len()) > ABLY_REVOCATION_MAX_ENTRIES {
                return Err(AblyAuthError::revocation_capacity());
            }
            for (key, encoded) in page.entries {
                bytes = bytes
                    .checked_add(key.len())
                    .and_then(|bytes| bytes.checked_add(encoded.len()))
                    .filter(|bytes| *bytes <= ABLY_REVOCATION_MAX_BYTES)
                    .ok_or_else(AblyAuthError::revocation_capacity)?;
                records.push(
                    serde_json::from_str(&encoded)
                        .map_err(|_| auth_backend_failure("revocation cache decode"))?,
                );
            }
            match page.next_cursor {
                Some(next) if !next.is_empty() && cursor.as_deref() != Some(next.as_str()) => {
                    cursor = Some(next)
                }
                Some(_) => return Err(auth_backend_failure("revocation cache cursor")),
                None => return Ok((records, bytes)),
            }
        }
        Err(AblyAuthError::revocation_capacity())
    }

}
