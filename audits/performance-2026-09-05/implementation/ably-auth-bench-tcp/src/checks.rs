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
            let page = cache
                .scan_prefix_page(&prefix, cursor.clone(), PAGE_SIZE)
                .await
                .map_err(|_| auth_backend_failure("revocation cache scan"))?;
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

}
