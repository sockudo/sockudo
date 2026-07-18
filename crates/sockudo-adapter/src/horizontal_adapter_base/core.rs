use super::*;

impl<T: HorizontalTransport + 'static> HorizontalAdapterBase<T>
where
    T::Config: TransportConfig,
{
    pub async fn new(config: T::Config) -> Result<Self> {
        let horizontal = HorizontalAdapter::new();
        horizontal.requests_timeout.store(
            config.request_timeout_ms(),
            std::sync::atomic::Ordering::Relaxed,
        );
        let node_id = horizontal.node_id.clone();

        // Get the Arc reference to the same LocalAdapter that HorizontalAdapter uses
        let local_adapter = horizontal.local_adapter.clone();

        let transport = T::new(config.clone()).await?;

        let cluster_health_defaults = ClusterHealthConfig::default();

        Ok(Self {
            horizontal: Arc::new(horizontal),
            local_adapter,
            transport,
            config,
            event_bus: Arc::new(OnceLock::new()),
            node_id,
            cluster_health_enabled: cluster_health_defaults.enabled,
            heartbeat_interval_ms: cluster_health_defaults.heartbeat_interval_ms,
            node_timeout_ms: cluster_health_defaults.node_timeout_ms,
            cleanup_interval_ms: cluster_health_defaults.cleanup_interval_ms,
            enable_socket_counting: true,     // Default to enabled
            aggregate_counts: false,          // Tier 1A: opt-in via config
            fast_presence_transitions: false, // Opt-in eventual presence transitions
            #[cfg(feature = "delta")]
            delta_compression: None,
            #[cfg(feature = "delta")]
            app_manager: None,
            cache_manager: Arc::new(OnceLock::new()),
            idempotency_ttl: AtomicU64::new(120),
            is_running: Arc::new(AtomicBool::new(true)),
        })
    }

    #[cfg(feature = "delta")]
    /// Set delta compression manager and app manager for delta compression support
    pub async fn set_delta_compression(
        &mut self,
        delta_compression: Arc<sockudo_delta::DeltaCompressionManager>,
        app_manager: Arc<dyn AppManager + Send + Sync>,
    ) {
        // Set on the horizontal adapter base
        self.delta_compression = Some(delta_compression.clone());
        self.app_manager = Some(app_manager.clone());

        // Also set on the internal LocalAdapter
        self.horizontal
            .local_adapter
            .set_delta_compression(delta_compression, app_manager)
            .await;
    }

    pub async fn set_metrics(
        &self,
        metrics: Arc<dyn MetricsInterface + Send + Sync>,
    ) -> Result<()> {
        self.horizontal.set_metrics(metrics);
        self.transport.set_metrics(
            self.horizontal
                .metrics
                .get()
                .cloned()
                .expect("metrics just set"),
        );
        Ok(())
    }

    /// Set the cache manager used for cross-region idempotency deduplication.
    pub fn set_cache_manager(
        &self,
        cache_manager: Arc<dyn sockudo_core::cache::CacheManager + Send + Sync>,
        idempotency_ttl: u64,
    ) {
        let _ = self.cache_manager.set(cache_manager);
        self.idempotency_ttl
            .store(idempotency_ttl, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn set_event_bus(&self, event_sender: DeadNodeEventBusSender) {
        // OnceLock::set returns Err if already set, which we ignore (first-write-wins)
        let _ = self.event_bus.set(event_sender);
    }

    /// Configure this adapter with discovered nodes for testing
    /// This simulates that node discovery has already happened and sets up multi-node behavior
    pub async fn with_discovered_nodes(self, node_ids: Vec<&str>) -> Result<Self> {
        for node_id in node_ids {
            let node_id_string = node_id.to_string();
            if node_id_string != self.node_id {
                self.horizontal
                    .add_discovered_node_for_test(node_id_string)
                    .await;
            }
        }
        Ok(self)
    }

    pub async fn set_cluster_health(&mut self, cluster_health: &ClusterHealthConfig) -> Result<()> {
        // Validate cluster health configuration first
        if let Err(validation_error) = cluster_health.validate() {
            warn!(error = %validation_error, "cluster health configuration validation failed");
            warn!("keeping current cluster health settings");
            return Ok(());
        }

        // Set cluster health configuration directly on the base adapter
        self.heartbeat_interval_ms = cluster_health.heartbeat_interval_ms;
        self.node_timeout_ms = cluster_health.node_timeout_ms;
        self.cleanup_interval_ms = cluster_health.cleanup_interval_ms;
        self.cluster_health_enabled = cluster_health.enabled;

        // Log warning for high-frequency heartbeats
        if cluster_health.heartbeat_interval_ms < 1000 {
            warn!(
                heartbeat_interval_ms = cluster_health.heartbeat_interval_ms,
                "high-frequency heartbeat interval may cause unnecessary network load, consider using >= 1000ms"
            );
        }

        Ok(())
    }

    /// Set socket counting configuration
    pub fn set_socket_counting(&mut self, enable: bool) {
        self.enable_socket_counting = enable;
    }

    /// Tier 1A: enable reading channel counts from the gossiped registry.
    pub fn set_aggregate_counts(&mut self, enable: bool) {
        self.aggregate_counts = enable;
    }

    /// Enable the eventual-consistency fast path for presence transition checks.
    pub fn set_fast_presence_transitions(&mut self, enable: bool) {
        self.fast_presence_transitions = enable;
    }

    /// Publish a pre-built RequestBody and collect responses from other nodes
    pub async fn send_request_with_body(&self, request: RequestBody) -> Result<ResponseBody> {
        let app_id = request.app_id.clone();
        let request_type = request.request_type.clone();
        let request_id = request.request_id.clone();

        let should_skip_horizontal = self.should_skip_horizontal_communication().await;
        let discovered_node_count = self.horizontal.get_effective_node_count().await;
        let node_count = if should_skip_horizontal {
            1
        } else if self.transport.node_count_is_real_time() {
            // Transport supports accurate node counting, use it directly
            self.transport.get_node_count().await?
        } else if self.cluster_health_enabled && discovered_node_count > 1 {
            // Cluster health heartbeats are tracking peers
            discovered_node_count
        } else {
            // Fallback to transport hint
            std::cmp::max(
                discovered_node_count,
                self.transport.get_node_count().await?,
            )
        };

        if should_skip_horizontal {
            return Ok(ResponseBody {
                request_id,
                node_id: request.node_id,
                app_id: app_id.to_string(),
                members: HashMap::new(),
                socket_ids: Vec::new(),
                sockets_count: 0,
                channels_with_sockets_count: HashMap::new(),
                exists: false,
                channels: HashSet::new(),
                members_count: 0,
                responses_received: 0,
                expected_responses: 0,
                complete: true,
            });
        }

        // Add to pending requests
        {
            self.horizontal.pending_requests.insert(
                request_id.clone(),
                PendingRequest {
                    start_time: Instant::now(),
                    app_id: app_id.to_string(),
                    responses: Vec::with_capacity(node_count.saturating_sub(1)),
                    notify: Arc::new(Notify::new()),
                },
            );

            if let Some(metrics) = self.horizontal.metrics.get() {
                metrics.mark_horizontal_adapter_request_sent(&app_id);
            }
        }

        // Use direct reply routing if supported, otherwise fall back to global subject
        match self.transport.new_inbox() {
            Some(inbox) => {
                self.transport
                    .publish_request_with_reply(&request, &inbox)
                    .await?;
            }
            None => {
                self.transport.publish_request(&request).await?;
            }
        };

        // Wait for responses with adaptive timeout based on request type and node count
        let base_timeout = self
            .horizontal
            .requests_timeout
            .load(std::sync::atomic::Ordering::Relaxed);

        // Adaptive timeout: For simple queries (socket checks, counts), use shorter timeout
        // For presence queries that need member data, use full timeout
        let timeout_duration = match request_type {
            RequestType::SocketExistsInChannel
            | RequestType::SocketsCount
            | RequestType::Channels
            | RequestType::Sockets => {
                // Fast queries: 50ms per node, min 200ms (or base_timeout if lower), max base_timeout
                let min_timeout = std::cmp::min(200, base_timeout);
                let adaptive = (50 * node_count as u64).clamp(min_timeout, base_timeout);
                Duration::from_millis(adaptive)
            }
            RequestType::ChannelMembers
            | RequestType::ChannelMembersCount
            | RequestType::ChannelSocketsCount => {
                // Presence queries need more time for data aggregation
                Duration::from_millis(base_timeout)
            }
            _ => {
                // All other queries use base timeout
                Duration::from_millis(base_timeout)
            }
        };

        let max_expected_responses = node_count.saturating_sub(1);

        if max_expected_responses == 0 {
            self.horizontal.pending_requests.remove(&request_id);
            return Ok(ResponseBody {
                request_id,
                node_id: request.node_id,
                app_id: app_id.to_string(),
                members: HashMap::new(),
                socket_ids: Vec::new(),
                sockets_count: 0,
                channels_with_sockets_count: HashMap::new(),
                exists: false,
                channels: HashSet::new(),
                members_count: 0,
                responses_received: 0,
                expected_responses: 0,
                complete: true,
            });
        }

        // Wait for responses using event-driven approach
        let start = Instant::now();
        let deadline = start + timeout_duration;
        let notify = self
            .horizontal
            .pending_requests
            .get(&request_id)
            .map(|req| req.notify.clone())
            .ok_or_else(|| {
                Error::Other(format!(
                    "Request {request_id} not found in pending requests"
                ))
            })?;

        let (responses, timed_out) = loop {
            // Wait for notification or timeout
            let result = tokio::select! {
                _ = notify.notified() => {
                    // Check if we have enough responses OR can return early
                    if let Some(pending_request) = self.horizontal.pending_requests.get(&request_id) {
                        let response_count = pending_request.responses.len();

                        // Early return optimization: Return immediately when we find a positive result
                        // This dramatically improves latency for existence checks in large clusters
                        // Note: ChannelSockets must wait for all responses to get complete socket list
                        let can_early_return = match request_type {
                            RequestType::SocketExistsInChannel if response_count > 0 => {
                                // Check if any response is positive
                                pending_request.responses.iter().any(|r| r.exists)
                            }
                            _ => false,
                        };

                        if response_count >= max_expected_responses || can_early_return {
                            if can_early_return {
                                debug!(
                                    request_id = %request_id,
                                    response_count = response_count,
                                    expected = max_expected_responses,
                                    elapsed_ms = start.elapsed().as_millis(),
                                    "request early return with positive result"
                                );
                            } else {
                                debug!(
                                    request_id = %request_id,
                                    response_count = response_count,
                                    expected = max_expected_responses,
                                    elapsed_ms = start.elapsed().as_millis(),
                                    "request completed"
                                );
                            }
                            // Extract responses without removing the entry yet to avoid race condition
                            let responses = pending_request.responses.clone();
                            Some((responses, false))
                        } else {
                            None // Continue waiting
                        }
                    } else {
                        return Err(Error::Other(format!(
                            "Request {request_id} was removed unexpectedly"
                        )));
                    }
                }
                _ = tokio::time::sleep_until(deadline.into()) => {
                    // Timeout occurred
                    let responses = if let Some(pending_request) = self.horizontal.pending_requests.get(&request_id) {
                        pending_request.responses.clone()
                    } else {
                        Vec::new()
                    };
                    warn!(
                        request_id = %request_id,
                        request_type = ?request_type,
                        elapsed_ms = start.elapsed().as_millis(),
                        responses_received = responses.len(),
                        expected = max_expected_responses,
                        "request timed out"
                    );
                    Some((responses, true))
                }
            };

            if let Some(outcome) = result {
                break outcome;
            }
            // If result is None, continue waiting (notification came but not enough responses yet)
        };

        let responses_received = responses.len();
        let complete = responses_received >= max_expected_responses;

        // Aggregate responses first, then clean up to prevent race condition
        let combined_response = self.horizontal.aggregate_responses(
            request_id.clone(),
            request.node_id,
            app_id.to_string(),
            &request_type,
            responses,
            AggregationStats {
                responses_received,
                expected_responses: max_expected_responses,
                complete,
            },
        );

        // Clean up the pending request after aggregation is complete
        self.horizontal.pending_requests.remove(&request_id);

        // Track metrics
        if let Some(metrics) = self.horizontal.metrics.get() {
            let duration_ms = start.elapsed().as_micros() as f64 / 1000.0; // Convert to milliseconds with 3 decimal places
            metrics.track_horizontal_adapter_resolve_time(&app_id, duration_ms);

            // Empty results are still resolved — only a timeout is uncomplete
            metrics.track_horizontal_adapter_resolved_promises(&app_id, !timed_out);
        }

        Ok(combined_response)
    }

    /// Enhanced send_request that properly integrates with HorizontalAdapter
    pub async fn send_request(
        &self,
        app_id: &str,
        request_type: RequestType,
        channel: Option<&str>,
        socket_id: Option<&str>,
        user_id: Option<&str>,
    ) -> Result<ResponseBody> {
        let request = RequestBody {
            request_id: Uuid::new_v4().to_string(),
            node_id: self.horizontal.node_id.clone(),
            app_id: app_id.to_string(),
            request_type,
            channel: channel.map(String::from),
            socket_id: socket_id.map(String::from),
            user_id: user_id.map(String::from),
            user_info: None,
            timestamp: None,
            dead_node_id: None,
            target_node_id: None,
            reply_to: None,
            channels: None,
        };
        self.send_request_with_body(request).await
    }

    pub async fn start_listeners(&self) -> Result<()> {
        self.horizontal.start_request_cleanup();

        // Start cluster health system only if enabled
        if self.cluster_health_enabled {
            self.start_cluster_health_system().await;
        }

        // Set up transport handlers
        let horizontal_arc = self.horizontal.clone();

        let broadcast_horizontal = horizontal_arc.clone();
        let request_horizontal = horizontal_arc.clone();
        let response_horizontal = horizontal_arc.clone();
        let transport_for_request = self.transport.clone();
        let broadcast_cache_manager = self.cache_manager.clone();
        let broadcast_idempotency_ttl = self
            .idempotency_ttl
            .load(std::sync::atomic::Ordering::Relaxed);

        let handlers = TransportHandlers {
            node_id: self.node_id.clone(),
            on_broadcast: Arc::new(move |broadcast| {
                let horizontal_clone = broadcast_horizontal.clone();
                let cache_manager_clone = broadcast_cache_manager.clone();
                let idempotency_ttl = broadcast_idempotency_ttl;
                Box::pin(async move {
                    let node_id = horizontal_clone.node_id.clone();

                    if broadcast.node_id == node_id {
                        return;
                    }

                    // Register the idempotency key in local cache so duplicate
                    // publishes arriving at this node are caught.
                    if let Some(ref key) = broadcast.idempotency_key
                        && let Some(cache) = cache_manager_clone.get()
                    {
                        let cache_key = format!("app:{}:idempotency:{}", broadcast.app_id, key);
                        let _ = cache
                            .set_if_not_exists(&cache_key, "cross_region", idempotency_ttl)
                            .await;
                    }

                    if let Ok(message) = sonic_rs::from_str::<PusherMessage>(&broadcast.message) {
                        // Very hot path under fanout load; keep detailed per-message
                        // diagnostics behind TRACE.
                        tracing::trace!(
                            node_id = %broadcast.node_id,
                            channel = %broadcast.channel,
                            event = ?message.event,
                            stream_id = ?message.stream_id,
                            serial = ?message.serial,
                            tags = ?message.tags,
                            "received broadcast"
                        );

                        let except_id = broadcast
                            .except_socket_id
                            .as_ref()
                            .and_then(|id| SocketId::from_string(id).ok());

                        let horizontal_lock = &horizontal_clone;

                        // Log tag filtering status
                        #[cfg(feature = "tag-filtering")]
                        {
                            let tag_filtering_enabled =
                                horizontal_lock.local_adapter.is_tag_filtering_enabled();
                            tracing::trace!(
                                tag_filtering_enabled = tag_filtering_enabled,
                                "tag filtering status on this node"
                            );
                        }

                        // Count local recipients for this node (adjusts for excluded socket)
                        let local_recipient_count = horizontal_lock
                            .get_local_recipient_count(
                                &broadcast.app_id,
                                &broadcast.channel,
                                except_id.as_ref(),
                            )
                            .await;

                        // Check if compression metadata is present and delta compression is enabled
                        #[cfg(feature = "delta")]
                        let (send_result, compression_used) = if let Some(ref compression_meta) =
                            broadcast.compression_metadata
                            && compression_meta.enabled
                        {
                            // Try to get delta compression manager and app manager (lock-free via OnceLock)
                            let delta_compression_opt =
                                horizontal_lock.local_adapter.get_delta_compression();
                            let app_manager_opt = horizontal_lock.local_adapter.get_app_manager();

                            // Only proceed with compression if both are available
                            if let (Some(delta_compression), Some(app_manager)) =
                                (delta_compression_opt.cloned(), app_manager_opt.cloned())
                            {
                                // Try to get channel-specific settings
                                let channel_settings = if let Ok(Some(app)) =
                                    app_manager.find_by_id(&broadcast.app_id).await
                                {
                                    app.channel_delta_compression_ref()
                                        .and_then(|map| map.get(&broadcast.channel))
                                        .and_then(|config| {
                                            use sockudo_delta::ChannelDeltaConfig;
                                            match config {
                                                ChannelDeltaConfig::Full(settings) => {
                                                    // Use the conflation key from compression metadata
                                                    let mut settings = settings.clone();
                                                    if compression_meta.conflation_key.is_some() {
                                                        settings.conflation_key =
                                                            compression_meta.conflation_key.clone();
                                                    }
                                                    Some(settings)
                                                }
                                                _ => None,
                                            }
                                        })
                                } else {
                                    None
                                };

                                // Use compression-aware sending
                                let result = horizontal_lock
                                    .local_adapter
                                    .send_with_compression(
                                        &broadcast.channel,
                                        message,
                                        except_id.as_ref(),
                                        &broadcast.app_id,
                                        broadcast.timestamp_ms,
                                        crate::connection_manager::CompressionParams {
                                            delta_compression,
                                            channel_settings: channel_settings.as_ref(),
                                        },
                                    )
                                    .await;
                                (result, true)
                            } else {
                                // Delta compression not available, use regular send
                                let result = horizontal_lock
                                    .local_adapter
                                    .send(
                                        &broadcast.channel,
                                        message,
                                        except_id.as_ref(),
                                        &broadcast.app_id,
                                        broadcast.timestamp_ms,
                                    )
                                    .await;
                                (result, false)
                            }
                        } else {
                            // No compression metadata or compression not enabled, use regular send
                            let result = horizontal_lock
                                .local_adapter
                                .send(
                                    &broadcast.channel,
                                    message,
                                    except_id.as_ref(),
                                    &broadcast.app_id,
                                    broadcast.timestamp_ms,
                                )
                                .await;
                            (result, false)
                        };

                        #[cfg(not(feature = "delta"))]
                        let send_result = horizontal_lock
                            .local_adapter
                            .send(
                                &broadcast.channel,
                                message,
                                except_id.as_ref(),
                                &broadcast.app_id,
                                broadcast.timestamp_ms,
                            )
                            .await;

                        // Track delta compression metrics if compression was used
                        #[cfg(feature = "delta")]
                        if compression_used && let Some(metrics) = horizontal_lock.metrics.get() {
                            metrics.track_horizontal_delta_compression(
                                &broadcast.app_id,
                                &broadcast.channel,
                                true,
                            );
                        }

                        // Track broadcast latency metrics using helper function
                        let metrics_ref = horizontal_lock.metrics.get().cloned();

                        HorizontalAdapter::track_broadcast_latency_if_successful(
                            &send_result,
                            broadcast.timestamp_ms,
                            Some(local_recipient_count), // Use local count, not sender's count
                            &broadcast.app_id,
                            &broadcast.channel,
                            metrics_ref,
                        )
                        .await;
                    }
                })
            }),
            on_request: Arc::new(move |request| {
                let horizontal_clone = request_horizontal.clone();
                let transport_clone = transport_for_request.clone();
                Box::pin(async move {
                    let node_id = horizontal_clone.node_id.clone();

                    if request.node_id == node_id {
                        return Err(Error::OwnRequestIgnored);
                    }

                    // Check if this is a targeted request for another node
                    if let Some(ref target_node) = request.target_node_id
                        && target_node != &node_id
                    {
                        // Not for us, ignore silently
                        return Err(Error::RequestNotForThisNode);
                    }

                    let response = horizontal_clone.process_request(request.clone()).await?;

                    // Check if this was a heartbeat that detected a new node
                    if request.request_type == RequestType::Heartbeat && response.exists {
                        // New node detected, send our presence state to it
                        // Spawn task to avoid blocking request processing
                        let new_node_id = request.node_id.clone();
                        let horizontal_clone_for_task = horizontal_clone.clone();
                        let transport_for_task = transport_clone.clone();
                        let node_id = horizontal_clone.node_id.clone();

                        tokio::spawn(async move {
                            let mut hasher = ahash::AHasher::default();
                            node_id.hash(&mut hasher);
                            let stagger_ms = hasher.finish() % PRESENCE_SYNC_STAGGER_MAX_MS;
                            tokio::time::sleep(Duration::from_millis(100 + stagger_ms)).await;

                            if let Err(e) = transport_for_task
                                .sync_presence_state_to_node(
                                    &horizontal_clone_for_task,
                                    &new_node_id,
                                )
                                .await
                            {
                                error!(node_id = %new_node_id, error = %e, "failed to sync presence state to new node");
                            }
                        });
                    }

                    // No peer is waiting for a response, skip publishing.
                    if request.request_type.is_fire_and_forget() {
                        return Err(Error::NoResponseNeeded);
                    }

                    Ok(response)
                })
            }),
            on_response: Arc::new(move |response| {
                let horizontal_clone = response_horizontal.clone();
                Box::pin(async move {
                    let node_id = horizontal_clone.node_id.clone();

                    if response.node_id == node_id {
                        return;
                    }

                    let _ = horizontal_clone.process_response(response).await;
                })
            }),
        };

        self.transport.start_listeners(handlers).await?;
        Ok(())
    }

    /// Start cluster health monitoring system
    pub async fn start_cluster_health_system(&self) {
        let heartbeat_interval_ms = self.heartbeat_interval_ms;
        let node_timeout_ms = self.node_timeout_ms;

        info!(
            heartbeat_interval_ms = heartbeat_interval_ms,
            node_timeout_ms = node_timeout_ms,
            "starting cluster health system"
        );

        // Start heartbeat broadcasting
        self.start_heartbeat_loop().await;

        // Start dead node detection
        self.start_dead_node_detection().await;

        // Tier 1A: gossip local channel counts so peers answer counts locally.
        if self.aggregate_counts {
            self.start_count_gossip_loop().await;
        }
    }

    /// Tier 1A: periodically gossip this node's local channel counts so every node
    /// answers count queries locally. Per-tick diffs (ChannelCountUpdate) plus a
    /// periodic full snapshot (ChannelCountSync) seed new nodes and heal drift.
    async fn start_count_gossip_loop(&self) {
        let transport = self.transport.clone();
        let local_adapter = self.local_adapter.clone();
        let horizontal = self.horizontal.clone();
        let node_id = self.node_id.clone();
        let is_running = self.is_running.clone();

        tokio::spawn(async move {
            const FLUSH_INTERVAL_MS: u64 = 100;
            const FULL_SNAPSHOT_EVERY_TICKS: u64 = 50; // ~5s
            let mut interval = tokio::time::interval(Duration::from_millis(FLUSH_INTERVAL_MS));
            let mut last: std::collections::HashMap<String, ahash::AHashMap<String, usize>> =
                std::collections::HashMap::new();
            let mut tick: u64 = 0;

            loop {
                interval.tick().await;
                if !is_running.load(Ordering::Relaxed) {
                    break;
                }
                tick = tick.wrapping_add(1);
                let full = tick.is_multiple_of(FULL_SNAPSHOT_EVERY_TICKS);

                // Only gossip when there are peers to receive it.
                if horizontal.get_effective_node_count().await <= 1 {
                    continue;
                }

                let namespaces = match local_adapter.get_namespaces().await {
                    Ok(namespaces) => namespaces,
                    Err(e) => {
                        debug!(error = %e, "namespace fetch failed, skipping");
                        continue;
                    }
                };

                let mut seen_apps = std::collections::HashSet::new();
                for (app_id, namespace) in namespaces {
                    seen_apps.insert(app_id.clone());
                    let current = namespace
                        .get_channels_with_socket_count()
                        .await
                        .unwrap_or_default();
                    let prev = last.entry(app_id.clone()).or_default();

                    let payload: ahash::AHashMap<String, usize> = if full {
                        current.clone()
                    } else {
                        let mut changed = ahash::AHashMap::new();
                        for (channel, count) in &current {
                            if prev.get(channel) != Some(count) {
                                changed.insert(channel.clone(), *count);
                            }
                        }
                        for channel in prev.keys() {
                            if !current.contains_key(channel) {
                                changed.insert(channel.clone(), 0);
                            }
                        }
                        changed
                    };

                    *prev = current;

                    // Diff ticks skip when nothing changed; full ticks always send
                    // (an empty full snapshot clears this node's contribution).
                    if payload.is_empty() && !full {
                        continue;
                    }

                    if let Ok(user_info) = sonic_rs::to_value(&payload) {
                        let request = RequestBody {
                            request_id: generate_request_id(),
                            node_id: node_id.clone(),
                            app_id,
                            request_type: if full {
                                RequestType::ChannelCountSync
                            } else {
                                RequestType::ChannelCountUpdate
                            },
                            channel: None,
                            socket_id: None,
                            user_id: None,
                            user_info: Some(user_info),
                            timestamp: Some(current_timestamp()),
                            dead_node_id: None,
                            target_node_id: None,
                            reply_to: None,
                            channels: None,
                        };
                        let _ = transport.publish_request(&request).await;
                    }
                }
                last.retain(|app, _| seen_apps.contains(app));
            }
        });
    }

    /// Start heartbeat broadcasting loop
    pub(super) async fn start_heartbeat_loop(&self) {
        let transport = self.transport.clone();
        let node_id = self.node_id.clone();
        let heartbeat_interval_ms = self.heartbeat_interval_ms;
        let is_running = self.is_running.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(heartbeat_interval_ms));

            loop {
                interval.tick().await;
                if !is_running.load(Ordering::Relaxed) {
                    break;
                }

                let heartbeat_request = RequestBody {
                    request_id: generate_request_id(),
                    node_id: node_id.clone(),
                    app_id: "cluster".to_string(),
                    request_type: RequestType::Heartbeat,
                    channel: None,
                    socket_id: None,
                    user_id: None,
                    user_info: None,
                    timestamp: Some(current_timestamp()),
                    dead_node_id: None,
                    target_node_id: None,
                    reply_to: None,
                    channels: None,
                };

                if let Err(e) = transport.publish_request(&heartbeat_request).await {
                    error!(error = %e, "failed to send heartbeat");
                }
            }
        });
    }

    /// Start dead node detection loop
    pub(super) async fn start_dead_node_detection(&self) {
        let transport = self.transport.clone();
        let horizontal = self.horizontal.clone();
        let event_bus = self.event_bus.clone();
        let node_id = self.node_id.clone();
        let cleanup_interval_ms = self.cleanup_interval_ms;
        let node_timeout_ms = self.node_timeout_ms;
        let cluster_health_enabled = self.cluster_health_enabled;
        let is_running = self.is_running.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(cleanup_interval_ms));

            loop {
                interval.tick().await;
                if !is_running.load(Ordering::Relaxed) {
                    break;
                }

                let dead_nodes = horizontal.get_dead_nodes(node_timeout_ms).await;

                if !dead_nodes.is_empty() {
                    // Single leader election for entire cleanup round
                    let is_leader = horizontal.is_cleanup_leader(&dead_nodes).await;

                    if is_leader {
                        info!(
                            dead_node_count = dead_nodes.len(),
                            dead_nodes = ?dead_nodes,
                            "elected as cleanup leader"
                        );

                        // Process ALL dead nodes as the elected leader
                        for dead_node_id in dead_nodes {
                            debug!(node_id = %dead_node_id, "processing dead node");

                            // 1. Remove from local heartbeat tracking
                            horizontal.remove_dead_node(&dead_node_id).await;

                            // 2. Get orphaned members and clean up local registry
                            let cleanup_tasks = match horizontal
                                .handle_dead_node_cleanup(&dead_node_id)
                                .await
                            {
                                Ok(tasks) => tasks,
                                Err(e) => {
                                    error!(node_id = %dead_node_id, error = %e, "failed to cleanup dead node");
                                    continue;
                                }
                            };

                            if !cleanup_tasks.is_empty() {
                                // 3. Convert to orphaned members and emit event
                                let orphaned_members: Vec<OrphanedMember> = cleanup_tasks
                                    .into_iter()
                                    .map(|(app_id, channel, user_id, user_info)| OrphanedMember {
                                        app_id,
                                        channel,
                                        user_id,
                                        user_info, // Preserve user_info for proper presence management
                                    })
                                    .collect();

                                let event = DeadNodeEvent {
                                    dead_node_id: dead_node_id.clone(),
                                    orphaned_members: orphaned_members.clone(),
                                };

                                // Emit event for processing by ConnectionHandler (lock-free read via OnceLock)
                                if let Some(event_sender) = event_bus.get() {
                                    if let Err(e) = event_sender.send(event) {
                                        error!(error = %e, "failed to send dead node event");
                                    }
                                } else {
                                    warn!("No event bus configured for dead node cleanup");
                                }

                                info!(
                                    orphaned_member_count = orphaned_members.len(),
                                    node_id = %dead_node_id,
                                    "emitted cleanup event for orphaned presence members"
                                );
                            }

                            // 4. Notify followers to clean their registries (skip if single node)
                            let should_skip = should_skip_horizontal_communication_impl(
                                cluster_health_enabled,
                                &horizontal,
                            )
                            .await;

                            if !should_skip {
                                let dead_node_request = RequestBody {
                                    request_id: generate_request_id(),
                                    node_id: node_id.clone(),
                                    app_id: "cluster".to_string(),
                                    request_type: RequestType::NodeDead,
                                    channel: None,
                                    socket_id: None,
                                    user_id: None,
                                    user_info: None,
                                    timestamp: Some(current_timestamp()),
                                    dead_node_id: Some(dead_node_id.clone()),
                                    target_node_id: None,
                                    reply_to: None,
                                    channels: None,
                                };

                                if let Err(e) = transport.publish_request(&dead_node_request).await
                                {
                                    error!(error = %e, "failed to send dead node notification");
                                }
                            }
                        }
                    } else {
                        debug!(
                            dead_node_count = dead_nodes.len(),
                            "not cleanup leader, waiting for leader broadcasts"
                        );
                    }
                }
            }
        });
    }

    /// Get a snapshot of the cluster presence registry
    pub async fn get_cluster_presence_registry(
        &self,
    ) -> crate::horizontal_adapter::ClusterPresenceRegistry {
        let registry = self.horizontal.cluster_presence_registry.read().await;
        registry.clone()
    }

    /// Get a snapshot of node heartbeat tracking
    pub async fn get_node_heartbeats(&self) -> HashMap<String, Instant> {
        let heartbeats = self.horizontal.node_heartbeats.read().await;
        heartbeats.clone()
    }
}
