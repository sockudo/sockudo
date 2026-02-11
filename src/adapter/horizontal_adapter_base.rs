use ahash::AHashMap as HashMap;
use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use crate::adapter::connection_manager::{ConnectionManager, HorizontalAdapterInterface};
use crate::adapter::horizontal_adapter::{
    BroadcastMessage, DeadNodeEvent, HorizontalAdapter, OrphanedMember, PendingRequest,
    RequestBody, RequestType, ResponseBody, current_timestamp, generate_request_id,
};
use crate::adapter::horizontal_transport::{
    HorizontalTransport, TransportConfig, TransportHandlers,
};
use crate::adapter::local_adapter::LocalAdapter;
use crate::app::manager::AppManager;
use crate::channel::PresenceMemberInfo;
use crate::error::{Error, Result};
use crate::metrics::MetricsInterface;
use crate::namespace::Namespace;
use crate::options::ClusterHealthConfig;
use crate::protocol::messages::PusherMessage;
use crate::websocket::{SocketId, WebSocketRef};
use async_trait::async_trait;
use sockudo_ws::axum_integration::WebSocketWriter;
use tokio::sync::{Mutex, Notify, RwLock};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Generic base adapter that handles all common horizontal scaling logic
pub struct HorizontalAdapterBase<T: HorizontalTransport> {
    pub horizontal: Arc<RwLock<HorizontalAdapter>>,
    pub local_adapter: Arc<LocalAdapter>,
    pub transport: T,
    pub config: T::Config,
    pub event_bus: Arc<OnceLock<tokio::sync::mpsc::UnboundedSender<DeadNodeEvent>>>,
    pub node_id: String,
    pub cluster_health_enabled: bool,
    pub heartbeat_interval_ms: u64,
    pub node_timeout_ms: u64,
    pub cleanup_interval_ms: u64,
    pub enable_socket_counting: bool,
    // Delta compression manager for bandwidth optimization
    delta_compression: Option<Arc<crate::delta_compression::DeltaCompressionManager>>,
    // App manager for getting channel-specific delta settings
    app_manager: Option<Arc<dyn AppManager + Send + Sync>>,
}

/// Check if we should skip horizontal communication (single node optimization)
/// This is determined by checking if cluster health is enabled and
/// if the effective node count is 1 or less.
async fn should_skip_horizontal_communication_impl(
    cluster_health_enabled: bool,
    horizontal: &Arc<RwLock<HorizontalAdapter>>,
) -> bool {
    // Don't skip sending if cluster health is disabled, we have no way of knowing other nodes
    if !cluster_health_enabled {
        return false;
    }
    let horizontal_guard = horizontal.read().await;
    let effective_node_count = horizontal_guard.get_effective_node_count().await;
    effective_node_count <= 1
}

impl<T: HorizontalTransport> HorizontalAdapterBase<T> {
    /// Check if we should skip horizontal communication (single node optimization)
    /// This is determined by checking if cluster health is enabled and
    /// if the effective node count is 1 or less.
    pub async fn should_skip_horizontal_communication(&self) -> bool {
        should_skip_horizontal_communication_impl(self.cluster_health_enabled, &self.horizontal)
            .await
    }
}

impl<T: HorizontalTransport + 'static> HorizontalAdapterBase<T>
where
    T::Config: TransportConfig,
{
    pub async fn new(config: T::Config) -> Result<Self> {
        let mut horizontal = HorizontalAdapter::new();
        horizontal.requests_timeout = config.request_timeout_ms();
        let node_id = horizontal.node_id.clone();

        // Get the Arc reference to the same LocalAdapter that HorizontalAdapter uses
        let local_adapter = horizontal.local_adapter.clone();

        let transport = T::new(config.clone()).await?;

        let cluster_health_defaults = ClusterHealthConfig::default();

        Ok(Self {
            horizontal: Arc::new(RwLock::new(horizontal)),
            local_adapter,
            transport,
            config,
            event_bus: Arc::new(OnceLock::new()),
            node_id,
            cluster_health_enabled: cluster_health_defaults.enabled,
            heartbeat_interval_ms: cluster_health_defaults.heartbeat_interval_ms,
            node_timeout_ms: cluster_health_defaults.node_timeout_ms,
            cleanup_interval_ms: cluster_health_defaults.cleanup_interval_ms,
            enable_socket_counting: true, // Default to enabled
            delta_compression: None,
            app_manager: None,
        })
    }

    /// Set delta compression manager and app manager for delta compression support
    pub async fn set_delta_compression(
        &mut self,
        delta_compression: Arc<crate::delta_compression::DeltaCompressionManager>,
        app_manager: Arc<dyn AppManager + Send + Sync>,
    ) {
        // Set on the horizontal adapter base
        self.delta_compression = Some(delta_compression.clone());
        self.app_manager = Some(app_manager.clone());

        // Also set on the internal LocalAdapter
        let horizontal = self.horizontal.read().await;
        horizontal
            .local_adapter
            .set_delta_compression(delta_compression, app_manager)
            .await;
    }

    pub async fn set_metrics(
        &self,
        metrics: Arc<Mutex<dyn MetricsInterface + Send + Sync>>,
    ) -> Result<()> {
        let mut horizontal = self.horizontal.write().await;
        horizontal.metrics = Some(metrics);
        Ok(())
    }

    pub fn set_event_bus(&self, event_sender: tokio::sync::mpsc::UnboundedSender<DeadNodeEvent>) {
        // OnceLock::set returns Err if already set, which we ignore (first-write-wins)
        let _ = self.event_bus.set(event_sender);
    }

    /// Configure this adapter with discovered nodes for testing
    /// This simulates that node discovery has already happened and sets up multi-node behavior
    pub async fn with_discovered_nodes(self, node_ids: Vec<&str>) -> Result<Self> {
        let horizontal = self.horizontal.read().await;
        for node_id in node_ids {
            let node_id_string = node_id.to_string();
            if node_id_string != self.node_id {
                horizontal
                    .add_discovered_node_for_test(node_id_string)
                    .await;
            }
        }
        drop(horizontal); // Release the lock
        Ok(self)
    }

    pub async fn set_cluster_health(&mut self, cluster_health: &ClusterHealthConfig) -> Result<()> {
        // Validate cluster health configuration first
        if let Err(validation_error) = cluster_health.validate() {
            warn!(
                "Cluster health configuration validation failed: {}",
                validation_error
            );
            warn!("Keeping current cluster health settings");
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
                "High-frequency heartbeat interval ({} ms) may cause unnecessary network load. Consider using >= 1000 ms unless high availability is critical.",
                cluster_health.heartbeat_interval_ms
            );
        }

        Ok(())
    }

    /// Set socket counting configuration
    pub fn set_socket_counting(&mut self, enable: bool) {
        self.enable_socket_counting = enable;
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
        let node_count = self.transport.get_node_count().await?;

        // Create the request
        let request_id = Uuid::new_v4().to_string();
        let node_id = {
            let horizontal = self.horizontal.read().await;
            horizontal.node_id.clone()
        };

        let request = RequestBody {
            request_id: request_id.clone(),
            node_id,
            app_id: app_id.to_string(),
            request_type: request_type.clone(),
            channel: channel.map(String::from),
            socket_id: socket_id.map(String::from),
            user_id: user_id.map(String::from),
            // Cluster presence fields (not used for regular requests)
            user_info: None,
            timestamp: None,
            dead_node_id: None,
            target_node_id: None,
        };

        // Add to pending requests
        {
            let horizontal = self.horizontal.read().await;
            horizontal.pending_requests.insert(
                request_id.clone(),
                PendingRequest {
                    start_time: Instant::now(),
                    app_id: app_id.to_string(),
                    responses: Vec::with_capacity(node_count.saturating_sub(1)),
                    notify: Arc::new(Notify::new()),
                },
            );

            if let Some(metrics_ref) = &horizontal.metrics {
                let metrics = metrics_ref.lock().await;
                metrics.mark_horizontal_adapter_request_sent(app_id);
            }
        }

        // Broadcast the request via transport (skip if single node)
        if !self.should_skip_horizontal_communication().await {
            self.transport.publish_request(&request).await?;
        }

        // Wait for responses with adaptive timeout based on request type and node count
        let base_timeout = self.config.request_timeout_ms();

        // Adaptive timeout: For simple queries (socket checks, counts), use shorter timeout
        // For presence queries that need member data, use full timeout
        let timeout_duration = match request_type {
            RequestType::SocketExistsInChannel
            | RequestType::ChannelSocketsCount
            | RequestType::SocketsCount
            | RequestType::Channels
            | RequestType::Sockets => {
                // Fast queries: 50ms per node, min 200ms (or base_timeout if lower), max base_timeout
                let min_timeout = std::cmp::min(200, base_timeout);
                let adaptive = (50 * node_count as u64).clamp(min_timeout, base_timeout);
                Duration::from_millis(adaptive)
            }
            RequestType::ChannelMembers | RequestType::ChannelMembersCount => {
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
            self.horizontal
                .read()
                .await
                .pending_requests
                .remove(&request_id);
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
            });
        }

        // Wait for responses using event-driven approach
        let start = Instant::now();
        let deadline = start + timeout_duration;
        let notify = {
            let horizontal = self.horizontal.read().await;
            horizontal
                .pending_requests
                .get(&request_id)
                .map(|req| req.notify.clone())
                .ok_or_else(|| {
                    Error::Other(format!(
                        "Request {request_id} not found in pending requests"
                    ))
                })?
        };

        let responses = loop {
            // Wait for notification or timeout
            let result = tokio::select! {
                _ = notify.notified() => {
                    // Check if we have enough responses OR can return early
                    let horizontal = self.horizontal.read().await;
                    if let Some(pending_request) = horizontal.pending_requests.get(&request_id) {
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
                                    "Request {} early return with {}/{} responses in {}ms (found positive result)",
                                    request_id,
                                    response_count,
                                    max_expected_responses,
                                    start.elapsed().as_millis()
                                );
                            } else {
                                debug!(
                                    "Request {} completed with {}/{} responses in {}ms",
                                    request_id,
                                    response_count,
                                    max_expected_responses,
                                    start.elapsed().as_millis()
                                );
                            }
                            // Extract responses without removing the entry yet to avoid race condition
                            let responses = pending_request.responses.clone();
                            Some(responses)
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
                    warn!(
                        "Request {} timed out after {}ms",
                        request_id,
                        start.elapsed().as_millis()
                    );
                    let horizontal = self.horizontal.read().await;
                    let responses = if let Some(pending_request) = horizontal.pending_requests.get(&request_id) {
                        pending_request.responses.clone()
                    } else {
                        Vec::new()
                    };
                    Some(responses)
                }
            };

            if let Some(responses) = result {
                break responses;
            }
            // If result is None, continue waiting (notification came but not enough responses yet)
        };

        // Aggregate responses first, then clean up to prevent race condition
        let combined_response = {
            let horizontal = self.horizontal.read().await;
            horizontal.aggregate_responses(
                request_id.clone(),
                request.node_id,
                app_id.to_string(),
                &request_type,
                responses,
            )
        }; // horizontal lock released here

        // Clean up the pending request after aggregation is complete
        {
            let horizontal = self.horizontal.read().await;
            horizontal.pending_requests.remove(&request_id);
        }

        // Track metrics
        {
            let horizontal = self.horizontal.read().await;
            if let Some(metrics_ref) = &horizontal.metrics {
                let metrics = metrics_ref.lock().await;
                let duration_ms = start.elapsed().as_micros() as f64 / 1000.0; // Convert to milliseconds with 3 decimal places
                metrics.track_horizontal_adapter_resolve_time(app_id, duration_ms);

                let resolved = combined_response.sockets_count > 0
                    || !combined_response.members.is_empty()
                    || combined_response.exists
                    || !combined_response.channels.is_empty()
                    || combined_response.members_count > 0
                    || !combined_response.channels_with_sockets_count.is_empty();

                metrics.track_horizontal_adapter_resolved_promises(app_id, resolved);
            }
        } // horizontal and metrics locks released here

        Ok(combined_response)
    }

    pub async fn start_listeners(&self) -> Result<()> {
        {
            let horizontal = self.horizontal.read().await;
            horizontal.start_request_cleanup();
        }

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

        let handlers = TransportHandlers {
            on_broadcast: Arc::new(move |broadcast| {
                let horizontal_clone = broadcast_horizontal.clone();
                Box::pin(async move {
                    let node_id = {
                        let horizontal = horizontal_clone.read().await;
                        horizontal.node_id.clone()
                    };

                    if broadcast.node_id == node_id {
                        return;
                    }

                    if let Ok(message) = sonic_rs::from_str::<PusherMessage>(&broadcast.message) {
                        // Debug log for tag filtering diagnostics
                        tracing::debug!(
                            "Received broadcast from node {}: channel={}, event={:?}, tags={:?}",
                            broadcast.node_id,
                            broadcast.channel,
                            message.event,
                            message.tags
                        );

                        let except_id = broadcast
                            .except_socket_id
                            .as_ref()
                            .and_then(|id| SocketId::from_string(id).ok());

                        let horizontal_lock = horizontal_clone.read().await;

                        // Log tag filtering status
                        let tag_filtering_enabled =
                            horizontal_lock.local_adapter.is_tag_filtering_enabled();
                        tracing::debug!(
                            "Tag filtering enabled on this node: {}",
                            tag_filtering_enabled
                        );

                        // Count local recipients for this node (adjusts for excluded socket)
                        let local_recipient_count = horizontal_lock
                            .get_local_recipient_count(
                                &broadcast.app_id,
                                &broadcast.channel,
                                except_id.as_ref(),
                            )
                            .await;

                        // Check if compression metadata is present and delta compression is enabled
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
                                    app.channel_delta_compression
                                        .as_ref()
                                        .and_then(|map| map.get(&broadcast.channel))
                                        .and_then(|config| {
                                            use crate::delta_compression::ChannelDeltaConfig;
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
                                        crate::adapter::connection_manager::CompressionParams {
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

                        // Track delta compression metrics if compression was used
                        if compression_used
                            && let Some(ref metrics) = horizontal_lock.metrics {
                                let metrics_lock = metrics.lock().await;
                                metrics_lock.track_horizontal_delta_compression(
                                    &broadcast.app_id,
                                    &broadcast.channel,
                                    true,
                                );
                            }

                        // Track broadcast latency metrics using helper function
                        let metrics_ref = horizontal_lock.metrics.clone();
                        drop(horizontal_lock); // Release horizontal lock to avoid deadlock

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
                    let node_id = {
                        let horizontal = horizontal_clone.read().await;
                        horizontal.node_id.clone()
                    };

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

                    let horizontal_lock = horizontal_clone.read().await;
                    let response = horizontal_lock.process_request(request.clone()).await?;

                    // Check if this was a heartbeat that detected a new node
                    if request.request_type == RequestType::Heartbeat && response.exists {
                        // New node detected, send our presence state to it
                        // Spawn task to avoid blocking request processing
                        let new_node_id = request.node_id.clone();
                        let horizontal_clone_for_task = horizontal_clone.clone();
                        let transport_for_task = transport_clone.clone();

                        tokio::spawn(async move {
                            // Small delay to let the new node finish initialization
                            tokio::time::sleep(Duration::from_millis(100)).await;

                            if let Err(e) = send_presence_state_to_node(
                                &horizontal_clone_for_task,
                                &transport_for_task,
                                &new_node_id,
                            )
                            .await
                            {
                                error!(
                                    "Failed to send presence state to new node {}: {}",
                                    new_node_id, e
                                );
                            }
                        });
                    }

                    Ok(response)
                })
            }),
            on_response: Arc::new(move |response| {
                let horizontal_clone = response_horizontal.clone();
                Box::pin(async move {
                    let node_id = {
                        let horizontal = horizontal_clone.read().await;
                        horizontal.node_id.clone()
                    };

                    if response.node_id == node_id {
                        return;
                    }

                    let horizontal_lock = horizontal_clone.read().await;
                    let _ = horizontal_lock.process_response(response).await;
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
            "Starting cluster health system with heartbeat interval: {}ms, timeout: {}ms",
            heartbeat_interval_ms, node_timeout_ms
        );

        // Start heartbeat broadcasting
        self.start_heartbeat_loop().await;

        // Start dead node detection
        self.start_dead_node_detection().await;
    }

    /// Start heartbeat broadcasting loop
    async fn start_heartbeat_loop(&self) {
        let transport = self.transport.clone();
        let node_id = self.node_id.clone();
        let heartbeat_interval_ms = self.heartbeat_interval_ms;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(heartbeat_interval_ms));

            loop {
                interval.tick().await;

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
                };

                if let Err(e) = transport.publish_request(&heartbeat_request).await {
                    error!("Failed to send heartbeat: {}", e);
                }
            }
        });
    }

    /// Start dead node detection loop
    async fn start_dead_node_detection(&self) {
        let transport = self.transport.clone();
        let horizontal = self.horizontal.clone();
        let event_bus = self.event_bus.clone();
        let node_id = self.node_id.clone();
        let cleanup_interval_ms = self.cleanup_interval_ms;
        let node_timeout_ms = self.node_timeout_ms;
        let cluster_health_enabled = self.cluster_health_enabled;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(cleanup_interval_ms));

            loop {
                interval.tick().await;

                let dead_nodes = {
                    let horizontal_guard = horizontal.read().await;
                    horizontal_guard.get_dead_nodes(node_timeout_ms).await
                };

                if !dead_nodes.is_empty() {
                    // Single leader election for entire cleanup round
                    let is_leader = {
                        let horizontal_guard = horizontal.read().await;
                        horizontal_guard.is_cleanup_leader(&dead_nodes).await
                    };

                    if is_leader {
                        info!(
                            "Elected as cleanup leader for {} dead nodes: {:?}",
                            dead_nodes.len(),
                            dead_nodes
                        );

                        // Process ALL dead nodes as the elected leader
                        for dead_node_id in dead_nodes {
                            debug!("Processing dead node: {}", dead_node_id);

                            // 1. Remove from local heartbeat tracking
                            {
                                let horizontal_guard = horizontal.read().await;
                                horizontal_guard.remove_dead_node(&dead_node_id).await;
                            }

                            // 2. Get orphaned members and clean up local registry
                            let cleanup_tasks = {
                                let horizontal_guard = horizontal.read().await;
                                match horizontal_guard
                                    .handle_dead_node_cleanup(&dead_node_id)
                                    .await
                                {
                                    Ok(tasks) => tasks,
                                    Err(e) => {
                                        error!(
                                            "Failed to cleanup dead node {}: {}",
                                            dead_node_id, e
                                        );
                                        continue;
                                    }
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
                                        error!("Failed to send dead node event: {}", e);
                                    }
                                } else {
                                    warn!("No event bus configured for dead node cleanup");
                                }

                                info!(
                                    "Emitted cleanup event for {} orphaned presence members from dead node {}",
                                    orphaned_members.len(),
                                    dead_node_id
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
                                };

                                if let Err(e) = transport.publish_request(&dead_node_request).await
                                {
                                    error!("Failed to send dead node notification: {}", e);
                                }
                            }
                        }
                    } else {
                        debug!(
                            "Not cleanup leader for {} dead nodes, waiting for leader's broadcasts",
                            dead_nodes.len()
                        );
                    }
                }
            }
        });
    }

    /// Get a snapshot of the cluster presence registry
    pub async fn get_cluster_presence_registry(
        &self,
    ) -> crate::adapter::horizontal_adapter::ClusterPresenceRegistry {
        let horizontal = self.horizontal.read().await;
        let registry = horizontal.cluster_presence_registry.read().await;
        registry.clone()
    }

    /// Get a snapshot of node heartbeat tracking
    pub async fn get_node_heartbeats(&self) -> HashMap<String, Instant> {
        let horizontal = self.horizontal.read().await;
        let heartbeats = horizontal.node_heartbeats.read().await;
        heartbeats.clone()
    }
}

#[async_trait]
impl<T: HorizontalTransport + 'static> ConnectionManager for HorizontalAdapterBase<T>
where
    T::Config: TransportConfig,
{
    async fn init(&self) {
        self.local_adapter.init().await;

        if let Err(e) = self.start_listeners().await {
            error!("Failed to start transport listeners: {}", e);
        }
    }

    async fn get_namespace(&self, app_id: &str) -> Option<Arc<Namespace>> {
        self.local_adapter.get_namespace(app_id).await
    }

    async fn add_socket(
        &self,
        socket_id: SocketId,
        socket: WebSocketWriter,
        app_id: &str,
        app_manager: Arc<dyn AppManager + Send + Sync>,
        buffer_config: crate::websocket::WebSocketBufferConfig,
    ) -> Result<()> {
        self.local_adapter
            .add_socket(socket_id, socket, app_id, app_manager, buffer_config)
            .await
    }

    async fn get_connection(&self, socket_id: &SocketId, app_id: &str) -> Option<WebSocketRef> {
        self.local_adapter.get_connection(socket_id, app_id).await
    }

    async fn remove_connection(&self, socket_id: &SocketId, app_id: &str) -> Result<()> {
        self.local_adapter
            .remove_connection(socket_id, app_id)
            .await
    }

    async fn send_message(
        &self,
        app_id: &str,
        socket_id: &SocketId,
        message: PusherMessage,
    ) -> Result<()> {
        self.local_adapter
            .send_message(app_id, socket_id, message)
            .await
    }

    async fn send(
        &self,
        channel: &str,
        message: PusherMessage,
        except: Option<&SocketId>,
        app_id: &str,
        start_time_ms: Option<f64>,
    ) -> Result<()> {
        // Check if delta compression is available and configured for this channel
        if let (Some(delta_compression), Some(app_manager)) =
            (&self.delta_compression, &self.app_manager)
        {
            // Get app config to check for channel-specific delta settings
            if let Ok(Some(app)) = app_manager.find_by_id(app_id).await {
                // Get channel-specific delta compression settings
                let channel_settings = app
                    .channel_delta_compression
                    .as_ref()
                    .and_then(|map| map.get(channel))
                    .and_then(|config| {
                        use crate::delta_compression::ChannelDeltaConfig;
                        match config {
                            ChannelDeltaConfig::Full(settings) => Some(settings.clone()),
                            _ => None,
                        }
                    });

                // Use compression-aware sending if we have settings with conflation key
                if channel_settings
                    .as_ref()
                    .and_then(|s| s.conflation_key.as_ref())
                    .is_some()
                {
                    return self
                        .send_with_compression(
                            channel,
                            message,
                            except,
                            app_id,
                            start_time_ms,
                            crate::adapter::connection_manager::CompressionParams {
                                delta_compression: Arc::clone(delta_compression),
                                channel_settings: channel_settings.as_ref(),
                            },
                        )
                        .await;
                }
            }
        }

        // Fall back to regular sending without delta compression
        // Send locally first (tracked in connection manager for metrics)
        let node_id = {
            let horizontal_lock = self.horizontal.read().await;
            horizontal_lock.node_id.clone()
        };

        let local_result = self
            .local_adapter
            .send(channel, message.clone(), except, app_id, start_time_ms)
            .await;

        if let Err(e) = local_result {
            warn!("Local send failed for channel {}: {}", channel, e);
        }

        // Broadcast to other nodes
        let message_json = sonic_rs::to_string(&message)?;
        let broadcast = BroadcastMessage {
            node_id,
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            message: message_json,
            except_socket_id: except.map(|id| id.to_string()),
            timestamp_ms: start_time_ms.or_else(|| {
                Some(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos() as f64
                        / 1_000_000.0, // Convert to milliseconds with microsecond precision
                )
            }),
            compression_metadata: None,
        };

        // Skip broadcasting to other nodes if we're in single-node mode
        if !self.should_skip_horizontal_communication().await {
            self.transport.publish_broadcast(&broadcast).await?;
        }

        Ok(())
    }

    async fn send_with_compression(
        &self,
        channel: &str,
        message: PusherMessage,
        except: Option<&SocketId>,
        app_id: &str,
        start_time_ms: Option<f64>,
        compression: crate::adapter::connection_manager::CompressionParams<'_>,
    ) -> Result<()> {
        // Send locally first with delta compression support
        let (node_id, local_result) = {
            let horizontal_lock = self.horizontal.read().await;

            let result = horizontal_lock
                .local_adapter
                .send_with_compression(
                    channel,
                    message.clone(),
                    except,
                    app_id,
                    start_time_ms,
                    crate::adapter::connection_manager::CompressionParams {
                        delta_compression: compression.delta_compression.clone(),
                        channel_settings: compression.channel_settings,
                    },
                )
                .await;
            (horizontal_lock.node_id.clone(), result)
        };

        if let Err(e) = local_result {
            warn!(
                "Local send with compression failed for channel {}: {}",
                channel, e
            );
        }

        // Broadcast to other nodes with compression metadata
        // Other nodes will apply their own delta compression using this metadata
        let message_json = sonic_rs::to_string(&message)?;

        // Extract conflation key from channel settings
        let conflation_key = compression
            .channel_settings
            .and_then(|s| s.conflation_key.clone());

        // Extract event name from message for tracking
        let event_name = message.event.as_deref().map(|s| s.to_string());

        // Check cluster coordination for synchronized full message intervals
        let (cluster_should_send_full, cluster_delta_count) = if compression
            .delta_compression
            .has_cluster_coordination()
        {
            if let Some(ck) = conflation_key.as_ref() {
                // Use cluster coordination to determine if we should send full message
                match compression
                    .delta_compression
                    .check_cluster_interval(app_id, channel, ck)
                    .await
                {
                    Ok((should_send_full, count)) => {
                        debug!(
                            "Cluster coordination: should_send_full={}, count={} for app={}, channel={}, key={}",
                            should_send_full, count, app_id, channel, ck
                        );
                        (Some(should_send_full), Some(count))
                    }
                    Err(e) => {
                        warn!(
                            "Cluster coordination failed: {}, falling back to node-local",
                            e
                        );
                        (None, None)
                    }
                }
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        // For horizontal broadcasts, we send full messages and let each node
        // decide whether to apply delta compression based on its local state.
        // If cluster coordination is enabled, we use the cluster-wide decision.
        let is_full_message = cluster_should_send_full.unwrap_or(true);

        let broadcast = BroadcastMessage {
            node_id,
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            message: message_json,
            except_socket_id: except.map(|id| id.to_string()),
            timestamp_ms: start_time_ms.or_else(|| {
                Some(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos() as f64
                        / 1_000_000.0,
                )
            }),
            compression_metadata: Some(crate::adapter::horizontal_adapter::CompressionMetadata {
                conflation_key,
                enabled: true,
                sequence: cluster_delta_count, // Cluster-wide sequence if coordination enabled
                is_full_message, // Determined by cluster coordination or defaults to true
                event_name,
            }),
        };

        // Skip broadcasting to other nodes if we're in single-node mode
        if !self.should_skip_horizontal_communication().await {
            self.transport.publish_broadcast(&broadcast).await?;
        }

        Ok(())
    }

    async fn get_channel_members(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HashMap<String, PresenceMemberInfo>> {
        // Get local members
        let mut members = self
            .local_adapter
            .get_channel_members(app_id, channel)
            .await?;

        // Get distributed members
        let response = self
            .send_request(
                app_id,
                RequestType::ChannelMembers,
                Some(channel),
                None,
                None,
            )
            .await?;

        members.extend(response.members);
        Ok(members)
    }

    async fn get_channel_sockets(&self, app_id: &str, channel: &str) -> Result<Vec<SocketId>> {
        // Get local sockets
        let mut all_socket_ids = self
            .local_adapter
            .get_channel_sockets(app_id, channel)
            .await?;

        // Get remote sockets
        let response = self
            .send_request(
                app_id,
                RequestType::ChannelSockets,
                Some(channel),
                None,
                None,
            )
            .await?;

        for socket_id in response.socket_ids {
            if let Ok(sid) = SocketId::from_string(&socket_id) {
                all_socket_ids.push(sid);
            }
        }

        Ok(all_socket_ids)
    }

    async fn remove_channel(&self, app_id: &str, channel: &str) {
        self.local_adapter.remove_channel(app_id, channel).await;
    }

    async fn is_in_channel(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<bool> {
        // Check locally first
        let local_result = self
            .local_adapter
            .is_in_channel(app_id, channel, socket_id)
            .await?;

        if local_result {
            return Ok(true);
        }

        // Check other nodes
        let response = self
            .send_request(
                app_id,
                RequestType::SocketExistsInChannel,
                Some(channel),
                Some(&socket_id.to_string()),
                None,
            )
            .await?;

        Ok(response.exists)
    }

    async fn get_user_sockets(&self, user_id: &str, app_id: &str) -> Result<Vec<WebSocketRef>> {
        self.local_adapter.get_user_sockets(user_id, app_id).await
    }

    async fn cleanup_connection(&self, app_id: &str, ws: WebSocketRef) {
        self.local_adapter.cleanup_connection(app_id, ws).await;
    }

    async fn terminate_connection(&self, app_id: &str, user_id: &str) -> Result<()> {
        // Terminate locally
        self.local_adapter
            .terminate_user_connections(app_id, user_id)
            .await?;

        // Broadcast termination to other nodes
        let _response = self
            .send_request(
                app_id,
                RequestType::TerminateUserConnections,
                None,
                None,
                Some(user_id),
            )
            .await?;

        Ok(())
    }

    async fn add_channel_to_sockets(&self, app_id: &str, channel: &str, socket_id: &SocketId) {
        self.local_adapter
            .add_channel_to_sockets(app_id, channel, socket_id)
            .await;
    }

    async fn get_channel_socket_count(&self, app_id: &str, channel: &str) -> usize {
        // Get local count
        let local_count = self
            .local_adapter
            .get_channel_socket_count(app_id, channel)
            .await;

        // Get distributed count
        match self
            .send_request(
                app_id,
                RequestType::ChannelSocketsCount,
                Some(channel),
                None,
                None,
            )
            .await
        {
            Ok(response) => local_count + response.sockets_count,
            Err(e) => {
                error!("Failed to get remote channel socket count: {}", e);
                local_count
            }
        }
    }

    async fn add_to_channel(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<bool> {
        // Fast path: direct local adapter access without locking horizontal
        self.local_adapter
            .add_to_channel(app_id, channel, socket_id)
            .await
    }

    async fn remove_from_channel(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<bool> {
        // Fast path: direct local adapter access without locking horizontal
        self.local_adapter
            .remove_from_channel(app_id, channel, socket_id)
            .await
    }

    async fn get_presence_member(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Option<PresenceMemberInfo> {
        self.local_adapter
            .get_presence_member(app_id, channel, socket_id)
            .await
    }

    async fn terminate_user_connections(&self, app_id: &str, user_id: &str) -> Result<()> {
        self.terminate_connection(app_id, user_id).await
    }

    async fn add_user(&self, ws_ref: WebSocketRef) -> Result<()> {
        self.local_adapter.add_user(ws_ref).await
    }

    async fn remove_user(&self, ws_ref: WebSocketRef) -> Result<()> {
        self.local_adapter.remove_user(ws_ref).await
    }

    async fn remove_user_socket(
        &self,
        user_id: &str,
        socket_id: &SocketId,
        app_id: &str,
    ) -> Result<()> {
        self.local_adapter
            .remove_user_socket(user_id, socket_id, app_id)
            .await
    }

    async fn count_user_connections_in_channel(
        &self,
        user_id: &str,
        app_id: &str,
        channel: &str,
        excluding_socket: Option<&SocketId>,
    ) -> Result<usize> {
        // Get local count (with excluding_socket filter)
        let local_count = self
            .local_adapter
            .count_user_connections_in_channel(user_id, app_id, channel, excluding_socket)
            .await?;

        // Get remote count (no excluding_socket since it's local-only)
        match self
            .send_request(
                app_id,
                RequestType::CountUserConnectionsInChannel,
                Some(channel),
                None,
                Some(user_id),
            )
            .await
        {
            Ok(response) => Ok(local_count + response.sockets_count),
            Err(e) => {
                error!("Failed to get remote user connections count: {}", e);
                Ok(local_count)
            }
        }
    }

    async fn get_channels_with_socket_count(&self, app_id: &str) -> Result<HashMap<String, usize>> {
        // Get local channels
        let mut channels = self
            .local_adapter
            .get_channels_with_socket_count(app_id)
            .await?;

        // Get distributed channels
        match self
            .send_request(
                app_id,
                RequestType::ChannelsWithSocketsCount,
                None,
                None,
                None,
            )
            .await
        {
            Ok(response) => {
                for (channel, count) in response.channels_with_sockets_count {
                    *channels.entry(channel).or_insert(0) += count;
                }
            }
            Err(e) => {
                error!("Failed to get remote channels with socket count: {}", e);
            }
        }

        Ok(channels)
    }

    async fn get_sockets_count(&self, app_id: &str) -> Result<usize> {
        // Check if socket counting is enabled
        if !self.enable_socket_counting {
            return Ok(0);
        }

        // Get local count
        let local_count = self.local_adapter.get_sockets_count(app_id).await?;

        // Get distributed count
        match self
            .send_request(app_id, RequestType::SocketsCount, None, None, None)
            .await
        {
            Ok(response) => Ok(local_count + response.sockets_count),
            Err(e) => {
                error!("Failed to get remote socket count: {}", e);
                Ok(local_count)
            }
        }
    }

    async fn get_namespaces(&self) -> Result<Vec<(String, Arc<Namespace>)>> {
        self.local_adapter.get_namespaces().await
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    async fn check_health(&self) -> Result<()> {
        self.transport.check_health().await
    }

    fn get_node_id(&self) -> String {
        self.node_id.clone()
    }

    fn as_horizontal_adapter(&self) -> Option<&dyn HorizontalAdapterInterface> {
        Some(self)
    }

    fn configure_dead_node_events(
        &self,
    ) -> Option<tokio::sync::mpsc::UnboundedReceiver<DeadNodeEvent>> {
        let (event_sender, event_receiver) = tokio::sync::mpsc::unbounded_channel();
        self.set_event_bus(event_sender);
        Some(event_receiver)
    }
}

#[async_trait]
impl<T: HorizontalTransport> HorizontalAdapterInterface for HorizontalAdapterBase<T> {
    /// Broadcast presence member joined to all nodes
    async fn broadcast_presence_join(
        &self,
        app_id: &str,
        channel: &str,
        user_id: &str,
        socket_id: &str,
        user_info: Option<sonic_rs::Value>,
    ) -> Result<()> {
        // Store in our own registry first with a single lock acquisition
        {
            let horizontal = self.horizontal.read().await;
            horizontal
                .add_presence_entry(
                    &self.node_id,
                    channel,
                    socket_id,
                    user_id,
                    app_id,
                    user_info.clone(),
                )
                .await;
        }

        // Skip cluster broadcast if cluster health is disabled
        if !self.cluster_health_enabled {
            return Ok(());
        }

        let request = RequestBody {
            request_id: crate::adapter::horizontal_adapter::generate_request_id(),
            node_id: self.node_id.clone(),
            app_id: app_id.to_string(),
            request_type: RequestType::PresenceMemberJoined,
            channel: Some(channel.to_string()),
            socket_id: Some(socket_id.to_string()),
            user_id: Some(user_id.to_string()),
            // Cluster presence fields
            user_info,
            timestamp: None,
            dead_node_id: None,
            target_node_id: None,
        };

        // Send without waiting for response (broadcast) - skip if single node
        if !self.should_skip_horizontal_communication().await {
            self.transport.publish_request(&request).await
        } else {
            Ok(())
        }
    }

    /// Broadcast presence member left to all nodes
    async fn broadcast_presence_leave(
        &self,
        app_id: &str,
        channel: &str,
        user_id: &str,
        socket_id: &str,
    ) -> Result<()> {
        // Remove from our own registry first with a single lock acquisition
        {
            let horizontal = self.horizontal.read().await;
            horizontal
                .remove_presence_entry(&self.node_id, channel, socket_id)
                .await;
        }

        // Skip cluster broadcast if cluster health is disabled
        if !self.cluster_health_enabled {
            return Ok(());
        }

        let request = RequestBody {
            request_id: crate::adapter::horizontal_adapter::generate_request_id(),
            node_id: self.node_id.clone(),
            app_id: app_id.to_string(),
            request_type: RequestType::PresenceMemberLeft,
            channel: Some(channel.to_string()),
            socket_id: Some(socket_id.to_string()),
            user_id: Some(user_id.to_string()),
            // Cluster presence fields
            user_info: None,
            timestamp: None,
            dead_node_id: None,
            target_node_id: None,
        };

        // Send without waiting for response (broadcast) - skip if single node
        if !self.should_skip_horizontal_communication().await {
            self.transport.publish_request(&request).await
        } else {
            Ok(())
        }
    }
}

/// Helper function to send presence state to a new node
async fn send_presence_state_to_node<T: HorizontalTransport>(
    horizontal: &Arc<tokio::sync::RwLock<HorizontalAdapter>>,
    transport: &T,
    target_node_id: &str,
) -> Result<()> {
    // Get our presence data
    let (our_node_id, data_to_send) = {
        let horizontal_lock = horizontal.read().await;
        let registry = horizontal_lock.cluster_presence_registry.read().await;

        // Get only our node's data
        if let Some(our_presence_data) = registry.get(&horizontal_lock.node_id) {
            // Clone the data to avoid holding the lock
            (
                horizontal_lock.node_id.clone(),
                Some(our_presence_data.clone()),
            )
        } else {
            (horizontal_lock.node_id.clone(), None)
        }
    };

    if let Some(data_to_send) = data_to_send {
        // Serialize the presence data
        let serialized_data = sonic_rs::to_value(&data_to_send)?;

        let sync_request = RequestBody {
            request_id: generate_request_id(),
            node_id: our_node_id,
            app_id: "cluster".to_string(),
            request_type: RequestType::PresenceStateSync,
            target_node_id: Some(target_node_id.to_string()),
            user_info: Some(serialized_data), // Reuse this field for bulk data
            channel: None,
            socket_id: None,
            user_id: None,
            timestamp: None,
            dead_node_id: None,
        };

        // This broadcasts but only target_node will process it
        transport.publish_request(&sync_request).await?;

        info!(
            "Sent presence state to new node: {} ({} channels)",
            target_node_id,
            data_to_send.len()
        );
    } else {
        debug!("No presence data to send to new node: {}", target_node_id);
    }

    Ok(())
}
