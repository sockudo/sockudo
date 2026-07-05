#[cfg(all(feature = "push", feature = "monolith"))]
use super::push::workers::start_push_monolith_workers;
#[cfg(feature = "push")]
use super::push::{PushAdmissionSnapshot, create_push_queue, create_push_store};
use super::{MetricsFactory, ServerState, SockudoServer};
use crate::cleanup::CleanupSender;
use crate::cleanup::multi_worker::MultiWorkerCleanupSystem;
use crate::history::create_history_store;
#[cfg(feature = "versioned-messages")]
use crate::history::create_version_store;
#[cfg(feature = "ably-compat")]
use crate::http_handler::global_ably_hub;
use crate::presence_history::create_presence_history_store;
use sockudo_adapter::ConnectionHandler;
use sockudo_adapter::factory::AdapterFactory;
use sockudo_app::AppManagerFactory;
use sockudo_cache::CacheManagerFactory;
use sockudo_cache::MemoryCacheManager;
use sockudo_core::auth::AuthValidator;
use sockudo_core::error::{Error, Result};
use sockudo_core::options::{AdapterDriver, DeltaCoordinationBackend, QueueDriver, ServerOptions};
use sockudo_queue::QueueManagerFactory;
use sockudo_rate_limiter::factory::RateLimiterFactory;
use sockudo_webhook::integration::QueueManager;
use sockudo_webhook::{BatchingConfig, WebhookConfig, WebhookIntegration};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
#[cfg(feature = "delta")]
use std::time::Duration;
use tracing::{error, info, warn};

fn resolve_delta_coordination_backend(config: &ServerOptions) -> DeltaCoordinationBackend {
    match config.delta_compression.coordination_backend {
        DeltaCoordinationBackend::Auto => match config.adapter.driver {
            AdapterDriver::Redis => DeltaCoordinationBackend::Redis,
            AdapterDriver::RedisCluster => DeltaCoordinationBackend::RedisCluster,
            AdapterDriver::Nats => DeltaCoordinationBackend::Nats,
            _ => DeltaCoordinationBackend::None,
        },
        ref backend => backend.clone(),
    }
}

impl SockudoServer {
    pub(crate) async fn new(config: ServerOptions) -> Result<Self> {
        let debug_enabled = config.debug;
        info!(
            "Initializing Sockudo server with new configuration... Debug mode: {}",
            debug_enabled
        );

        let cache_manager = CacheManagerFactory::create(&config.cache, &config.database.redis)
            .await
            .unwrap_or_else(|e| {
                warn!(
                    "CacheManagerFactory creation failed: {}. Using a NoOp (Memory) Cache.",
                    e
                );
                let fallback_cache_options = config.cache.memory.clone();
                Arc::new(MemoryCacheManager::new(
                    "fallback_cache".to_string(),
                    fallback_cache_options,
                ))
            });
        info!(
            "CacheManager initialized with driver: {:?}",
            config.cache.driver
        );

        let app_manager = AppManagerFactory::create(
            &config.app_manager,
            &config.database,
            &config.database_pooling,
            cache_manager.clone(),
        )
        .await?;
        info!(
            "AppManager initialized with driver: {:?} (cache: {})",
            config.app_manager.driver, config.app_manager.cache.enabled
        );

        let (connection_manager, typed_adapter) =
            AdapterFactory::create_with_typed(&config.adapter, &config.database).await?;
        let local_adapter = Some(typed_adapter.local_adapter());

        info!(
            "Adapter initialized with driver: {:?}",
            config.adapter.driver
        );

        // Set up dead node cleanup event bus for horizontal adapters (only if cluster health is enabled)
        let dead_node_event_receiver = if config.adapter.cluster_health.enabled {
            let receiver_opt = connection_manager.configure_dead_node_events();

            if receiver_opt.is_some() {
                info!(
                    "Event bus configured for clustering adapter: {:?}",
                    config.adapter.driver
                );
            } else {
                info!(
                    "Adapter {:?} doesn't require dead node cleanup events",
                    config.adapter.driver
                );
            }

            receiver_opt
        } else {
            info!("Cluster health disabled, skipping dead node cleanup event bus setup");
            None
        };

        let auth_validator = Arc::new(AuthValidator::new(app_manager.clone()));

        let metrics =
            if config.metrics.enabled {
                info!(
                    "Initializing metrics with driver: {:?}",
                    config.metrics.driver
                );
                match MetricsFactory::create(
                    config.metrics.driver.as_ref(),
                    config.metrics.port,
                    Some(&config.metrics.prometheus.prefix),
                    config.metrics.tcp_exporter.enabled.then(|| {
                        sockudo_metrics::TcpExporterOptions {
                            host: config.metrics.tcp_exporter.host.clone(),
                            port: config.metrics.tcp_exporter.port,
                            buffer_size: config.metrics.tcp_exporter.buffer_size,
                        }
                    }),
                )
                .await
                {
                    Some(metrics_driver) => {
                        info!("Metrics driver initialized successfully");
                        Some(metrics_driver)
                    }
                    None => {
                        warn!("Failed to initialize metrics driver, metrics will be disabled");
                        None
                    }
                }
            } else {
                info!("Metrics are disabled in configuration");
                None
            };

        let http_api_rate_limiter_instance = if config.rate_limiter.enabled {
            RateLimiterFactory::create_api(&config.rate_limiter, &config.database.redis)
                .await
                .unwrap_or_else(|e| {
                    error!(
                        "Failed to initialize HTTP API rate limiter: {}. Using a permissive limiter.",
                        e
                    );
                    Arc::new(sockudo_rate_limiter::memory_limiter::MemoryRateLimiter::new(
                        u32::MAX, 1,
                    ))
                })
        } else {
            info!("HTTP API Rate limiting is globally disabled. Using a permissive limiter.");
            Arc::new(sockudo_rate_limiter::memory_limiter::MemoryRateLimiter::new(u32::MAX, 1))
        };
        info!(
            "HTTP API RateLimiter initialized (enabled: {}) with driver: {:?}",
            config.rate_limiter.enabled, config.rate_limiter.driver
        );

        let websocket_rate_limiter_instance = if config.rate_limiter.enabled {
            RateLimiterFactory::create_websocket(&config.rate_limiter, &config.database.redis)
                .await
                .unwrap_or_else(|e| {
                    error!(
                        "Failed to initialize WebSocket rate limiter: {}. Using a permissive limiter.",
                        e
                    );
                    Arc::new(sockudo_rate_limiter::memory_limiter::MemoryRateLimiter::new(
                        u32::MAX, 1,
                    ))
                })
        } else {
            info!("WebSocket rate limiting is globally disabled. Using a permissive limiter.");
            Arc::new(sockudo_rate_limiter::memory_limiter::MemoryRateLimiter::new(u32::MAX, 1))
        };
        info!(
            "WebSocket RateLimiter initialized (enabled: {}) with driver: {:?}",
            config.rate_limiter.enabled, config.rate_limiter.driver
        );

        #[cfg(feature = "push")]
        let push_acceptance_rps = std::env::var("PUSH_ACCEPTANCE_RATE_LIMIT")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(config.push.default_quotas.acceptance_rps);

        #[cfg(feature = "push")]
        let push_acceptance_rate_limiter_instance = if config.rate_limiter.enabled
            && push_acceptance_rps > 0
        {
            let limit = push_acceptance_rps.min(u64::from(u32::MAX)) as u32;
            RateLimiterFactory::create_push_acceptance(
                &config.rate_limiter,
                limit,
                1,
                &config.database.redis,
            )
            .await
            .unwrap_or_else(|e| {
                error!(
                    "Failed to initialize Push acceptance rate limiter: {}. Using a permissive limiter.",
                    e
                );
                Arc::new(sockudo_rate_limiter::memory_limiter::MemoryRateLimiter::new(
                    u32::MAX,
                    1,
                ))
            })
        } else {
            info!("Push acceptance rate limiting is disabled. Using a permissive limiter.");
            Arc::new(sockudo_rate_limiter::memory_limiter::MemoryRateLimiter::new(u32::MAX, 1))
        };
        #[cfg(feature = "push")]
        info!(
            "Push acceptance RateLimiter initialized (enabled: {}) with driver: {:?}, limit: {}/s",
            config.rate_limiter.enabled && push_acceptance_rps > 0,
            config.rate_limiter.driver,
            push_acceptance_rps
        );

        let owned_default_queue_redis_url: String;
        let queue_redis_url_arg: Option<&str>;

        if let Some(url_override) = config.queue.redis.url_override.as_ref() {
            queue_redis_url_arg = Some(url_override.as_str());
        } else {
            owned_default_queue_redis_url = config.database.redis.to_url();
            queue_redis_url_arg = Some(&owned_default_queue_redis_url);
        }

        let queue_manager_opt = match config.queue.driver {
            QueueDriver::Sqs => {
                match QueueManagerFactory::create_sqs(config.queue.sqs.clone()).await {
                    Ok(queue_driver_impl) => {
                        info!("Queue manager initialized with SQS driver");
                        Some(Arc::new(QueueManager::new(queue_driver_impl)))
                    }
                    Err(e) => {
                        warn!(
                            "Failed to initialize SQS queue manager: {}, queues will be disabled",
                            e
                        );
                        None
                    }
                }
            }
            QueueDriver::Sns => {
                match QueueManagerFactory::create_sns(config.queue.sns.clone()).await {
                    Ok(queue_driver_impl) => {
                        info!("Queue manager initialized with SNS driver");
                        Some(Arc::new(QueueManager::new(queue_driver_impl)))
                    }
                    Err(e) => {
                        warn!(
                            "Failed to initialize SNS queue manager: {}, queues will be disabled",
                            e
                        );
                        None
                    }
                }
            }
            QueueDriver::Nats => {
                match QueueManagerFactory::create_nats(config.queue.nats.clone()).await {
                    Ok(queue_driver_impl) => {
                        info!("Queue manager initialized with NATS JetStream driver");
                        Some(Arc::new(QueueManager::new(queue_driver_impl)))
                    }
                    Err(e) => {
                        warn!(
                            "Failed to initialize NATS JetStream queue manager: {}, queues will be disabled",
                            e
                        );
                        None
                    }
                }
            }
            QueueDriver::RabbitMq => {
                match QueueManagerFactory::create_rabbitmq(config.queue.rabbitmq.clone()).await {
                    Ok(queue_driver_impl) => {
                        info!("Queue manager initialized with RabbitMQ driver");
                        Some(Arc::new(QueueManager::new(queue_driver_impl)))
                    }
                    Err(e) => {
                        warn!(
                            "Failed to initialize RabbitMQ queue manager: {}, queues will be disabled",
                            e
                        );
                        None
                    }
                }
            }
            QueueDriver::Kafka => {
                match QueueManagerFactory::create_kafka(config.queue.kafka.clone()).await {
                    Ok(queue_driver_impl) => {
                        info!("Queue manager initialized with Kafka driver");
                        Some(Arc::new(QueueManager::new(queue_driver_impl)))
                    }
                    Err(e) => {
                        warn!(
                            "Failed to initialize Kafka queue manager: {}, queues will be disabled",
                            e
                        );
                        None
                    }
                }
            }
            QueueDriver::Iggy => {
                match QueueManagerFactory::create_iggy(config.queue.iggy.clone()).await {
                    Ok(queue_driver_impl) => {
                        info!("Queue manager initialized with Apache Iggy driver");
                        Some(Arc::new(QueueManager::new(queue_driver_impl)))
                    }
                    Err(e) => {
                        warn!(
                            "Failed to initialize Apache Iggy queue manager: {}, queues will be disabled",
                            e
                        );
                        None
                    }
                }
            }
            QueueDriver::Pulsar => {
                match QueueManagerFactory::create_pulsar(config.queue.pulsar.clone()).await {
                    Ok(queue_driver_impl) => {
                        info!("Queue manager initialized with Pulsar driver");
                        Some(Arc::new(QueueManager::new(queue_driver_impl)))
                    }
                    Err(e) => {
                        warn!(
                            "Failed to initialize Pulsar queue manager: {}, queues will be disabled",
                            e
                        );
                        None
                    }
                }
            }
            QueueDriver::GooglePubSub => {
                match QueueManagerFactory::create_google_pubsub(config.queue.google_pubsub.clone())
                    .await
                {
                    Ok(queue_driver_impl) => {
                        info!("Queue manager initialized with Google Pub/Sub driver");
                        Some(Arc::new(QueueManager::new(queue_driver_impl)))
                    }
                    Err(e) => {
                        warn!(
                            "Failed to initialize Google Pub/Sub queue manager: {}, queues will be disabled",
                            e
                        );
                        None
                    }
                }
            }
            QueueDriver::None => {
                info!("Queue driver set to None, queue manager will be disabled.");
                None
            }
            QueueDriver::Redis | QueueDriver::RedisCluster | QueueDriver::Memory => {
                let (
                    queue_redis_url_or_nodes,
                    queue_prefix,
                    queue_concurrency,
                    queue_response_timeout_ms,
                ) = match config.queue.driver {
                    QueueDriver::Redis => {
                        let owned_default_queue_redis_url: String;
                        let queue_redis_url_arg: Option<&str>;

                        if let Some(url_override) = config.queue.redis.url_override.as_ref() {
                            queue_redis_url_arg = Some(url_override.as_str());
                        } else {
                            owned_default_queue_redis_url = config.database.redis.to_url();
                            queue_redis_url_arg = Some(&owned_default_queue_redis_url);
                        }

                        (
                            queue_redis_url_arg.map(|s| s.to_string()),
                            config
                                .queue
                                .redis
                                .prefix
                                .as_deref()
                                .unwrap_or("sockudo_queue:"),
                            config.queue.redis.concurrency as usize,
                            Some(config.queue.redis.response_timeout_ms),
                        )
                    }
                    QueueDriver::RedisCluster => {
                        let cluster_nodes = if config.queue.redis_cluster.nodes.is_empty() {
                            vec![
                                "redis://127.0.0.1:7000".to_string(),
                                "redis://127.0.0.1:7001".to_string(),
                                "redis://127.0.0.1:7002".to_string(),
                            ]
                        } else {
                            config.queue.redis_cluster.nodes.clone()
                        };

                        let nodes_str = cluster_nodes.join(",");

                        (
                            Some(nodes_str),
                            config
                                .queue
                                .redis_cluster
                                .prefix
                                .as_deref()
                                .unwrap_or("sockudo_queue:"),
                            config.queue.redis_cluster.concurrency as usize,
                            Some(config.queue.redis_cluster.request_timeout_ms),
                        )
                    }
                    _ => (None, "sockudo_queue:", 5, None),
                };

                match QueueManagerFactory::create(
                    config.queue.driver.as_ref(),
                    queue_redis_url_or_nodes.as_deref(),
                    Some(queue_prefix),
                    Some(queue_concurrency),
                    queue_response_timeout_ms,
                )
                .await
                {
                    Ok(queue_driver_impl) => {
                        info!(
                            "Queue manager initialized with driver: {:?}",
                            config.queue.driver
                        );
                        Some(Arc::new(QueueManager::new(queue_driver_impl)))
                    }
                    Err(e) => {
                        warn!(
                            "Failed to initialize queue manager with driver '{:?}': {}, queues will be disabled",
                            config.queue.driver, e
                        );
                        None
                    }
                }
            }
        };

        let webhook_config_for_integration = WebhookConfig {
            enabled: queue_manager_opt.is_some(),
            batching: BatchingConfig {
                enabled: config.webhooks.batching.enabled,
                duration: config.webhooks.batching.duration,
                size: config.webhooks.batching.size,
            },
            retry: config.webhooks.retry.clone(),
            request_timeout_ms: config.webhooks.request_timeout_ms,
            process_id: config.instance.process_id.clone(),
            debug: config.debug,
        };

        let webhook_integration = match WebhookIntegration::new(
            webhook_config_for_integration,
            app_manager.clone(),
            queue_manager_opt.clone(),
        )
        .await
        {
            Ok(integration) => {
                if integration.is_enabled() {
                    info!("Webhook integration initialized successfully with queue manager");
                } else if queue_manager_opt.is_none() {
                    info!("Webhooks disabled (no queue manager available)");
                } else {
                    info!("Webhook integration initialized (disabled)");
                }
                Arc::new(integration)
            }
            Err(e) => {
                warn!(
                    "Failed to initialize webhook integration: {}, creating disabled instance",
                    e
                );
                let disabled_config = WebhookConfig {
                    enabled: false,
                    ..Default::default()
                };
                Arc::new(WebhookIntegration::new(disabled_config, app_manager.clone(), None).await?)
            }
        };

        let history_store = create_history_store(
            &config.history,
            &config.database,
            &config.database_pooling,
            metrics.clone(),
            Some(cache_manager.clone()),
        )
        .await?;

        let presence_history_store = create_presence_history_store(
            &config.presence_history,
            config.history.enabled,
            Some(history_store.clone()),
            metrics.clone(),
        )
        .await;

        // Initialize cleanup queue if enabled
        let cleanup_config = config.cleanup.clone();

        // Validate cleanup configuration
        if let Err(e) = cleanup_config.validate() {
            error!("Invalid cleanup configuration: {}", e);
            return Err(Error::Internal(format!(
                "Invalid cleanup configuration: {}",
                e
            )));
        }

        let (cleanup_queue, cleanup_worker_handles) = if cleanup_config.async_enabled {
            let multi_worker_system = MultiWorkerCleanupSystem::new(
                connection_manager.clone(),
                app_manager.clone(),
                Some(webhook_integration.clone()),
                presence_history_store.clone(),
                config.presence_history.clone(),
                cleanup_config.clone(),
                metrics.clone(),
            );

            let cleanup_sender =
                if let Some(direct_sender) = multi_worker_system.get_direct_sender() {
                    info!("Using direct sender for single worker (optimized)");
                    CleanupSender::Direct(direct_sender)
                } else {
                    info!("Using multi-worker sender with round-robin distribution");
                    CleanupSender::Multi(multi_worker_system.get_sender())
                };

            let worker_handles = multi_worker_system.get_worker_handles();

            info!("Multi-worker cleanup system initialized");
            (Some(cleanup_sender), Some(worker_handles))
        } else {
            (None, None)
        };

        // Initialize delta compression manager
        #[cfg(feature = "delta")]
        let delta_compression_manager = {
            let algorithm = match config.delta_compression.algorithm.as_str() {
                "xdelta3" => sockudo_delta::DeltaAlgorithm::Xdelta3,
                _ => sockudo_delta::DeltaAlgorithm::Fossil,
            };

            let delta_config = sockudo_delta::DeltaCompressionConfig {
                enabled: config.delta_compression.enabled,
                algorithm,
                full_message_interval: config.delta_compression.full_message_interval,
                min_message_size: config.delta_compression.min_message_size,
                max_state_age: Duration::from_secs(config.delta_compression.max_state_age_secs),
                max_channel_states_per_socket: config
                    .delta_compression
                    .max_channel_states_per_socket,
                min_compression_ratio: 0.9,
                max_conflation_states_per_channel: config
                    .delta_compression
                    .max_conflation_states_per_channel,
                conflation_key_path: config.delta_compression.conflation_key_path.clone(),
                cluster_coordination: config.delta_compression.cluster_coordination,
                omit_delta_algorithm: config.delta_compression.omit_delta_algorithm,
            };
            let delta_compression_manager =
                sockudo_delta::DeltaCompressionManager::new(delta_config);

            // Setup cluster coordination if enabled and adapter supports it
            let delta_compression_manager = {
                #[allow(unused_mut)]
                let mut manager = delta_compression_manager;

                if config.delta_compression.cluster_coordination {
                    let coordination_backend = resolve_delta_coordination_backend(&config);

                    match coordination_backend {
                        #[cfg(feature = "redis")]
                        DeltaCoordinationBackend::Redis => {
                            let redis_url = config.database.redis.to_url();
                            match sockudo_delta::coordination::RedisClusterCoordinator::new(
                                &redis_url,
                                Some(&config.database.redis.key_prefix),
                            )
                            .await
                            {
                                Ok(coordinator) => {
                                    info!(
                                        "Delta compression cluster coordination enabled via Redis"
                                    );
                                    manager.set_cluster_coordinator(Arc::new(coordinator));
                                }
                                Err(e) => {
                                    warn!(
                                        "Failed to setup Redis delta coordination, falling back to node-local: {}",
                                        e
                                    );
                                }
                            }
                        }
                        #[cfg(feature = "redis")]
                        DeltaCoordinationBackend::RedisCluster => {
                            let nodes = config.database.redis.cluster_node_urls();
                            match sockudo_delta::coordination::RedisClusterCoordinator::new_cluster(
                                nodes,
                                Some(&config.database.redis.key_prefix),
                            )
                            .await
                            {
                                Ok(coordinator) => {
                                    info!(
                                        "Delta compression cluster coordination enabled via Redis Cluster"
                                    );
                                    manager.set_cluster_coordinator(Arc::new(coordinator));
                                }
                                Err(e) => {
                                    warn!(
                                        "Failed to setup Redis Cluster delta coordination, falling back to node-local: {}",
                                        e
                                    );
                                }
                            }
                        }
                        #[cfg(feature = "nats")]
                        DeltaCoordinationBackend::Nats => {
                            let nats_servers = config.adapter.nats.servers.clone();

                            match sockudo_delta::coordination::NatsClusterCoordinator::new(
                                nats_servers,
                                Some(&config.adapter.nats.prefix),
                            )
                            .await
                            {
                                Ok(coordinator) => {
                                    info!(
                                        "Delta compression cluster coordination enabled via NATS"
                                    );
                                    manager.set_cluster_coordinator(Arc::new(coordinator));
                                }
                                Err(e) => {
                                    warn!(
                                        "Failed to setup NATS delta coordination, falling back to node-local: {}",
                                        e
                                    );
                                }
                            }
                        }
                        DeltaCoordinationBackend::None => {
                            info!(
                                "Delta compression cluster coordination disabled or unsupported for this adapter"
                            );
                        }
                        #[allow(unreachable_patterns)]
                        other => {
                            warn!(
                                "Delta coordination backend {:?} requested but not compiled in; falling back to node-local",
                                other
                            );
                        }
                    }
                }

                Arc::new(manager)
            };

            // Start background cleanup task for delta compression state management
            delta_compression_manager.start_cleanup_task().await;
            delta_compression_manager
        };

        // Set cache manager for cross-region idempotency deduplication on horizontal adapters
        if config.idempotency.enabled {
            typed_adapter.set_cache_manager(cache_manager.clone(), config.idempotency.ttl_seconds);
            info!(
                "Cross-region idempotency deduplication configured for adapter: {:?} (TTL: {}s)",
                config.adapter.driver, config.idempotency.ttl_seconds
            );
        }

        #[cfg(feature = "push")]
        let push_store = create_push_store(&config).await?;
        #[cfg(feature = "push")]
        let push_admission =
            Arc::new(PushAdmissionSnapshot::from_config(&config, &push_store).await);
        #[cfg(feature = "push")]
        let push_queue = create_push_queue(&config, queue_manager_opt.clone(), &push_admission)?;
        #[cfg(all(feature = "push", feature = "monolith"))]
        let push_worker_handles =
            start_push_monolith_workers(&config, push_store.clone(), push_queue.clone());

        let state = ServerState {
            app_manager: app_manager.clone(),
            connection_manager: connection_manager.clone(),
            local_adapter: local_adapter.clone(),
            auth_validator,
            cache_manager,
            queue_manager: queue_manager_opt,
            webhooks_integration: webhook_integration.clone(),
            metrics: metrics.clone(),
            running: Arc::new(AtomicBool::new(true)),
            http_api_rate_limiter: Some(http_api_rate_limiter_instance.clone()),
            websocket_rate_limiter: Some(websocket_rate_limiter_instance.clone()),
            #[cfg(feature = "push")]
            push_acceptance_rate_limiter: push_acceptance_rate_limiter_instance.clone(),
            debug_enabled,
            cleanup_queue,
            cleanup_worker_handles,
            cleanup_config,
            #[cfg(feature = "delta")]
            delta_compression: delta_compression_manager.clone(),
            typed_adapter: typed_adapter.clone(),
            #[cfg(feature = "push")]
            push_store,
            #[cfg(feature = "push")]
            push_queue,
            #[cfg(feature = "push")]
            push_admission,
            #[cfg(all(feature = "push", feature = "monolith"))]
            push_worker_handles,
        };

        let mut builder = ConnectionHandler::builder(
            state.app_manager.clone(),
            state.connection_manager.clone(),
            state.cache_manager.clone(),
            config.clone(),
        )
        .webhook_integration(webhook_integration);

        // Spawn the periodic history purge worker for backends without native TTL
        // (PostgreSQL, MySQL, SurrealDB). DynamoDB and ScyllaDB use native TTL and
        // inherit the trait's default `purge_before` which returns `(0, false)`.
        if config.history.enabled && config.history.retention_window_seconds > 0 {
            let purge_store = history_store.clone();
            let retention_ms =
                (config.history.retention_window_seconds as i64).saturating_mul(1000);
            let interval =
                std::time::Duration::from_secs(config.history.purge_interval_seconds.max(10));
            let batch_size = config.history.purge_batch_size.max(1);
            let max_per_tick = config.history.max_purge_per_tick;

            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    ticker.tick().await;
                    let cutoff = sockudo_core::history::now_ms().saturating_sub(retention_ms);
                    let mut total: u64 = 0;
                    loop {
                        match purge_store.purge_before(cutoff, batch_size).await {
                            Ok((deleted, has_more)) => {
                                total = total.saturating_add(deleted);
                                if !has_more || total >= max_per_tick as u64 {
                                    break;
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    error = %e,
                                    "history store purge failed"
                                );
                                break;
                            }
                        }
                    }
                    if total > 0 {
                        tracing::debug!(deleted = total, "history store purge tick complete");
                    }
                }
            });
            info!(
                "History store purge worker started (retention: {}s, interval: {}s)",
                config.history.retention_window_seconds,
                interval.as_secs()
            );
        }

        builder = builder.history_store(history_store);
        #[cfg(feature = "versioned-messages")]
        if config.versioned_messages.enabled {
            let version_store = create_version_store(
                &config.versioned_messages,
                &config.history,
                &config.database,
                &config.database_pooling,
            )
            .await?;
            let version_store: Arc<dyn sockudo_core::version_store::VersionStore + Send + Sync> =
                if config.ai_transport.enabled {
                    const AI_TRANSPORT_VERSION_SERIAL_LEASE_SIZE: u64 = 128;
                    Arc::new(sockudo_core::version_store::LeasedVersionStore::new(
                        version_store,
                        AI_TRANSPORT_VERSION_SERIAL_LEASE_SIZE,
                    ))
                } else {
                    version_store
                };

            // Spawn the periodic purge worker for backends without native TTL
            // (MySQL, PostgreSQL, SurrealDB, Memory). ScyllaDB and DynamoDB
            // inherit the trait's default `purge_before`, which returns
            // `(0, false)` — the worker ticks against them are effectively
            // free, so we keep the code path uniform.
            if config.versioned_messages.retention_window_seconds > 0 {
                let purge_store = version_store.clone();
                let retention_ms = (config.versioned_messages.retention_window_seconds as i64)
                    .saturating_mul(1000);
                let interval = std::time::Duration::from_secs(
                    config.versioned_messages.purge_interval_seconds.max(10),
                );
                let batch_size = config.versioned_messages.purge_batch_size.max(1);
                let max_per_tick = config.versioned_messages.max_purge_per_tick;

                tokio::spawn(async move {
                    let mut ticker = tokio::time::interval(interval);
                    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    loop {
                        ticker.tick().await;
                        let cutoff = sockudo_core::history::now_ms().saturating_sub(retention_ms);
                        let mut total: u64 = 0;
                        loop {
                            match purge_store.purge_before(cutoff, batch_size).await {
                                Ok((deleted, has_more)) => {
                                    total = total.saturating_add(deleted);
                                    if !has_more || total >= max_per_tick as u64 {
                                        break;
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        error = %e,
                                        "version store purge failed"
                                    );
                                    break;
                                }
                            }
                        }
                        if total > 0 {
                            tracing::debug!(deleted = total, "version store purge tick complete");
                        }
                    }
                });
                info!(
                    "Version store purge worker started (retention: {}s, interval: {}s)",
                    config.versioned_messages.retention_window_seconds,
                    interval.as_secs()
                );
            }

            builder = builder.version_store(version_store);
        }
        builder = builder.presence_history_store(presence_history_store);

        if let Some(adapter) = state.local_adapter.clone() {
            builder = builder.local_adapter(adapter);
        }
        if let Some(m) = state.metrics.clone() {
            builder = builder.metrics(m);
        }
        if let Some(q) = state.cleanup_queue.clone() {
            builder = builder.cleanup_queue(q);
        }
        #[cfg(feature = "ably-compat")]
        {
            builder = builder.realtime_egress_tap(global_ably_hub());
        }

        #[cfg(feature = "delta")]
        {
            builder = builder.delta_compression(delta_compression_manager.clone());
        }

        builder = builder.running(Arc::clone(&state.running));
        let handler = Arc::new(builder.build());

        // Start dead node cleanup event processing loop (only runs if cluster health is enabled)
        if let Some(event_receiver) = dead_node_event_receiver {
            let handler_clone = handler.clone();
            tokio::spawn(async move {
                info!("Starting dead node cleanup event processing loop");
                while let Ok(event) = event_receiver.recv().await {
                    if let Err(e) = handler_clone.handle_dead_node_cleanup(event).await {
                        error!("Error processing dead node cleanup event: {}", e);
                    }
                }
                info!("Dead node cleanup event processing loop ended");
            });
        }

        // Start replay buffer eviction task (only when connection recovery is enabled)
        #[cfg(feature = "recovery")]
        if config.connection_recovery.enabled
            && let Some(replay_buf) = handler.replay_buffer().cloned()
        {
            let eviction_interval =
                std::time::Duration::from_secs(config.connection_recovery.buffer_ttl_seconds / 4);
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(eviction_interval);
                loop {
                    interval.tick().await;
                    replay_buf.evict_expired();
                }
            });
            info!(
                "Connection recovery replay buffer eviction task started (interval: {}s)",
                eviction_interval.as_secs()
            );
        }

        // Set metrics for adapters using TypedAdapter (lock-free configuration)
        if let Some(metrics_instance_arc) = &metrics {
            if let Err(e) = typed_adapter
                .set_metrics(metrics_instance_arc.clone())
                .await
            {
                warn!("Failed to set metrics for adapter: {}", e);
            } else {
                info!(
                    "Metrics configured for adapter: {:?}",
                    config.adapter.driver
                );
            }
        }

        // Set delta compression for adapters using TypedAdapter (lock-free configuration)
        #[cfg(feature = "delta")]
        {
            typed_adapter
                .set_delta_compression(delta_compression_manager.clone(), app_manager.clone())
                .await;
            info!(
                "Delta compression initialized for adapter: {:?}",
                config.adapter.driver
            );
        }

        // Set tag filtering enabled flag using TypedAdapter
        #[cfg(feature = "tag-filtering")]
        {
            typed_adapter.set_tag_filtering_enabled(config.tag_filtering.enabled);
            if config.tag_filtering.enabled {
                info!(
                    "Tag filtering enabled for adapter: {:?}",
                    config.adapter.driver
                );
            }

            // Set global enable_tags flag using TypedAdapter
            typed_adapter.set_enable_tags_globally(config.tag_filtering.enable_tags);
            info!(
                "Global enable_tags setting: {}",
                config.tag_filtering.enable_tags
            );
        }

        Ok(Self {
            config,
            state,
            handler,
        })
    }

    pub(crate) async fn init(&self) -> Result<()> {
        info!("Server init sequence started.");
        self.state.app_manager.init().await?;

        // Initialize ConnectionManager (Adapter)
        self.state.connection_manager.init().await;

        // Register apps from configuration
        if !self.config.app_manager.array.apps.is_empty() {
            info!(
                "Registering {} apps from configuration",
                self.config.app_manager.array.apps.len()
            );
            let apps_to_register = self.config.app_manager.array.apps.clone();
            for app in apps_to_register {
                info!("Attempting to register app: id={}, key={}", app.id, app.key);
                match self.state.app_manager.find_by_id(&app.id).await {
                    Ok(Some(_existing_app)) => {
                        info!("App {} already exists, attempting to update.", app.id);
                        if let Err(update_err) =
                            self.state.app_manager.update_app(app.clone()).await
                        {
                            error!("Failed to update existing app {}: {}", app.id, update_err);
                        } else {
                            info!("Successfully updated app: {}", app.id);
                        }
                    }
                    Ok(None) => match self.state.app_manager.create_app(app.clone()).await {
                        Ok(_) => info!("Successfully registered new app: {}", app.id),
                        Err(create_err) => {
                            error!("Failed to register new app {}: {}", app.id, create_err)
                        }
                    },
                    Err(e) => {
                        error!(
                            "Error checking existence of app {}: {}. Skipping registration/update.",
                            app.id, e
                        );
                    }
                }
            }
        }

        // Log registered apps
        match self.state.app_manager.get_apps().await {
            Ok(apps) => {
                info!("Server has {} registered apps:", apps.len());
                for app in apps {
                    info!(
                        "- App: id={}, key={}, enabled={}",
                        app.id, app.key, app.enabled
                    );
                }
            }
            Err(e) => warn!("Failed to retrieve registered apps: {}", e),
        }

        // Initialize Metrics
        if let Some(metrics) = &self.state.metrics
            && let Err(e) = metrics.init().await
        {
            warn!("Failed to initialize metrics: {}", e);
        }
        info!("Server init sequence completed.");
        Ok(())
    }
}
