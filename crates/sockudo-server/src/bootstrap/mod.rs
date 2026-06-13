#[cfg(feature = "push")]
pub(crate) mod push;
mod router;
mod runtime;
mod server;

use crate::cleanup::{CleanupConfig, CleanupSender};
use sockudo_adapter::ConnectionHandler;
use sockudo_adapter::ConnectionManager;
use sockudo_core::app::AppManager;
use sockudo_core::auth::AuthValidator;
use sockudo_core::cache::CacheManager;
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::ServerOptions;
use sockudo_core::rate_limiter::RateLimiter;
use sockudo_webhook::WebhookIntegration;
use sockudo_webhook::integration::QueueManager;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

/// Factory for creating metrics instances
pub struct MetricsFactory;

impl MetricsFactory {
    /// Create a new metrics driver based on the specified driver type
    pub async fn create(
        driver_type: &str,
        port: u16,
        prefix: Option<&str>,
        tcp_exporter: Option<sockudo_metrics::TcpExporterOptions>,
    ) -> Option<Arc<dyn MetricsInterface + Send + Sync>> {
        match driver_type.to_lowercase().as_str() {
            "prometheus" => {
                let driver = sockudo_metrics::PrometheusMetricsDriver::with_tcp_exporter(
                    port,
                    prefix,
                    tcp_exporter,
                )
                .await;
                Some(Arc::new(driver))
            }
            _ => None,
        }
    }
}

/// Server state containing all managers
struct ServerState {
    app_manager: Arc<dyn AppManager + Send + Sync>,
    connection_manager: Arc<dyn ConnectionManager + Send + Sync>,
    local_adapter: Option<Arc<sockudo_adapter::local_adapter::LocalAdapter>>,
    auth_validator: Arc<AuthValidator>,
    cache_manager: Arc<dyn CacheManager + Send + Sync>,
    queue_manager: Option<Arc<QueueManager>>,
    webhooks_integration: Arc<WebhookIntegration>,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    running: AtomicBool,
    http_api_rate_limiter: Option<Arc<dyn RateLimiter + Send + Sync>>,
    websocket_rate_limiter: Option<Arc<dyn RateLimiter + Send + Sync>>,
    #[cfg(feature = "push")]
    push_acceptance_rate_limiter: Arc<dyn RateLimiter + Send + Sync>,
    debug_enabled: bool,
    cleanup_queue: Option<CleanupSender>,
    cleanup_worker_handles: Option<Vec<tokio::task::JoinHandle<()>>>,
    cleanup_config: CleanupConfig,
    #[cfg(feature = "delta")]
    delta_compression: Arc<sockudo_delta::DeltaCompressionManager>,
    /// Typed adapter for configuration and runtime type inspection
    typed_adapter: sockudo_adapter::factory::TypedAdapter,
    #[cfg(feature = "push")]
    push_store: sockudo_push::DynPushStore,
    #[cfg(feature = "push")]
    push_queue: sockudo_push::DynPushQueue,
}

/// Main server struct
pub(crate) struct SockudoServer {
    config: ServerOptions,
    state: ServerState,
    handler: Arc<ConnectionHandler>,
}
