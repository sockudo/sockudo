pub mod prometheus;

pub use prometheus::PrometheusMetricsDriver;
pub use sockudo_core::metrics::MetricsInterface;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct MetricsFactory;

impl MetricsFactory {
    pub async fn create(
        driver_type: &str,
        port: u16,
        prefix: Option<&str>,
    ) -> Option<Arc<Mutex<dyn MetricsInterface + Send + Sync>>> {
        match driver_type.to_lowercase().as_str() {
            "prometheus" => {
                let driver = PrometheusMetricsDriver::new(port, prefix).await;
                Some(Arc::new(Mutex::new(driver)))
            }
            _ => None,
        }
    }
}
