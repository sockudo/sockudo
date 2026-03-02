pub mod prometheus {
    pub use sockudo_metrics::prometheus::*;
}

pub use sockudo_metrics::{MetricsFactory, MetricsInterface, PrometheusMetricsDriver};
