use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use metrics_exporter_tcp::TcpBuilder;
use metrics_util::layers::FanoutBuilder;
use std::net::SocketAddr;
use std::sync::OnceLock;
use tracing::warn;

// Histogram buckets for internal operations (in milliseconds)
//
// Fine at the bottom because the common case is sub-millisecond to low-millisecond, and extended
// past 1000 because the only metric using these - horizontal_adapter_resolve_time - measures a
// cross-pod request whose default request_timeout_ms is 1000. Topping out at 10 meant every timed
// out request landed in +Inf, so histogram_quantile returned the top bucket and p95/p99 were pinned
// there: exactly the tail the metric exists to show was the part it could not express. On a
// four-cluster deployment 5-9% of requests exceeded 10ms, which is enough to clip p95 permanently.
//
// The original boundaries are all still here, so low-end buckets stay comparable across the change.
pub(super) const INTERNAL_LATENCY_HISTOGRAM_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0,
    1000.0, 2500.0,
];

// Histogram buckets for end-to-end operations (in milliseconds)
// Covers typical response times and high-latency scenarios
pub(super) const END_TO_END_LATENCY_HISTOGRAM_BUCKETS: &[f64] = &[
    0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0,
];

pub(super) const ANNOTATION_REBUILD_HISTOGRAM_BUCKETS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
];

pub(super) const PUSH_SECONDS_HISTOGRAM_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

pub(super) const PUSH_FANOUT_HISTOGRAM_BUCKETS: &[f64] =
    &[1.0, 10.0, 100.0, 1_000.0, 10_000.0, 100_000.0, 1_000_000.0];

static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

#[derive(Debug, Clone)]
pub(super) struct ResolvedTcpExporterOptions {
    pub(super) listen_addr: SocketAddr,
    pub(super) buffer_size: Option<usize>,
}

pub(super) fn install_prometheus_recorder(
    prefix: &str,
    tcp_exporter: Option<ResolvedTcpExporterOptions>,
    export_to_opentelemetry: bool,
) -> PrometheusHandle {
    PROMETHEUS_HANDLE
        .get_or_init(|| {
            #[allow(deprecated)]
            let mut builder = PrometheusBuilder::new()
                .set_enable_unit_suffix(false)
                .set_buckets(END_TO_END_LATENCY_HISTOGRAM_BUCKETS)
                .expect("valid Prometheus histogram buckets");

            for (metric, buckets) in [
                (
                    format!("{prefix}horizontal_adapter_resolve_time"),
                    INTERNAL_LATENCY_HISTOGRAM_BUCKETS,
                ),
                (
                    format!("{prefix}annotation_projection_rebuild_duration_seconds"),
                    ANNOTATION_REBUILD_HISTOGRAM_BUCKETS,
                ),
                (
                    "sockudo_push_dispatch_duration_seconds".to_owned(),
                    PUSH_SECONDS_HISTOGRAM_BUCKETS,
                ),
                (
                    "sockudo_push_publish_acceptance_duration_seconds".to_owned(),
                    PUSH_SECONDS_HISTOGRAM_BUCKETS,
                ),
                (
                    "sockudo_push_planner_duration_seconds".to_owned(),
                    PUSH_SECONDS_HISTOGRAM_BUCKETS,
                ),
                (
                    "sockudo_push_fanout_size".to_owned(),
                    PUSH_FANOUT_HISTOGRAM_BUCKETS,
                ),
            ] {
                builder = builder
                    .set_buckets_for_metric(Matcher::Full(metric), buckets)
                    .expect("valid Prometheus histogram bucket override");
            }

            let prometheus_recorder = builder.build_recorder();
            let handle = prometheus_recorder.handle();
            let fanout = FanoutBuilder::default().add_recorder(prometheus_recorder);

            #[cfg(feature = "opentelemetry")]
            let fanout = if export_to_opentelemetry {
                fanout.add_recorder(crate::opentelemetry::OpenTelemetryRecorder::new())
            } else {
                fanout
            };

            #[cfg(not(feature = "opentelemetry"))]
            let _ = export_to_opentelemetry;

            if let Some(tcp_exporter) = tcp_exporter {
                let tcp_recorder = TcpBuilder::new()
                    .listen_address(tcp_exporter.listen_addr)
                    .buffer_size(tcp_exporter.buffer_size)
                    .build();

                match tcp_recorder {
                    Ok(tcp_recorder) => {
                        metrics::set_global_recorder(fanout.add_recorder(tcp_recorder).build())
                            .expect("failed to install metrics-rs fanout recorder");
                    }
                    Err(error) => {
                        warn!(
                            listen_addr = %tcp_exporter.listen_addr,
                            error = %error,
                            "failed to start metrics tcp exporter, continuing with prometheus only"
                        );
                        metrics::set_global_recorder(fanout.build())
                            .expect("failed to install metrics-rs fanout recorder");
                    }
                }
            } else {
                metrics::set_global_recorder(fanout.build())
                    .expect("failed to install metrics-rs fanout recorder");
            }

            handle
        })
        .clone()
}
