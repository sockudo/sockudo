#[cfg(feature = "opentelemetry")]
use crate::horizontal_adapter::RequestType;
use crate::horizontal_adapter::{BroadcastMessage, RequestBody};
use std::collections::BTreeMap;
use tracing::Span;

#[cfg(feature = "opentelemetry")]
use opentelemetry::global;
#[cfg(feature = "opentelemetry")]
use opentelemetry::propagation::{Extractor, Injector};
#[cfg(feature = "opentelemetry")]
use tracing::debug_span;
#[cfg(feature = "opentelemetry")]
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub(crate) fn current_context() -> BTreeMap<String, String> {
    #[cfg(feature = "opentelemetry")]
    {
        let mut carrier = TraceCarrier::default();
        global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&Span::current().context(), &mut carrier);
        });
        carrier.0
    }

    #[cfg(not(feature = "opentelemetry"))]
    BTreeMap::new()
}

/// The consumer span for a broadcast received from the transport, continuing the publisher's trace.
///
/// Every node also receives its own broadcasts and drops them unprocessed, so those get no span: it
/// would report a delivery that never happened.
pub(crate) fn broadcast_consumer_span(broadcast: &BroadcastMessage, local_node_id: &str) -> Span {
    #[cfg(feature = "opentelemetry")]
    {
        if broadcast.node_id == local_node_id {
            return Span::none();
        }
        // `debug_span!`, not `trace_span!`: surrealdb (part of `full`) enables
        // `tracing/release_max_level_debug`, which caps every release binary that includes it at
        // DEBUG. A TRACE span is compiled out there and no filter can turn it back on. These spans
        // are still off at the default `info` filter; `with_telemetry_span_filter` turns them on
        // whenever OpenTelemetry traces are enabled.
        let span = debug_span!(
            target: "sockudo_telemetry",
            "messaging.receive",
            otel.kind = "consumer",
            otel.name = "sockudo broadcast receive",
            messaging.system = "sockudo.horizontal",
            messaging.operation.name = "receive",
            app_id = %broadcast.app_id,
            channel = %broadcast.channel,
        );
        let parent = global::get_text_map_propagator(|propagator| {
            propagator.extract(&TraceCarrierRef(&broadcast.trace_context))
        });
        let _ = span.set_parent(parent);
        span
    }

    #[cfg(not(feature = "opentelemetry"))]
    {
        let _ = (broadcast, local_node_id);
        Span::none()
    }
}

/// The consumer span for a request received from the transport, continuing the requester's trace,
/// or no span when [`traces_request`] rules the request out.
pub(crate) fn request_consumer_span(request: &RequestBody, local_node_id: &str) -> Span {
    #[cfg(feature = "opentelemetry")]
    {
        if !traces_request(request, local_node_id) {
            return Span::none();
        }
        // `debug_span!` for the same reason as in `broadcast_consumer_span`.
        let span = debug_span!(
            target: "sockudo_telemetry",
            "messaging.receive",
            otel.kind = "consumer",
            otel.name = "sockudo request receive",
            messaging.system = "sockudo.horizontal",
            messaging.operation.name = "receive",
            app_id = %request.app_id,
        );
        let parent = global::get_text_map_propagator(|propagator| {
            propagator.extract(&TraceCarrierRef(&request.trace_context))
        });
        let _ = span.set_parent(parent);
        span
    }

    #[cfg(not(feature = "opentelemetry"))]
    {
        let _ = (request, local_node_id);
        Span::none()
    }
}

/// Whether a received request gets a consumer span. It does not when:
///
/// - this node drops it unprocessed: its own request (every node receives what it publishes), or
///   one targeted at another node;
/// - it is periodic cluster upkeep: heartbeats (every `heartbeat_interval_ms` from every node) and,
///   with `aggregate_counts` on, channel-count gossip (a full snapshot about every 5 s, changes up
///   to every 100 ms). They are sent from background loops with no trace to continue, so each one
///   would start a new root trace on every node it reaches. This is the adapter's counterpart of
///   the probe and `/metrics` paths the HTTP server span already skips.
#[cfg(feature = "opentelemetry")]
fn traces_request(request: &RequestBody, local_node_id: &str) -> bool {
    request.node_id != local_node_id
        && request
            .target_node_id
            .as_deref()
            .is_none_or(|target| target == local_node_id)
        && !matches!(
            request.request_type,
            RequestType::Heartbeat
                | RequestType::ChannelCountUpdate
                | RequestType::ChannelCountSync
        )
}

#[cfg(feature = "opentelemetry")]
#[derive(Default)]
struct TraceCarrier(BTreeMap<String, String>);

#[cfg(feature = "opentelemetry")]
impl Injector for TraceCarrier {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_owned(), value);
    }
}

#[cfg(feature = "opentelemetry")]
struct TraceCarrierRef<'a>(&'a BTreeMap<String, String>);

#[cfg(feature = "opentelemetry")]
impl Extractor for TraceCarrierRef<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}

#[cfg(all(test, feature = "opentelemetry"))]
mod tests {
    use super::*;

    fn request(request_type: RequestType, from: &str, target: Option<&str>) -> RequestBody {
        RequestBody {
            request_id: "request".to_string(),
            node_id: from.to_string(),
            app_id: "app".to_string(),
            request_type,
            channel: None,
            socket_id: None,
            user_id: None,
            user_info: None,
            timestamp: None,
            dead_node_id: None,
            target_node_id: target.map(str::to_string),
            channels: None,
            reply_to: None,
            trace_context: BTreeMap::new(),
        }
    }

    #[test]
    fn a_peer_request_this_node_processes_is_traced() {
        assert!(traces_request(
            &request(RequestType::ChannelSockets, "peer", None),
            "local"
        ));
        assert!(traces_request(
            &request(RequestType::PresenceStateSync, "peer", Some("local")),
            "local"
        ));
    }

    #[test]
    fn a_request_this_node_drops_is_not_traced() {
        assert!(!traces_request(
            &request(RequestType::ChannelSockets, "local", None),
            "local"
        ));
        assert!(!traces_request(
            &request(RequestType::PresenceStateSync, "peer", Some("other")),
            "local"
        ));
    }

    #[test]
    fn periodic_cluster_upkeep_is_not_traced() {
        for request_type in [
            RequestType::Heartbeat,
            RequestType::ChannelCountUpdate,
            RequestType::ChannelCountSync,
        ] {
            assert!(!traces_request(
                &request(request_type, "peer", None),
                "local"
            ));
        }
    }
}
