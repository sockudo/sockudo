//! Opt-in connection timeline telemetry (`enable_stats`).
//!
//! Mirrors the JS client: a bounded timeline of connection events (state
//! changes, retries, errors) is sent as
//! `GET {scheme}://{stats_host}/timeline/v2/fetch/2?…` on connect and every
//! 60 seconds while there is something to report. Every query value is
//! base64-encoded (JSON for structured values). A `{"host": …}` reply
//! redirects later bundles. No payloads, tokens or auth data are recorded.

use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use parking_lot::Mutex;
use sonic_rs::{JsonValueMutTrait, JsonValueTrait, Value};
use tokio::sync::Notify;

use crate::client::Shared;

/// Most events kept between bundles.
const LIMIT: usize = 50;
/// Bundle interval.
const INTERVAL: Duration = Duration::from_secs(60);

pub(crate) struct Timeline {
    session: u32,
    events: Mutex<VecDeque<Value>>,
    sent: Mutex<u64>,
    host: Mutex<String>,
    pub(crate) wake: Notify,
}

impl fmt::Debug for Timeline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Timeline")
            .field("events", &self.events.lock().len())
            .finish_non_exhaustive()
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

impl Timeline {
    pub(crate) fn new(host: String) -> Self {
        Self {
            session: fastrand::u32(..1_000_000_000),
            events: Mutex::new(VecDeque::with_capacity(LIMIT)),
            sent: Mutex::new(0),
            host: Mutex::new(host),
            wake: Notify::new(),
        }
    }

    /// Record an event (a JSON object); a timestamp is added.
    pub(crate) fn log(&self, mut event: Value) {
        if let Some(object) = event.as_object_mut() {
            object.insert("timestamp", Value::from(now_ms()));
        }
        let mut events = self.events.lock();
        events.push_back(event);
        while events.len() > LIMIT {
            events.pop_front();
        }
    }

    /// Drain the timeline into a bundle's query string, or `None` when empty.
    pub(crate) fn bundle_query(
        &self,
        key: &str,
        cluster: Option<&str>,
        extra: &[(String, String)],
    ) -> Option<String> {
        let events: Vec<Value> = {
            let mut events = self.events.lock();
            if events.is_empty() {
                return None;
            }
            events.drain(..).collect()
        };
        let bundle = {
            let mut sent = self.sent.lock();
            *sent += 1;
            *sent
        };
        let timeline = sonic_rs::to_string(&events).unwrap_or_else(|_| "[]".to_owned());
        let mut pairs: Vec<(String, String)> = vec![
            ("session".into(), self.session.to_string()),
            ("bundle".into(), bundle.to_string()),
            ("key".into(), key.to_owned()),
            ("lib".into(), "rust".into()),
            ("version".into(), env!("CARGO_PKG_VERSION").into()),
            ("features".into(), r#"["ws"]"#.into()),
            ("timeline".into(), timeline),
        ];
        if let Some(cluster) = cluster {
            pairs.push(("cluster".into(), cluster.to_owned()));
        }
        pairs.extend(extra.iter().cloned());
        let engine = base64::engine::general_purpose::STANDARD;
        let mut query = form_urlencoded::Serializer::new(String::new());
        for (name, value) in &pairs {
            query.append_pair(name, &engine.encode(value.as_bytes()));
        }
        Some(query.finish())
    }

    fn host(&self) -> String {
        self.host.lock().clone()
    }

    fn redirect(&self, host: &str) {
        if !host.is_empty() {
            *self.host.lock() = host.to_owned();
        }
    }
}

/// Send bundles until the connection task ends (it aborts this task).
pub(crate) async fn run(shared: Arc<Shared>) {
    let Some(timeline) = shared.timeline.as_ref() else {
        return;
    };
    let scheme = if shared.config.use_tls {
        "https"
    } else {
        "http"
    };
    loop {
        tokio::select! {
            () = tokio::time::sleep(INTERVAL) => {}
            () = timeline.wake.notified() => {}
        }
        let Some(query) = timeline.bundle_query(
            &shared.config.key,
            shared.config.cluster.as_deref(),
            &shared.config.timeline_params,
        ) else {
            continue;
        };
        let url = format!("{scheme}://{}/timeline/v2/fetch/2?{query}", timeline.host());
        // Telemetry is best effort: failures are logged and never surfaced.
        match shared
            .http
            .get(url)
            .timeout(Duration::from_secs(10))
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                if let Ok(body) = response.bytes().await
                    && let Ok(reply) = sonic_rs::from_slice::<Value>(&body)
                    && let Some(host) = reply.get("host").and_then(|host| host.as_str())
                {
                    timeline.redirect(host);
                }
            }
            Ok(response) => {
                tracing::debug!(status = %response.status(), "stats endpoint rejected bundle")
            }
            Err(error) => tracing::debug!(error = %error.without_url(), "stats bundle failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(query: &str) -> Vec<(String, String)> {
        let engine = base64::engine::general_purpose::STANDARD;
        form_urlencoded::parse(query.as_bytes())
            .map(|(k, v)| {
                (
                    k.into_owned(),
                    String::from_utf8(engine.decode(v.as_bytes()).unwrap()).unwrap(),
                )
            })
            .collect()
    }

    #[test]
    fn bundles_are_bounded_base64_and_drained() {
        let timeline = Timeline::new("stats.example".into());
        assert!(timeline.bundle_query("k", None, &[]).is_none());
        for index in 0..60 {
            timeline.log(sonic_rs::json!({"state": "connecting", "n": index}));
        }
        let query = timeline
            .bundle_query("app-key", Some("eu"), &[("tenant".into(), "t1".into())])
            .unwrap();
        let pairs = decode(&query);
        let get = |name: &str| {
            pairs
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
        };
        assert_eq!(get("lib").as_deref(), Some("rust"));
        assert_eq!(get("key").as_deref(), Some("app-key"));
        assert_eq!(get("cluster").as_deref(), Some("eu"));
        assert_eq!(get("bundle").as_deref(), Some("1"));
        assert_eq!(get("tenant").as_deref(), Some("t1"));
        let events: Vec<Value> = sonic_rs::from_str(&get("timeline").unwrap()).unwrap();
        assert_eq!(events.len(), LIMIT);
        assert_eq!(events[0]["n"].as_u64(), Some(10));
        assert!(events[0]["timestamp"].as_u64().is_some());
        assert!(timeline.bundle_query("k", None, &[]).is_none(), "drained");
        timeline.log(sonic_rs::json!({"state": "connected"}));
        let pairs = decode(&timeline.bundle_query("k", None, &[]).unwrap());
        assert!(pairs.contains(&("bundle".into(), "2".into())));
    }

    #[test]
    fn replies_can_redirect_the_host() {
        let timeline = Timeline::new("a".into());
        timeline.redirect("b.example");
        assert_eq!(timeline.host(), "b.example");
        timeline.redirect("");
        assert_eq!(timeline.host(), "b.example");
    }
}
