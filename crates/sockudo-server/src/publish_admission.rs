//! Latency-based load shedding for HTTP publishes.
//!
//! Without it, a node past its CPU or storage capacity keeps accepting every
//! publish and each one waits longer, until callers time out with no signal
//! to back off. The controller watches the fastest publish of each interval:
//! if even that one missed `target_latency_ms`, the node is overloaded, and
//! for the next interval it admits only the concurrency it has shown it can
//! serve within the target (Little's law: completions per second times the
//! target latency). Requests over that limit get `503` with `Retry-After`
//! before their body is read or their signature is checked. Once publishes
//! meet the target again the cap doubles each interval, and it is lifted
//! after an interval in which it did not have to shed anything, so the node
//! does not swing between flooding and shedding.

use crate::http_handler::AppError;
use axum::{body::Body, extract::State, http::Request, middleware::Next, response::Response};
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::PublishAdmission;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{debug, warn};

pub struct PublishAdmissionController {
    target: Duration,
    interval: Duration,
    state: Mutex<AdmissionState>,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
}

struct AdmissionState {
    window_start: Instant,
    window_min: Option<Duration>,
    window_completions: u32,
    /// Whether the cap shed any request in the current window.
    window_shed: bool,
    in_flight: u32,
    /// Concurrency cap while overloaded; `None` admits everything.
    limit: Option<u32>,
}

impl PublishAdmissionController {
    pub fn new(
        config: &PublishAdmission,
        metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    ) -> Self {
        Self::starting_at(config, metrics, Instant::now())
    }

    fn starting_at(
        config: &PublishAdmission,
        metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
        now: Instant,
    ) -> Self {
        Self {
            target: Duration::from_millis(config.target_latency_ms),
            interval: Duration::from_millis(config.interval_ms),
            state: Mutex::new(AdmissionState {
                window_start: now,
                window_min: None,
                window_completions: 0,
                window_shed: false,
                in_flight: 0,
                limit: None,
            }),
            metrics,
        }
    }

    /// Admits a publish, or returns `None` when it should be shed.
    fn try_admit(self: &Arc<Self>, now: Instant) -> Option<AdmissionGuard> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        self.roll_window(&mut state, now);
        if state.limit.is_some_and(|limit| state.in_flight >= limit) {
            state.window_shed = true;
            return None;
        }
        state.in_flight += 1;
        Some(AdmissionGuard {
            controller: Arc::clone(self),
            started: now,
        })
    }

    fn complete(&self, started: Instant, now: Instant) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.in_flight = state.in_flight.saturating_sub(1);
        self.roll_window(&mut state, now);
        let elapsed = now.saturating_duration_since(started);
        state.window_min = Some(state.window_min.map_or(elapsed, |min| min.min(elapsed)));
        state.window_completions = state.window_completions.saturating_add(1);
    }

    fn roll_window(&self, state: &mut AdmissionState, now: Instant) {
        let elapsed = now.saturating_duration_since(state.window_start);
        if elapsed < self.interval {
            return;
        }
        let was_shedding = state.limit.is_some();
        state.limit = match state.window_min {
            Some(min) if min > self.target => {
                // Little's law: the concurrency that completes within the
                // target at the throughput the node just achieved.
                let per_target = u128::from(state.window_completions) * self.target.as_nanos()
                    / elapsed.as_nanos().max(1);
                Some(u32::try_from(per_target).unwrap_or(u32::MAX).max(1))
            }
            // Back under target: grow a cap that is still shedding, and
            // lift it once it no longer binds.
            Some(_) => match state.limit {
                Some(limit) if state.window_shed => Some(limit.saturating_mul(2)),
                _ => None,
            },
            // Nothing finished this window: keep the decision while
            // requests are still stuck, and reset once the node is idle.
            None if state.in_flight > 0 => state.limit,
            None => None,
        };
        state.window_start = now;
        state.window_min = None;
        state.window_completions = 0;
        state.window_shed = false;

        let shedding = state.limit.is_some();
        if shedding != was_shedding {
            if let Some(limit) = state.limit {
                warn!(
                    limit,
                    target_latency_ms = self.target.as_millis() as u64,
                    "publish latency over target; shedding HTTP publishes"
                );
            } else {
                debug!("publish latency back under target; admitting all HTTP publishes");
            }
            if let Some(metrics) = &self.metrics {
                metrics.update_publish_shedding(shedding);
            }
        }
    }
}

/// Releases the admission slot when the request finishes or is dropped, so
/// a cancelled request still counts as a (slow) completion.
pub struct AdmissionGuard {
    controller: Arc<PublishAdmissionController>,
    started: Instant,
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        self.controller.complete(self.started, Instant::now());
    }
}

pub async fn publish_admission_middleware(
    State(controller): State<Arc<PublishAdmissionController>>,
    request: Request<Body>,
    next: Next,
) -> Result<Response, AppError> {
    let Some(_guard) = controller.try_admit(Instant::now()) else {
        if let Some(metrics) = &controller.metrics {
            metrics.mark_publish_shed();
        }
        return Err(AppError::Backpressure {
            message: "Publish latency is over target; retry later".to_string(),
            retry_after_seconds: 1,
        });
    };
    Ok(next.run(request).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);

    fn controller(start: Instant) -> Arc<PublishAdmissionController> {
        Arc::new(PublishAdmissionController::starting_at(
            &PublishAdmission {
                enabled: true,
                target_latency_ms: 100,
                interval_ms: 1000,
            },
            None,
            start,
        ))
    }

    /// Completes an admitted request at `at` instead of at drop time.
    fn finish(c: &PublishAdmissionController, guard: AdmissionGuard, at: Instant) {
        let started = guard.started;
        std::mem::forget(guard);
        c.complete(started, at);
    }

    /// Admits a request at `at` and completes it after `latency`.
    fn run(c: &Arc<PublishAdmissionController>, at: Instant, latency: Duration) {
        let guard = c.try_admit(at).expect("admitted");
        finish(c, guard, at + latency);
    }

    fn limit(c: &PublishAdmissionController) -> Option<u32> {
        c.state.lock().unwrap().limit
    }

    #[test]
    fn healthy_latency_never_sheds() {
        let t0 = Instant::now();
        let c = controller(t0);
        for i in 0..50 {
            run(&c, t0 + 40 * MS * i, 5 * MS);
        }
        let held: Vec<_> = (0..500).map(|_| c.try_admit(t0 + 2100 * MS)).collect();
        assert!(held.iter().all(Option::is_some));
        assert_eq!(limit(&c), None);
    }

    #[test]
    fn one_fast_publish_in_the_window_keeps_admitting() {
        let t0 = Instant::now();
        let c = controller(t0);
        run(&c, t0, 900 * MS);
        run(&c, t0 + 100 * MS, 10 * MS);
        assert!(c.try_admit(t0 + 1001 * MS).is_some());
        assert_eq!(limit(&c), None);
    }

    #[test]
    fn sustained_slow_publishes_cap_concurrency_and_recover() {
        let t0 = Instant::now();
        let c = controller(t0);
        // 40 publishes completed in the first second, all slower than 100 ms.
        for i in 0..40 {
            run(&c, t0 + 20 * MS * i, 150 * MS);
        }
        // 40/s × 100 ms = 4 concurrent publishes allowed.
        let now = t0 + 1000 * MS;
        let held: Vec<_> = (0..4)
            .map(|_| c.try_admit(now).expect("admitted"))
            .collect();
        assert_eq!(limit(&c), Some(4));
        assert!(
            c.try_admit(now).is_none(),
            "fifth concurrent publish is shed"
        );

        // The admitted ones finish fast while the cap is still shedding, so
        // the next window doubles it instead of opening the floodgates.
        for guard in held {
            finish(&c, guard, now + 20 * MS);
        }
        let next = now + 1000 * MS;
        let held: Vec<_> = (0..8)
            .map(|_| c.try_admit(next).expect("admitted"))
            .collect();
        assert_eq!(limit(&c), Some(8));
        for guard in held {
            finish(&c, guard, next + 20 * MS);
        }

        // A fast window that shed nothing lifts the cap.
        let lifted = next + 1000 * MS;
        let held: Vec<_> = (0..20).map(|_| c.try_admit(lifted)).collect();
        assert!(held.iter().all(Option::is_some));
        assert_eq!(limit(&c), None);
    }

    #[test]
    fn stuck_publishes_keep_shedding_until_idle() {
        let t0 = Instant::now();
        let c = controller(t0);
        run(&c, t0, 500 * MS);
        let stuck = c.try_admit(t0 + 600 * MS).expect("admitted");
        // Window 1 closes slow; one publish is still running.
        assert!(c.try_admit(t0 + 1000 * MS).is_none());
        // Window 2 saw no completions while the publish is stuck: keep shedding.
        assert!(c.try_admit(t0 + 2000 * MS).is_none());
        finish(&c, stuck, t0 + 2500 * MS);
        // That completion was slow too: window 3 admits one publish at a time.
        let one = c.try_admit(t0 + 3000 * MS).expect("admitted at the floor");
        assert_eq!(limit(&c), Some(1));
        assert!(c.try_admit(t0 + 3000 * MS).is_none());
        finish(&c, one, t0 + 3010 * MS);
        // A fast completion after shedding doubles the cap in window 4.
        let two: Vec<_> = (0..2)
            .map(|_| c.try_admit(t0 + 4000 * MS).expect("admitted"))
            .collect();
        assert_eq!(limit(&c), Some(2));
        for guard in two {
            finish(&c, guard, t0 + 4010 * MS);
        }
        // An idle node with nothing in flight resets.
        assert!(c.try_admit(t0 + 10_000 * MS).is_some());
        assert_eq!(limit(&c), None);
    }
}
