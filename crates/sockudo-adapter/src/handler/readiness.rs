use parking_lot::Mutex;
use sockudo_core::error::{Error, Result};
use sockudo_core::options::Readiness;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
pub(super) struct Capacity {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    live: usize,
    shedding: bool,
    /// Per-app reservations. Entries live for the process lifetime so a guard
    /// released after its app went idle still decrements its own counter.
    apps: HashMap<String, Arc<AppLive>>,
}

#[derive(Default)]
struct AppLive {
    live: AtomicUsize,
}

impl Capacity {
    /// Reserve one live slot for `app_id` before the socket is registered.
    ///
    /// Limits are checked and counters incremented under one mutex: admission
    /// used to read socket counts and register afterwards, so concurrent
    /// upgrades awaiting app lookup inside `add_socket` overshot both limits.
    /// Occupancy is counted for every app, so a quota enabled at runtime sees
    /// the connections that are already open.
    pub(super) fn try_reserve(
        self: &Arc<Self>,
        app_id: &str,
        node_limit: u32,
        app_limit: u32,
    ) -> Result<LiveConnection> {
        let mut state = self.state.lock();
        if node_limit > 0 && state.live >= node_limit as usize {
            return Err(Error::OverCapacity);
        }
        let app_live = match state.apps.get(app_id) {
            Some(app_live) => Arc::clone(app_live),
            None => {
                let app_live = Arc::new(AppLive::default());
                state.apps.insert(app_id.to_owned(), Arc::clone(&app_live));
                app_live
            }
        };
        if app_limit > 0 && app_live.live.load(Ordering::Relaxed) >= app_limit as usize {
            return Err(Error::OverConnectionQuota);
        }
        app_live.live.fetch_add(1, Ordering::Relaxed);
        state.live += 1;
        Ok(LiveConnection {
            capacity: Some(Arc::clone(self)),
            app: app_live,
        })
    }

    pub(super) fn is_ready(&self, max: u32, config: &Readiness) -> bool {
        let mut state = self.state.lock();
        if max == 0 {
            state.shedding = false;
        } else {
            let load = state.live as f64 / f64::from(max);
            if load >= config.high_watermark {
                state.shedding = true;
            } else if load <= config.low_watermark {
                state.shedding = false;
            }
        }
        !state.shedding
    }
}

pub(super) struct LiveConnection {
    capacity: Option<Arc<Capacity>>,
    app: Arc<AppLive>,
}

impl LiveConnection {
    pub(super) fn release(&mut self) {
        if let Some(capacity) = self.capacity.take() {
            capacity.state.lock().live -= 1;
            self.app.live.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl Drop for LiveConnection {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_hysteresis_counts_live_connections_only() {
        let capacity = Arc::new(Capacity::default());
        let config = Readiness::default();
        let mut sockets: Vec<_> = (0..95)
            .map(|_| capacity.try_reserve("app", 0, 0).expect("unlimited"))
            .collect();
        assert!(!capacity.is_ready(100, &config));
        sockets.truncate(90);
        assert!(!capacity.is_ready(100, &config));
        // Cleanup may retain its own socket state after the live guard is released.
        for socket in &mut sockets[85..] {
            socket.release();
        }
        assert!(capacity.is_ready(100, &config));
        sockets.truncate(85); // Releasing twice must not decrement again.
        assert_eq!(capacity.state.lock().live, 85);
        sockets.extend((0..9).map(|_| capacity.try_reserve("app", 0, 0).expect("unlimited")));
        assert!(capacity.is_ready(100, &config));
        sockets.push(capacity.try_reserve("app", 0, 0).expect("unlimited"));
        assert!(!capacity.is_ready(100, &config));
        assert!(capacity.is_ready(0, &config));
    }

    #[test]
    fn reservation_enforces_node_and_app_limits_independently() {
        let capacity = Arc::new(Capacity::default());
        let node = capacity.try_reserve("app", 2, 0).expect("node slot");
        let _second = capacity.try_reserve("app", 2, 0).expect("node slot");
        assert!(matches!(
            capacity.try_reserve("app", 2, 0),
            Err(Error::OverCapacity)
        ));
        // A different app is still refused: the node cap spans all apps.
        assert!(matches!(
            capacity.try_reserve("other", 2, 0),
            Err(Error::OverCapacity)
        ));
        drop(node);

        let app = capacity.try_reserve("limited", 0, 1).expect("app slot");
        assert!(matches!(
            capacity.try_reserve("limited", 0, 1),
            Err(Error::OverConnectionQuota)
        ));
        assert!(
            capacity.try_reserve("unlimited", 0, 0).is_ok(),
            "zero disables both limits"
        );
        drop(app);
        assert!(capacity.try_reserve("limited", 0, 1).is_ok());

        // Occupancy is counted even while the app is unlimited, so a quota
        // enabled at runtime sees the connections that are already open.
        let open = capacity.try_reserve("late-limit", 0, 0).expect("unlimited");
        assert_eq!(
            capacity.state.lock().apps["late-limit"]
                .live
                .load(Ordering::Relaxed),
            1
        );
        assert!(matches!(
            capacity.try_reserve("late-limit", 0, 1),
            Err(Error::OverConnectionQuota)
        ));
        drop(open);
        assert!(capacity.try_reserve("late-limit", 0, 1).is_ok());
    }

    #[test]
    fn concurrent_reservations_never_exceed_the_cap() {
        const MAX: u32 = 32;
        const ATTEMPTS: usize = 256;

        let capacity = Arc::new(Capacity::default());
        let barrier = Arc::new(std::sync::Barrier::new(ATTEMPTS));
        let held = Arc::new(std::sync::Mutex::new(Vec::new()));
        let threads: Vec<_> = (0..ATTEMPTS)
            .map(|_| {
                let capacity = Arc::clone(&capacity);
                let barrier = Arc::clone(&barrier);
                let held = Arc::clone(&held);
                std::thread::spawn(move || {
                    barrier.wait();
                    if let Ok(guard) = capacity.try_reserve("app", MAX, MAX) {
                        held.lock().unwrap().push(guard);
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }

        let granted = held.lock().unwrap().len();
        assert_eq!(granted, MAX as usize);
        assert_eq!(capacity.state.lock().live, MAX as usize);
        assert_eq!(
            capacity.state.lock().apps["app"]
                .live
                .load(Ordering::Relaxed),
            MAX as usize
        );
    }

    #[tokio::test]
    async fn aborted_socket_task_releases_capacity() {
        let capacity = Arc::new(Capacity::default());
        let guard = capacity.try_reserve("app", 0, 0).expect("unlimited");
        let task = tokio::spawn(async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        });
        assert!(!capacity.is_ready(1, &Readiness::default()));
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(capacity.is_ready(1, &Readiness::default()));
        assert_eq!(
            capacity.state.lock().apps["app"]
                .live
                .load(Ordering::Relaxed),
            0,
            "aborting the task must release the app slot too"
        );
    }
}
