//! Process memory sampling used to shed new connections before a cgroup OOM.

use crate::options::AcceptTraffic;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::Duration;
use tracing::{info, warn};

const LIMIT_SOURCE_UNAVAILABLE: u8 = 0;
const LIMIT_SOURCE_CONFIGURED: u8 = 1;
const LIMIT_SOURCE_CGROUP_V2: u8 = 2;
const LIMIT_SOURCE_CGROUP_V1: u8 = 3;
#[cfg(any(target_os = "linux", test))]
const CGROUP_V1_UNLIMITED_FLOOR: u64 = 1 << 60;

/// Callback notified whenever the current shedding state is sampled.
pub type MemoryPressureObserver = Arc<dyn Fn(bool) + Send + Sync>;

/// Current memory-pressure admission state.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MemoryPressureSnapshot {
    pub enabled: bool,
    pub sample_available: bool,
    pub shedding: bool,
    pub resident_memory_bytes: u64,
    pub memory_limit_bytes: u64,
    pub memory_threshold: f64,
    pub limit_source: &'static str,
}

/// Periodically samples process RSS without depending on metrics collection.
pub struct MemoryPressureMonitor {
    enabled: bool,
    memory_threshold: f64,
    configured_limit_bytes: Option<u64>,
    sample_interval: Duration,
    sample_available: AtomicBool,
    shedding: AtomicBool,
    resident_memory_bytes: AtomicU64,
    memory_limit_bytes: AtomicU64,
    limit_source: AtomicU8,
    unavailable_reported: AtomicBool,
    observer: Option<MemoryPressureObserver>,
}

/// Read the current process resident set size in bytes.
///
/// Linux exposes the current (not peak) RSS through `/proc/self/status`.
pub fn process_resident_memory_bytes() -> io::Result<u64> {
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status")?;
        parse_linux_resident_memory_bytes(&status).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "VmRSS was missing or invalid in /proc/self/status",
            )
        })
    }

    #[cfg(not(target_os = "linux"))]
    {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "current process RSS sampling is supported only on Linux",
        ))
    }
}

impl MemoryPressureMonitor {
    /// Construct a monitor from validated server configuration.
    #[must_use]
    pub fn new(config: &AcceptTraffic, observer: Option<MemoryPressureObserver>) -> Arc<Self> {
        Arc::new(Self {
            enabled: config.enabled,
            memory_threshold: config.memory_threshold,
            configured_limit_bytes: config.memory_limit_bytes.filter(|limit| *limit > 0),
            sample_interval: Duration::from_millis(config.sample_interval_ms.max(1)),
            sample_available: AtomicBool::new(false),
            shedding: AtomicBool::new(false),
            resident_memory_bytes: AtomicU64::new(0),
            memory_limit_bytes: AtomicU64::new(0),
            limit_source: AtomicU8::new(LIMIT_SOURCE_UNAVAILABLE),
            unavailable_reported: AtomicBool::new(false),
            observer,
        })
    }

    /// Take an initial sample and start the independent periodic sampler.
    pub fn start(self: &Arc<Self>) {
        if !self.enabled {
            self.notify_observer(false);
            return;
        }

        self.sample_once();

        let sample_interval = self.sample_interval;
        let monitor = Arc::downgrade(self);
        if let Err(error) = std::thread::Builder::new()
            .name("sockudo-memory-pressure".to_string())
            .spawn(move || {
                loop {
                    std::thread::sleep(sample_interval);
                    let Some(monitor) = monitor.upgrade() else {
                        return;
                    };
                    monitor.sample_once();
                }
            })
        {
            warn!(error = %error, "memory pressure admission sampler could not start");
        }
    }

    /// Whether new connections should currently be rejected for memory pressure.
    #[must_use]
    pub fn is_shedding(&self) -> bool {
        self.enabled && self.shedding.load(Ordering::Acquire)
    }

    /// Return the latest sampled admission state.
    #[must_use]
    pub fn snapshot(&self) -> MemoryPressureSnapshot {
        MemoryPressureSnapshot {
            enabled: self.enabled,
            sample_available: self.sample_available.load(Ordering::Acquire),
            shedding: self.is_shedding(),
            resident_memory_bytes: self.resident_memory_bytes.load(Ordering::Acquire),
            memory_limit_bytes: self.memory_limit_bytes.load(Ordering::Acquire),
            memory_threshold: self.memory_threshold,
            limit_source: limit_source_name(self.limit_source.load(Ordering::Acquire)),
        }
    }

    fn sample_once(&self) {
        match read_memory_sample(self.configured_limit_bytes) {
            Ok(sample) => self.apply_sample(sample),
            Err(error) => self.fail_open(error),
        }
    }

    fn apply_sample(&self, sample: MemorySample) {
        self.resident_memory_bytes
            .store(sample.resident_memory_bytes, Ordering::Release);
        self.memory_limit_bytes
            .store(sample.memory_limit_bytes, Ordering::Release);
        self.limit_source
            .store(sample.limit_source, Ordering::Release);
        self.sample_available.store(true, Ordering::Release);
        self.unavailable_reported.store(false, Ordering::Release);

        let shedding = sample.resident_memory_bytes as f64
            >= sample.memory_limit_bytes as f64 * self.memory_threshold;
        let previous = self.shedding.swap(shedding, Ordering::AcqRel);
        self.notify_observer(shedding);

        if shedding && !previous {
            warn!(
                resident_memory_bytes = sample.resident_memory_bytes,
                memory_limit_bytes = sample.memory_limit_bytes,
                memory_threshold = self.memory_threshold,
                limit_source = limit_source_name(sample.limit_source),
                "memory pressure admission shedding started"
            );
        } else if !shedding && previous {
            info!(
                resident_memory_bytes = sample.resident_memory_bytes,
                memory_limit_bytes = sample.memory_limit_bytes,
                memory_threshold = self.memory_threshold,
                limit_source = limit_source_name(sample.limit_source),
                "memory pressure admission shedding stopped"
            );
        }
    }

    fn fail_open(&self, error: io::Error) {
        self.sample_available.store(false, Ordering::Release);
        self.shedding.store(false, Ordering::Release);
        self.limit_source
            .store(LIMIT_SOURCE_UNAVAILABLE, Ordering::Release);
        self.notify_observer(false);

        if !self.unavailable_reported.swap(true, Ordering::AcqRel) {
            warn!(error = %error, "memory pressure admission sampling unavailable; failing open");
        }
    }

    fn notify_observer(&self, shedding: bool) {
        if let Some(observer) = self.observer.as_ref() {
            observer(shedding);
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct MemorySample {
    resident_memory_bytes: u64,
    memory_limit_bytes: u64,
    limit_source: u8,
}

#[cfg(target_os = "linux")]
fn read_memory_sample(configured_limit_bytes: Option<u64>) -> io::Result<MemorySample> {
    let resident_memory_bytes = process_resident_memory_bytes()?;
    let (memory_limit_bytes, limit_source) = match configured_limit_bytes {
        Some(limit) => (limit, LIMIT_SOURCE_CONFIGURED),
        None => read_linux_cgroup_limit()?,
    };

    Ok(MemorySample {
        resident_memory_bytes,
        memory_limit_bytes,
        limit_source,
    })
}

#[cfg(not(target_os = "linux"))]
fn read_memory_sample(_configured_limit_bytes: Option<u64>) -> io::Result<MemorySample> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "memory pressure admission is supported only on Linux",
    ))
}

#[cfg(target_os = "linux")]
fn read_linux_cgroup_limit() -> io::Result<(u64, u8)> {
    let v2 = std::fs::read_to_string("/sys/fs/cgroup/memory.max");
    if let Ok(value) = v2.as_deref()
        && let Some(limit) = parse_cgroup_limit(value, false)
    {
        return Ok((limit, LIMIT_SOURCE_CGROUP_V2));
    }

    let v1 = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes");
    if let Ok(value) = v1.as_deref()
        && let Some(limit) = parse_cgroup_limit(value, true)
    {
        return Ok((limit, LIMIT_SOURCE_CGROUP_V1));
    }

    let error = match (v2.err(), v1.err()) {
        (Some(v2_error), Some(v1_error)) => io::Error::new(
            v2_error.kind(),
            format!("no readable cgroup memory limit (v2: {v2_error}; v1: {v1_error})"),
        ),
        _ => io::Error::new(
            io::ErrorKind::NotFound,
            "no finite cgroup memory limit is configured",
        ),
    };
    Err(error)
}

#[cfg(any(target_os = "linux", test))]
fn parse_linux_resident_memory_bytes(status: &str) -> Option<u64> {
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    let mut fields = line["VmRSS:".len()..].split_whitespace();
    let value = fields.next()?.parse::<u64>().ok()?;
    match fields.next() {
        Some("kB") => value.checked_mul(1024),
        None => Some(value),
        _ => None,
    }
}

#[cfg(any(target_os = "linux", test))]
fn parse_cgroup_limit(value: &str, is_v1: bool) -> Option<u64> {
    let value = value.trim();
    if value.eq_ignore_ascii_case("max") {
        return None;
    }
    let value = value.parse::<u64>().ok()?;
    if value == 0 || (is_v1 && value >= CGROUP_V1_UNLIMITED_FLOOR) {
        None
    } else {
        Some(value)
    }
}

fn limit_source_name(source: u8) -> &'static str {
    match source {
        LIMIT_SOURCE_CONFIGURED => "configured",
        LIMIT_SOURCE_CGROUP_V2 => "cgroup_v2",
        LIMIT_SOURCE_CGROUP_V1 => "cgroup_v1",
        _ => "unavailable",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn monitor(threshold: f64) -> Arc<MemoryPressureMonitor> {
        MemoryPressureMonitor::new(
            &AcceptTraffic {
                enabled: true,
                memory_threshold: threshold,
                memory_limit_bytes: Some(1_000),
                sample_interval_ms: 500,
            },
            None,
        )
    }

    #[test]
    fn parses_linux_rss_in_kibibytes() {
        assert_eq!(
            parse_linux_resident_memory_bytes("Name:\tsockudo\nVmRSS:\t  123 kB\n"),
            Some(123 * 1024)
        );
        assert_eq!(parse_linux_resident_memory_bytes("Name:\tsockudo\n"), None);
    }

    #[test]
    fn parses_finite_cgroup_limits_and_rejects_unlimited_values() {
        assert_eq!(parse_cgroup_limit("1048576\n", false), Some(1_048_576));
        assert_eq!(parse_cgroup_limit("max\n", false), None);
        assert_eq!(parse_cgroup_limit("0", false), None);
        assert_eq!(parse_cgroup_limit("9223372036854771712", true), None);
    }

    #[test]
    fn threshold_crossing_changes_only_new_connection_admission() {
        let monitor = monitor(0.9);
        monitor.apply_sample(MemorySample {
            resident_memory_bytes: 899,
            memory_limit_bytes: 1_000,
            limit_source: LIMIT_SOURCE_CONFIGURED,
        });
        assert!(!monitor.is_shedding());

        monitor.apply_sample(MemorySample {
            resident_memory_bytes: 900,
            memory_limit_bytes: 1_000,
            limit_source: LIMIT_SOURCE_CONFIGURED,
        });
        assert!(monitor.is_shedding());
        assert!(monitor.snapshot().sample_available);
    }

    #[test]
    fn unavailable_sample_fails_open() {
        let monitor = monitor(0.9);
        monitor.apply_sample(MemorySample {
            resident_memory_bytes: 950,
            memory_limit_bytes: 1_000,
            limit_source: LIMIT_SOURCE_CONFIGURED,
        });
        assert!(monitor.is_shedding());

        monitor.fail_open(io::Error::new(
            io::ErrorKind::NotFound,
            "test source missing",
        ));
        assert!(!monitor.is_shedding());
        assert!(!monitor.snapshot().sample_available);
    }

    #[test]
    fn observer_tracks_shedding_and_fail_open_state() {
        let observed = Arc::new(AtomicBool::new(false));
        let observer_state = Arc::clone(&observed);
        let monitor = MemoryPressureMonitor::new(
            &AcceptTraffic {
                enabled: true,
                memory_threshold: 0.9,
                memory_limit_bytes: Some(1_000),
                sample_interval_ms: 500,
            },
            Some(Arc::new(move |shedding| {
                observer_state.store(shedding, Ordering::Release);
            })),
        );

        monitor.apply_sample(MemorySample {
            resident_memory_bytes: 950,
            memory_limit_bytes: 1_000,
            limit_source: LIMIT_SOURCE_CONFIGURED,
        });
        assert!(observed.load(Ordering::Acquire));

        monitor.fail_open(io::Error::new(
            io::ErrorKind::NotFound,
            "test source missing",
        ));
        assert!(!observed.load(Ordering::Acquire));
    }
}
