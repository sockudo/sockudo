use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::error::{SimulatorError, SimulatorResult};
use crate::push_lab::PushLabConfig;
use crate::workload::{ActionWeights, WorkloadConfig};

const SWARM_SEED_DOMAIN: u64 = 0x51a9_5a6d_b33f_500d;

/// Deterministic simulator configuration.
///
/// All workload choices, fault choices, fanout delays, and recovery probes are
/// derived from `seed`. A failure should reproduce with the same configuration
/// and seed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulatorConfig {
    pub seed: u64,
    pub ticks: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_operations: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_faults: Option<u64>,
    #[serde(default)]
    pub mode: SimulatorMode,
    #[serde(default)]
    pub liveness: LivenessConfig,
    pub nodes: usize,
    pub clients: usize,
    pub channels: usize,
    pub users: usize,
    pub oracle_every: u64,
    pub page_limit: usize,
    pub history_retention_messages: Option<usize>,
    pub presence_retention_events: Option<usize>,
    pub workload: WorkloadConfig,
    pub fault: FaultConfig,
    #[serde(default)]
    pub push: PushLabConfig,
}

impl Default for SimulatorConfig {
    fn default() -> Self {
        Self {
            seed: 0x5eed_5eed_cafe_f00d,
            ticks: 5_000,
            max_operations: None,
            max_faults: None,
            mode: SimulatorMode::default(),
            liveness: LivenessConfig::default(),
            nodes: 5,
            clients: 8,
            channels: 4,
            users: 16,
            oracle_every: 25,
            page_limit: 7,
            history_retention_messages: Some(512),
            presence_retention_events: Some(512),
            workload: WorkloadConfig::default(),
            fault: FaultConfig::default(),
            push: PushLabConfig::default(),
        }
    }
}

impl SimulatorConfig {
    /// Apply a deterministic swarm profile derived only from `seed`.
    ///
    /// Swarm mode randomizes the distribution of topology, workload, and fault
    /// parameters before the run starts. The seed still fully reproduces the
    /// resulting configuration.
    pub fn apply_swarm_profile(&mut self) {
        let mut rng = StdRng::seed_from_u64(self.seed ^ SWARM_SEED_DOMAIN);
        self.nodes = rng.random_range(3..=9);
        self.clients = rng.random_range(4..=32);
        self.channels = rng.random_range(2..=12);
        self.users = rng.random_range(self.clients.max(8)..=self.clients.saturating_mul(8).max(16));
        self.page_limit = rng.random_range(1..=17);
        self.oracle_every = rng.random_range(5..=50);
        self.history_retention_messages = if rng.random_ratio(1, 4) {
            Some(rng.random_range(32..=256))
        } else {
            Some(rng.random_range(256..=2048))
        };
        self.presence_retention_events = if rng.random_ratio(1, 4) {
            Some(rng.random_range(32..=256))
        } else {
            Some(rng.random_range(256..=2048))
        };

        self.workload.weights = ActionWeights {
            publish_message: rng.random_range(15..=55),
            create_versioned_message: rng.random_range(4..=25),
            mutate_versioned_message: rng.random_range(4..=35),
            presence_transition: rng.random_range(5..=35),
            recovery_probe: rng.random_range(3..=15),
            purge_history: rng.random_range(0..=5),
            push_register_device: rng.random_range(2..=14),
            push_delete_device: rng.random_range(0..=7),
            push_subscribe: rng.random_range(3..=14),
            push_unsubscribe: rng.random_range(0..=9),
            push_publish: rng.random_range(4..=20),
            push_scheduled_publish: rng.random_range(0..=8),
            push_provider_feedback: rng.random_range(0..=5),
            push_repair: rng.random_range(1..=6),
            oracle_check: rng.random_range(1..=4),
        };

        self.fault.fanout_drop_probability = rng.random_range(0.02..=0.22);
        self.fault.fanout_duplicate_probability = rng.random_range(0.00..=0.10);
        self.fault.max_fanout_delay_ticks = rng.random_range(0..=25);
        self.fault.node_crash_probability = rng.random_range(0.0005..=0.010);
        self.fault.node_restart_probability = rng.random_range(0.010..=0.080);
        self.fault.node_pause_probability = rng.random_range(0.0005..=0.010);
        self.fault.node_resume_probability = rng.random_range(0.010..=0.080);
        self.fault.node_partition_probability = rng.random_range(0.0005..=0.012);
        self.fault.node_heal_probability = rng.random_range(0.010..=0.080);
        self.fault.node_slow_probability = rng.random_range(0.0005..=0.014);
        self.fault.node_stale_probability = rng.random_range(0.0005..=0.010);
        self.fault.stream_reset_probability = rng.random_range(0.0000..=0.0025);
        self.fault.storage.dropped_write_probability = rng.random_range(0.000..=0.020);
        self.fault.storage.torn_write_probability = rng.random_range(0.000..=0.014);
        self.fault.storage.stale_read_probability = rng.random_range(0.000..=0.035);
        self.fault.storage.corrupt_read_probability = rng.random_range(0.000..=0.012);
        self.fault.storage.delayed_commit_probability = rng.random_range(0.000..=0.030);
        self.fault.storage.max_commit_delay_ticks = rng.random_range(0..=25);

        self.push.devices =
            rng.random_range(self.clients.max(8)..=self.clients.saturating_mul(8).max(16));
        self.push.max_retries = rng.random_range(2..=7);
        self.push.repair_every_ticks = rng.random_range(3..=31);
        self.push.max_queue_delay_ticks = self.fault.max_fanout_delay_ticks;
        self.push.max_provider_delay_ticks = rng.random_range(0..=25);
        self.push.queue_produce_lost_probability = rng.random_range(0.005..=0.080);
        self.push.queue_duplicate_probability = rng.random_range(0.000..=0.120);
        self.push.queue_ack_lost_probability = rng.random_range(0.000..=0.060);
        self.push.queue_lease_timeout_probability = rng.random_range(0.000..=0.060);
        self.push.backend_outage_probability = rng.random_range(0.000..=0.008);
        self.push.backend_recovery_probability = rng.random_range(0.020..=0.120);
        self.push.write_fail_before_commit_probability = rng.random_range(0.000..=0.025);
        self.push.write_fail_after_commit_probability = rng.random_range(0.000..=0.025);
        self.push.response_lost_probability = rng.random_range(0.000..=0.035);
        self.push.read_stale_probability = rng.random_range(0.000..=0.030);
        self.push.provider_retryable_probability = rng.random_range(0.040..=0.250);
        self.push.provider_permanent_rejection_probability = rng.random_range(0.010..=0.100);
        self.push.provider_invalid_token_probability = rng.random_range(0.005..=0.080);
        self.push.provider_lost_response_probability = rng.random_range(0.000..=0.060);
        self.push.provider_delayed_result_probability = rng.random_range(0.000..=0.120);
        self.push.provider_duplicate_result_probability = rng.random_range(0.000..=0.120);
    }

    pub(crate) fn validate(&self) -> SimulatorResult<()> {
        if self.ticks == 0 {
            return Err(SimulatorError::Config(
                "ticks must be greater than 0".into(),
            ));
        }
        if self.nodes == 0 {
            return Err(SimulatorError::Config(
                "nodes must be greater than 0".into(),
            ));
        }
        if self.clients == 0 {
            return Err(SimulatorError::Config(
                "clients must be greater than 0".into(),
            ));
        }
        if self.channels == 0 {
            return Err(SimulatorError::Config(
                "channels must be greater than 0".into(),
            ));
        }
        if self.users == 0 {
            return Err(SimulatorError::Config(
                "users must be greater than 0".into(),
            ));
        }
        if self.page_limit == 0 {
            return Err(SimulatorError::Config(
                "page_limit must be greater than 0".into(),
            ));
        }
        if self.history_retention_messages == Some(0) {
            return Err(SimulatorError::Config(
                "history_retention_messages must be greater than 0 when set".into(),
            ));
        }
        if self.presence_retention_events == Some(0) {
            return Err(SimulatorError::Config(
                "presence_retention_events must be greater than 0 when set".into(),
            ));
        }
        self.liveness.validate()?;
        self.workload.validate()?;
        self.fault.validate()?;
        self.push.validate()
    }

    pub(crate) fn retention_window(&self) -> Duration {
        Duration::from_secs(10 * 365 * 24 * 60 * 60)
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SimulatorMode {
    #[default]
    Safety,
    Liveness,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LivenessConfig {
    pub max_quiesce_ticks: u64,
    pub require_recovery_after_drop: bool,
    pub require_repair_after_queue_loss: bool,
}

impl Default for LivenessConfig {
    fn default() -> Self {
        Self {
            max_quiesce_ticks: 10_000,
            require_recovery_after_drop: true,
            require_repair_after_queue_loss: true,
        }
    }
}

impl LivenessConfig {
    fn validate(&self) -> SimulatorResult<()> {
        if self.max_quiesce_ticks == 0 {
            return Err(SimulatorError::Config(
                "liveness max_quiesce_ticks must be greater than 0".into(),
            ));
        }
        Ok(())
    }
}

/// Fault injection probabilities and network delay knobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaultConfig {
    pub fanout_drop_probability: f64,
    pub fanout_duplicate_probability: f64,
    pub max_fanout_delay_ticks: u64,
    pub node_crash_probability: f64,
    pub node_restart_probability: f64,
    pub node_pause_probability: f64,
    pub node_resume_probability: f64,
    pub node_partition_probability: f64,
    pub node_heal_probability: f64,
    pub node_slow_probability: f64,
    pub node_stale_probability: f64,
    pub stream_reset_probability: f64,
    #[serde(default)]
    pub storage: StorageFaultConfig,
}

impl Default for FaultConfig {
    fn default() -> Self {
        Self {
            fanout_drop_probability: 0.08,
            fanout_duplicate_probability: 0.03,
            max_fanout_delay_ticks: 12,
            node_crash_probability: 0.002,
            node_restart_probability: 0.020,
            node_pause_probability: 0.002,
            node_resume_probability: 0.030,
            node_partition_probability: 0.003,
            node_heal_probability: 0.020,
            node_slow_probability: 0.003,
            node_stale_probability: 0.002,
            stream_reset_probability: 0.0005,
            storage: StorageFaultConfig::default(),
        }
    }
}

impl FaultConfig {
    fn validate(&self) -> SimulatorResult<()> {
        for (name, value) in [
            ("fanout_drop_probability", self.fanout_drop_probability),
            (
                "fanout_duplicate_probability",
                self.fanout_duplicate_probability,
            ),
            ("node_crash_probability", self.node_crash_probability),
            ("node_restart_probability", self.node_restart_probability),
            ("node_pause_probability", self.node_pause_probability),
            ("node_resume_probability", self.node_resume_probability),
            (
                "node_partition_probability",
                self.node_partition_probability,
            ),
            ("node_heal_probability", self.node_heal_probability),
            ("node_slow_probability", self.node_slow_probability),
            ("node_stale_probability", self.node_stale_probability),
            ("stream_reset_probability", self.stream_reset_probability),
        ] {
            if !(0.0..=1.0).contains(&value) {
                return Err(SimulatorError::Config(format!(
                    "{name} must be between 0.0 and 1.0"
                )));
            }
        }
        self.storage.validate()
    }
}

/// Storage-specific fault injection probabilities for real durable-store calls.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageFaultConfig {
    pub dropped_write_probability: f64,
    pub torn_write_probability: f64,
    pub stale_read_probability: f64,
    pub corrupt_read_probability: f64,
    pub delayed_commit_probability: f64,
    pub max_commit_delay_ticks: u64,
}

impl Default for StorageFaultConfig {
    fn default() -> Self {
        Self {
            dropped_write_probability: 0.004,
            torn_write_probability: 0.002,
            stale_read_probability: 0.006,
            corrupt_read_probability: 0.002,
            delayed_commit_probability: 0.006,
            max_commit_delay_ticks: 8,
        }
    }
}

impl StorageFaultConfig {
    fn validate(&self) -> SimulatorResult<()> {
        for (name, value) in [
            (
                "storage.dropped_write_probability",
                self.dropped_write_probability,
            ),
            (
                "storage.torn_write_probability",
                self.torn_write_probability,
            ),
            (
                "storage.stale_read_probability",
                self.stale_read_probability,
            ),
            (
                "storage.corrupt_read_probability",
                self.corrupt_read_probability,
            ),
            (
                "storage.delayed_commit_probability",
                self.delayed_commit_probability,
            ),
        ] {
            if !(0.0..=1.0).contains(&value) {
                return Err(SimulatorError::Config(format!(
                    "{name} must be between 0.0 and 1.0"
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn swarm_profile_is_seed_deterministic() {
        let mut left = SimulatorConfig {
            seed: 0x5eed,
            ..SimulatorConfig::default()
        };
        let mut right = left.clone();

        left.apply_swarm_profile();
        right.apply_swarm_profile();

        assert_eq!(left.nodes, right.nodes);
        assert_eq!(left.clients, right.clients);
        assert_eq!(left.channels, right.channels);
        assert_eq!(left.workload.weights, right.workload.weights);
        assert_eq!(
            left.fault.fanout_drop_probability,
            right.fault.fanout_drop_probability
        );
        assert_eq!(
            left.push.queue_produce_lost_probability,
            right.push.queue_produce_lost_probability
        );
        assert_eq!(
            left.fault.storage.dropped_write_probability,
            right.fault.storage.dropped_write_probability
        );
        assert_eq!(
            left.fault.storage.max_commit_delay_ticks,
            right.fault.storage.max_commit_delay_ticks
        );
        left.validate().unwrap();
        right.validate().unwrap();
    }

    #[test]
    fn different_swarm_seeds_produce_different_profiles() {
        let mut left = SimulatorConfig {
            seed: 1,
            ..SimulatorConfig::default()
        };
        let mut right = SimulatorConfig {
            seed: 2,
            ..SimulatorConfig::default()
        };

        left.apply_swarm_profile();
        right.apply_swarm_profile();

        assert_ne!(
            (
                left.nodes,
                left.clients,
                left.channels,
                left.workload.weights,
                left.fault.max_fanout_delay_ticks,
            ),
            (
                right.nodes,
                right.clients,
                right.channels,
                right.workload.weights,
                right.fault.max_fanout_delay_ticks,
            )
        );
    }
}
