use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::error::{SimulatorError, SimulatorResult};
use crate::push_lab::PushLabConfig;
use crate::workload::WorkloadConfig;

/// Deterministic simulator configuration.
///
/// All workload choices, fault choices, fanout delays, and recovery probes are
/// derived from `seed`. A failure should reproduce with the same configuration
/// and seed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulatorConfig {
    pub seed: u64,
    pub ticks: u64,
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
        self.workload.validate()?;
        self.fault.validate()?;
        self.push.validate()
    }

    pub(crate) fn retention_window(&self) -> Duration {
        Duration::from_secs(10 * 365 * 24 * 60 * 60)
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
        Ok(())
    }
}
