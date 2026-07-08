use serde::{Deserialize, Serialize};
use std::future::Future;
use std::path::Path;
use thiserror::Error;

use crate::config::SimulatorConfig;
use crate::simulator::{DeterministicSimulator, FailureObservation};
use crate::workload::{ActionWeights, WorkloadAction};

#[derive(Debug, Clone)]
pub struct FailureRun {
    pub error: String,
    pub observation: FailureObservation,
}

#[derive(Debug, Clone)]
pub enum RunOutcome {
    Passed,
    Failed(FailureRun),
    Invalid(String),
}

#[derive(Debug, Clone)]
pub struct ShrinkOutcome {
    pub original_config: SimulatorConfig,
    pub original_error: String,
    pub original_observation: FailureObservation,
    pub config: SimulatorConfig,
    pub error: String,
    pub final_observation: FailureObservation,
    pub steps: Vec<ShrinkStep>,
}

impl ShrinkOutcome {
    #[must_use]
    pub fn artifact(
        &self,
        artifact_path: &Path,
        seed_derived_profile: Option<SeedDerivedProfile>,
    ) -> FailureArtifact {
        FailureArtifact {
            config: Box::new(self.config.clone()),
            error: Some(self.error.clone()),
            replay_command: replay_command_for_artifact(artifact_path),
            seed_derived_profile,
            shrink: Some(ShrinkArtifact {
                original_ticks: self.original_config.ticks,
                shrunk_ticks: self.config.ticks,
                original_operations: self.original_observation.operations,
                shrunk_operations: self.final_observation.operations,
                operation_limit: self.config.max_operations,
                original_fault_events: self.original_observation.fault_events,
                shrunk_fault_events: self.final_observation.fault_events,
                fault_limit: self.config.max_faults,
                original_error: self.original_error.clone(),
                steps: self.steps.clone(),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FailureArtifact {
    pub config: Box<SimulatorConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub replay_command: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed_derived_profile: Option<SeedDerivedProfile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shrink: Option<ShrinkArtifact>,
}

impl FailureArtifact {
    #[must_use]
    pub fn new(
        config: SimulatorConfig,
        error: String,
        artifact_path: &Path,
        seed_derived_profile: Option<SeedDerivedProfile>,
    ) -> Self {
        Self {
            config: Box::new(config),
            error: Some(error),
            replay_command: replay_command_for_artifact(artifact_path),
            seed_derived_profile,
            shrink: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SeedDerivedProfile {
    pub kind: SeedDerivedProfileKind,
    pub seed: u64,
    pub generated_config: Box<SimulatorConfig>,
}

impl SeedDerivedProfile {
    #[must_use]
    pub fn swarm(generated_config: &SimulatorConfig) -> Self {
        Self {
            kind: SeedDerivedProfileKind::Swarm,
            seed: generated_config.seed,
            generated_config: Box::new(generated_config.clone()),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SeedDerivedProfileKind {
    Swarm,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShrinkArtifact {
    pub original_ticks: u64,
    pub shrunk_ticks: u64,
    pub original_operations: u64,
    pub shrunk_operations: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_limit: Option<u64>,
    pub original_fault_events: u64,
    pub shrunk_fault_events: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fault_limit: Option<u64>,
    pub original_error: String,
    pub steps: Vec<ShrinkStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ShrinkStep {
    pub field: String,
    pub before: String,
    pub after: String,
}

#[derive(Debug, Error)]
pub enum ShrinkError {
    #[error("initial simulator config is invalid: {0}")]
    InvalidInitialConfig(String),
}

pub async fn shrink_failure(
    config: SimulatorConfig,
    min_ticks: u64,
) -> Result<Option<ShrinkOutcome>, ShrinkError> {
    shrink_with_runner(config, min_ticks, |config| async move {
        run_simulator_for_failure(config).await
    })
    .await
}

pub async fn shrink_with_runner<R, Fut>(
    config: SimulatorConfig,
    min_ticks: u64,
    mut runner: R,
) -> Result<Option<ShrinkOutcome>, ShrinkError>
where
    R: FnMut(SimulatorConfig) -> Fut,
    Fut: Future<Output = RunOutcome>,
{
    let original_failure = match runner(config.clone()).await {
        RunOutcome::Failed(failure) => failure,
        RunOutcome::Passed => return Ok(None),
        RunOutcome::Invalid(error) => return Err(ShrinkError::InvalidInitialConfig(error)),
    };

    let mut state = ShrinkState {
        config: config.clone(),
        failure: original_failure.clone(),
        steps: Vec::new(),
    };

    minimize_ticks(&mut state, min_ticks.max(1), &mut runner).await;
    materialize_operation_limit(&mut state, &mut runner).await;
    minimize_operation_limit(&mut state, &mut runner).await;
    materialize_fault_limit(&mut state, &mut runner).await;
    minimize_fault_limit(&mut state, &mut runner).await;
    shrink_topology(&mut state, &mut runner).await;
    simplify_workload_profile(&mut state, &mut runner).await;
    simplify_fault_profile(&mut state, &mut runner).await;
    minimize_ticks(&mut state, min_ticks.max(1), &mut runner).await;
    materialize_operation_limit(&mut state, &mut runner).await;
    minimize_operation_limit(&mut state, &mut runner).await;
    materialize_fault_limit(&mut state, &mut runner).await;
    minimize_fault_limit(&mut state, &mut runner).await;

    Ok(Some(ShrinkOutcome {
        original_config: config,
        original_error: original_failure.error,
        original_observation: original_failure.observation,
        config: state.config,
        error: state.failure.error,
        final_observation: state.failure.observation,
        steps: state.steps,
    }))
}

pub async fn run_simulator_for_failure(config: SimulatorConfig) -> RunOutcome {
    let mut simulator = match DeterministicSimulator::new(config) {
        Ok(simulator) => simulator,
        Err(error) => return RunOutcome::Invalid(error.to_string()),
    };

    match simulator.run().await {
        Ok(_) => RunOutcome::Passed,
        Err(error) => RunOutcome::Failed(FailureRun {
            error: error.to_string(),
            observation: simulator.failure_observation(),
        }),
    }
}

#[must_use]
pub fn replay_command_for_artifact(path: &Path) -> String {
    format!(
        "cargo run -p sockudo-simulator --bin sockudo-sim -- --corpus-file {}",
        shell_quote_path(path)
    )
}

struct ShrinkState {
    config: SimulatorConfig,
    failure: FailureRun,
    steps: Vec<ShrinkStep>,
}

async fn minimize_ticks<R, Fut>(state: &mut ShrinkState, min_ticks: u64, runner: &mut R)
where
    R: FnMut(SimulatorConfig) -> Fut,
    Fut: Future<Output = RunOutcome>,
{
    let current = state.config.ticks;
    if current <= min_ticks {
        return;
    }
    minimize_u64_field(
        state,
        runner,
        "ticks",
        min_ticks,
        current,
        |config, value| {
            config.ticks = value;
            if let Some(max_operations) = config.max_operations
                && max_operations > value
            {
                config.max_operations = Some(value);
            }
        },
    )
    .await;
}

async fn materialize_operation_limit<R, Fut>(state: &mut ShrinkState, runner: &mut R)
where
    R: FnMut(SimulatorConfig) -> Fut,
    Fut: Future<Output = RunOutcome>,
{
    if state.config.max_operations.is_some()
        || state.failure.observation.operations >= state.config.ticks
    {
        return;
    }
    let operations = state.failure.observation.operations;
    let mut candidate = state.config.clone();
    candidate.max_operations = Some(operations);
    accept_candidate(
        state,
        runner,
        candidate,
        "max_operations",
        "none".to_string(),
        operations.to_string(),
    )
    .await;
}

async fn minimize_operation_limit<R, Fut>(state: &mut ShrinkState, runner: &mut R)
where
    R: FnMut(SimulatorConfig) -> Fut,
    Fut: Future<Output = RunOutcome>,
{
    let current = state
        .config
        .max_operations
        .unwrap_or(state.failure.observation.operations);
    minimize_u64_field(
        state,
        runner,
        "max_operations",
        0,
        current,
        |config, value| {
            config.max_operations = Some(value);
        },
    )
    .await;
}

async fn materialize_fault_limit<R, Fut>(state: &mut ShrinkState, runner: &mut R)
where
    R: FnMut(SimulatorConfig) -> Fut,
    Fut: Future<Output = RunOutcome>,
{
    if state.config.max_faults.is_some() {
        return;
    }
    let faults = state.failure.observation.fault_events;
    let mut candidate = state.config.clone();
    candidate.max_faults = Some(faults);
    accept_candidate(
        state,
        runner,
        candidate,
        "max_faults",
        "none".to_string(),
        faults.to_string(),
    )
    .await;
}

async fn minimize_fault_limit<R, Fut>(state: &mut ShrinkState, runner: &mut R)
where
    R: FnMut(SimulatorConfig) -> Fut,
    Fut: Future<Output = RunOutcome>,
{
    let current = state
        .config
        .max_faults
        .unwrap_or(state.failure.observation.fault_events);
    minimize_u64_field(state, runner, "max_faults", 0, current, |config, value| {
        config.max_faults = Some(value);
    })
    .await;
}

async fn shrink_topology<R, Fut>(state: &mut ShrinkState, runner: &mut R)
where
    R: FnMut(SimulatorConfig) -> Fut,
    Fut: Future<Output = RunOutcome>,
{
    minimize_usize_field(
        state,
        runner,
        "nodes",
        1,
        |config| config.nodes,
        |config, value| {
            config.nodes = value;
        },
    )
    .await;
    minimize_usize_field(
        state,
        runner,
        "clients",
        1,
        |config| config.clients,
        |config, value| {
            config.clients = value;
        },
    )
    .await;
    minimize_usize_field(
        state,
        runner,
        "channels",
        1,
        |config| config.channels,
        |config, value| {
            config.channels = value;
        },
    )
    .await;
    minimize_usize_field(
        state,
        runner,
        "users",
        1,
        |config| config.users,
        |config, value| {
            config.users = value;
        },
    )
    .await;
    minimize_usize_field(
        state,
        runner,
        "push.devices",
        1,
        |config| config.push.devices,
        |config, value| {
            config.push.devices = value;
        },
    )
    .await;
}

async fn simplify_workload_profile<R, Fut>(state: &mut ShrinkState, runner: &mut R)
where
    R: FnMut(SimulatorConfig) -> Fut,
    Fut: Future<Output = RunOutcome>,
{
    for action in WorkloadAction::ALL {
        let weight = state.config.workload.weights.weight(action);
        if weight == 0
            || state.failure.observation.workload_actions.count(action) > 0
            || state.config.workload.weights.total() <= weight
        {
            continue;
        }
        let mut candidate = state.config.clone();
        set_weight(&mut candidate.workload.weights, action, 0);
        accept_candidate(
            state,
            runner,
            candidate,
            &format!("workload.weights.{}", action.as_str()),
            weight.to_string(),
            "0".to_string(),
        )
        .await;
    }

    for action in WorkloadAction::ALL {
        let weight = state.config.workload.weights.weight(action);
        if weight <= 1 {
            continue;
        }
        let mut candidate = state.config.clone();
        set_weight(&mut candidate.workload.weights, action, 1);
        accept_candidate(
            state,
            runner,
            candidate,
            &format!("workload.weights.{}", action.as_str()),
            weight.to_string(),
            "1".to_string(),
        )
        .await;
    }
}

async fn simplify_fault_profile<R, Fut>(state: &mut ShrinkState, runner: &mut R)
where
    R: FnMut(SimulatorConfig) -> Fut,
    Fut: Future<Output = RunOutcome>,
{
    zero_f64(
        state,
        runner,
        "fault.fanout_drop_probability",
        |config| config.fault.fanout_drop_probability,
        |config, value| {
            config.fault.fanout_drop_probability = value;
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "fault.fanout_duplicate_probability",
        |config| config.fault.fanout_duplicate_probability,
        |config, value| {
            config.fault.fanout_duplicate_probability = value;
        },
    )
    .await;
    zero_u64(
        state,
        runner,
        "fault.max_fanout_delay_ticks",
        |config| config.fault.max_fanout_delay_ticks,
        |config, value| {
            config.fault.max_fanout_delay_ticks = value;
            config.push.max_queue_delay_ticks = config.push.max_queue_delay_ticks.min(value);
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "fault.node_crash_probability",
        |config| config.fault.node_crash_probability,
        |config, value| {
            config.fault.node_crash_probability = value;
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "fault.node_restart_probability",
        |config| config.fault.node_restart_probability,
        |config, value| {
            config.fault.node_restart_probability = value;
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "fault.node_pause_probability",
        |config| config.fault.node_pause_probability,
        |config, value| {
            config.fault.node_pause_probability = value;
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "fault.node_resume_probability",
        |config| config.fault.node_resume_probability,
        |config, value| {
            config.fault.node_resume_probability = value;
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "fault.node_partition_probability",
        |config| config.fault.node_partition_probability,
        |config, value| {
            config.fault.node_partition_probability = value;
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "fault.node_heal_probability",
        |config| config.fault.node_heal_probability,
        |config, value| {
            config.fault.node_heal_probability = value;
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "fault.node_slow_probability",
        |config| config.fault.node_slow_probability,
        |config, value| {
            config.fault.node_slow_probability = value;
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "fault.node_stale_probability",
        |config| config.fault.node_stale_probability,
        |config, value| {
            config.fault.node_stale_probability = value;
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "fault.stream_reset_probability",
        |config| config.fault.stream_reset_probability,
        |config, value| {
            config.fault.stream_reset_probability = value;
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "fault.storage.dropped_write_probability",
        |config| config.fault.storage.dropped_write_probability,
        |config, value| {
            config.fault.storage.dropped_write_probability = value;
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "fault.storage.torn_write_probability",
        |config| config.fault.storage.torn_write_probability,
        |config, value| {
            config.fault.storage.torn_write_probability = value;
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "fault.storage.stale_read_probability",
        |config| config.fault.storage.stale_read_probability,
        |config, value| {
            config.fault.storage.stale_read_probability = value;
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "fault.storage.corrupt_read_probability",
        |config| config.fault.storage.corrupt_read_probability,
        |config, value| {
            config.fault.storage.corrupt_read_probability = value;
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "fault.storage.delayed_commit_probability",
        |config| config.fault.storage.delayed_commit_probability,
        |config, value| {
            config.fault.storage.delayed_commit_probability = value;
        },
    )
    .await;
    zero_u64(
        state,
        runner,
        "fault.storage.max_commit_delay_ticks",
        |config| config.fault.storage.max_commit_delay_ticks,
        |config, value| {
            config.fault.storage.max_commit_delay_ticks = value;
        },
    )
    .await;

    zero_f64(
        state,
        runner,
        "push.queue_produce_lost_probability",
        |config| config.push.queue_produce_lost_probability,
        |config, value| {
            config.push.queue_produce_lost_probability = value;
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "push.queue_duplicate_probability",
        |config| config.push.queue_duplicate_probability,
        |config, value| {
            config.push.queue_duplicate_probability = value;
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "push.queue_ack_lost_probability",
        |config| config.push.queue_ack_lost_probability,
        |config, value| {
            config.push.queue_ack_lost_probability = value;
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "push.queue_lease_timeout_probability",
        |config| config.push.queue_lease_timeout_probability,
        |config, value| {
            config.push.queue_lease_timeout_probability = value;
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "push.backend_outage_probability",
        |config| config.push.backend_outage_probability,
        |config, value| {
            config.push.backend_outage_probability = value;
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "push.backend_recovery_probability",
        |config| config.push.backend_recovery_probability,
        |config, value| {
            config.push.backend_recovery_probability = value;
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "push.write_fail_before_commit_probability",
        |config| config.push.write_fail_before_commit_probability,
        |config, value| {
            config.push.write_fail_before_commit_probability = value;
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "push.write_fail_after_commit_probability",
        |config| config.push.write_fail_after_commit_probability,
        |config, value| {
            config.push.write_fail_after_commit_probability = value;
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "push.response_lost_probability",
        |config| config.push.response_lost_probability,
        |config, value| {
            config.push.response_lost_probability = value;
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "push.read_stale_probability",
        |config| config.push.read_stale_probability,
        |config, value| {
            config.push.read_stale_probability = value;
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "push.provider_retryable_probability",
        |config| config.push.provider_retryable_probability,
        |config, value| {
            config.push.provider_retryable_probability = value;
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "push.provider_permanent_rejection_probability",
        |config| config.push.provider_permanent_rejection_probability,
        |config, value| {
            config.push.provider_permanent_rejection_probability = value;
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "push.provider_invalid_token_probability",
        |config| config.push.provider_invalid_token_probability,
        |config, value| {
            config.push.provider_invalid_token_probability = value;
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "push.provider_lost_response_probability",
        |config| config.push.provider_lost_response_probability,
        |config, value| {
            config.push.provider_lost_response_probability = value;
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "push.provider_delayed_result_probability",
        |config| config.push.provider_delayed_result_probability,
        |config, value| {
            config.push.provider_delayed_result_probability = value;
        },
    )
    .await;
    zero_f64(
        state,
        runner,
        "push.provider_duplicate_result_probability",
        |config| config.push.provider_duplicate_result_probability,
        |config, value| {
            config.push.provider_duplicate_result_probability = value;
        },
    )
    .await;
    zero_u64(
        state,
        runner,
        "push.max_queue_delay_ticks",
        |config| config.push.max_queue_delay_ticks,
        |config, value| {
            config.push.max_queue_delay_ticks = value;
        },
    )
    .await;
    zero_u64(
        state,
        runner,
        "push.max_provider_delay_ticks",
        |config| config.push.max_provider_delay_ticks,
        |config, value| {
            config.push.max_provider_delay_ticks = value;
        },
    )
    .await;
    one_u32(
        state,
        runner,
        "push.max_retries",
        |config| config.push.max_retries,
        |config, value| {
            config.push.max_retries = value;
        },
    )
    .await;
    one_u64(
        state,
        runner,
        "push.repair_every_ticks",
        |config| config.push.repair_every_ticks,
        |config, value| {
            config.push.repair_every_ticks = value;
        },
    )
    .await;
}

async fn minimize_u64_field<R, Fut>(
    state: &mut ShrinkState,
    runner: &mut R,
    field: &str,
    min: u64,
    current: u64,
    set: impl Fn(&mut SimulatorConfig, u64),
) where
    R: FnMut(SimulatorConfig) -> Fut,
    Fut: Future<Output = RunOutcome>,
{
    if current <= min {
        return;
    }

    let base = state.config.clone();
    let mut low = min;
    let mut high = current;
    let mut best = None;
    while low < high {
        let mid = low.saturating_add(high.saturating_sub(low) / 2);
        let mut candidate = base.clone();
        set(&mut candidate, mid);
        match runner(candidate).await {
            RunOutcome::Failed(failure) => {
                best = Some((mid, failure));
                high = mid;
            }
            RunOutcome::Passed | RunOutcome::Invalid(_) => {
                low = mid.saturating_add(1);
            }
        }
    }

    if let Some((value, failure)) = best {
        let mut candidate = base;
        set(&mut candidate, value);
        accept_known_failure(
            state,
            candidate,
            failure,
            field,
            current.to_string(),
            value.to_string(),
        );
    }
}

async fn minimize_usize_field<R, Fut>(
    state: &mut ShrinkState,
    runner: &mut R,
    field: &str,
    min: usize,
    get: impl Fn(&SimulatorConfig) -> usize,
    set: impl Fn(&mut SimulatorConfig, usize),
) where
    R: FnMut(SimulatorConfig) -> Fut,
    Fut: Future<Output = RunOutcome>,
{
    let current = get(&state.config);
    if current <= min {
        return;
    }

    let base = state.config.clone();
    let mut low = min;
    let mut high = current;
    let mut best = None;
    while low < high {
        let mid = low.saturating_add(high.saturating_sub(low) / 2);
        let mut candidate = base.clone();
        set(&mut candidate, mid);
        match runner(candidate).await {
            RunOutcome::Failed(failure) => {
                best = Some((mid, failure));
                high = mid;
            }
            RunOutcome::Passed | RunOutcome::Invalid(_) => {
                low = mid.saturating_add(1);
            }
        }
    }

    if let Some((value, failure)) = best {
        let mut candidate = base;
        set(&mut candidate, value);
        accept_known_failure(
            state,
            candidate,
            failure,
            field,
            current.to_string(),
            value.to_string(),
        );
    }
}

async fn zero_f64<R, Fut>(
    state: &mut ShrinkState,
    runner: &mut R,
    field: &str,
    get: impl Fn(&SimulatorConfig) -> f64,
    set: impl Fn(&mut SimulatorConfig, f64),
) where
    R: FnMut(SimulatorConfig) -> Fut,
    Fut: Future<Output = RunOutcome>,
{
    let current = get(&state.config);
    if current == 0.0 {
        return;
    }
    let mut candidate = state.config.clone();
    set(&mut candidate, 0.0);
    accept_candidate(
        state,
        runner,
        candidate,
        field,
        current.to_string(),
        "0".to_string(),
    )
    .await;
}

async fn zero_u64<R, Fut>(
    state: &mut ShrinkState,
    runner: &mut R,
    field: &str,
    get: impl Fn(&SimulatorConfig) -> u64,
    set: impl Fn(&mut SimulatorConfig, u64),
) where
    R: FnMut(SimulatorConfig) -> Fut,
    Fut: Future<Output = RunOutcome>,
{
    let current = get(&state.config);
    if current == 0 {
        return;
    }
    let mut candidate = state.config.clone();
    set(&mut candidate, 0);
    accept_candidate(
        state,
        runner,
        candidate,
        field,
        current.to_string(),
        "0".to_string(),
    )
    .await;
}

async fn one_u64<R, Fut>(
    state: &mut ShrinkState,
    runner: &mut R,
    field: &str,
    get: impl Fn(&SimulatorConfig) -> u64,
    set: impl Fn(&mut SimulatorConfig, u64),
) where
    R: FnMut(SimulatorConfig) -> Fut,
    Fut: Future<Output = RunOutcome>,
{
    let current = get(&state.config);
    if current <= 1 {
        return;
    }
    let mut candidate = state.config.clone();
    set(&mut candidate, 1);
    accept_candidate(
        state,
        runner,
        candidate,
        field,
        current.to_string(),
        "1".to_string(),
    )
    .await;
}

async fn one_u32<R, Fut>(
    state: &mut ShrinkState,
    runner: &mut R,
    field: &str,
    get: impl Fn(&SimulatorConfig) -> u32,
    set: impl Fn(&mut SimulatorConfig, u32),
) where
    R: FnMut(SimulatorConfig) -> Fut,
    Fut: Future<Output = RunOutcome>,
{
    let current = get(&state.config);
    if current <= 1 {
        return;
    }
    let mut candidate = state.config.clone();
    set(&mut candidate, 1);
    accept_candidate(
        state,
        runner,
        candidate,
        field,
        current.to_string(),
        "1".to_string(),
    )
    .await;
}

async fn accept_candidate<R, Fut>(
    state: &mut ShrinkState,
    runner: &mut R,
    candidate: SimulatorConfig,
    field: &str,
    before: String,
    after: String,
) -> bool
where
    R: FnMut(SimulatorConfig) -> Fut,
    Fut: Future<Output = RunOutcome>,
{
    match runner(candidate.clone()).await {
        RunOutcome::Failed(failure) => {
            accept_known_failure(state, candidate, failure, field, before, after);
            true
        }
        RunOutcome::Passed | RunOutcome::Invalid(_) => false,
    }
}

fn accept_known_failure(
    state: &mut ShrinkState,
    candidate: SimulatorConfig,
    failure: FailureRun,
    field: &str,
    before: String,
    after: String,
) {
    if before == after {
        return;
    }
    state.config = candidate;
    state.failure = failure;
    state.steps.push(ShrinkStep {
        field: field.to_string(),
        before,
        after,
    });
}

fn set_weight(weights: &mut ActionWeights, action: WorkloadAction, value: u32) {
    match action {
        WorkloadAction::PublishMessage => weights.publish_message = value,
        WorkloadAction::CreateVersionedMessage => weights.create_versioned_message = value,
        WorkloadAction::MutateVersionedMessage => weights.mutate_versioned_message = value,
        WorkloadAction::PresenceTransition => weights.presence_transition = value,
        WorkloadAction::RecoveryProbe => weights.recovery_probe = value,
        WorkloadAction::PurgeHistory => weights.purge_history = value,
        WorkloadAction::PushRegisterDevice => weights.push_register_device = value,
        WorkloadAction::PushDeleteDevice => weights.push_delete_device = value,
        WorkloadAction::PushSubscribe => weights.push_subscribe = value,
        WorkloadAction::PushUnsubscribe => weights.push_unsubscribe = value,
        WorkloadAction::PushPublish => weights.push_publish = value,
        WorkloadAction::PushScheduledPublish => weights.push_scheduled_publish = value,
        WorkloadAction::PushProviderFeedback => weights.push_provider_feedback = value,
        WorkloadAction::PushRepair => weights.push_repair = value,
        WorkloadAction::OracleCheck => weights.oracle_check = value,
    }
}

fn shell_quote_path(path: &Path) -> String {
    let value = path.display().to_string();
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '.' | '_' | '-' | ':'))
    {
        return value;
    }

    format!("'{}'", value.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::{WorkloadActionCounts, WorkloadConfig};

    #[tokio::test]
    async fn shrinker_minimizes_controlled_failure_fixture() {
        let mut config = SimulatorConfig {
            seed: 7,
            ticks: 64,
            nodes: 5,
            clients: 8,
            channels: 4,
            users: 12,
            workload: WorkloadConfig {
                weights: ActionWeights {
                    publish_message: 10,
                    create_versioned_message: 6,
                    mutate_versioned_message: 4,
                    presence_transition: 3,
                    recovery_probe: 2,
                    purge_history: 1,
                    push_register_device: 2,
                    push_delete_device: 1,
                    push_subscribe: 2,
                    push_unsubscribe: 1,
                    push_publish: 2,
                    push_scheduled_publish: 1,
                    push_provider_feedback: 1,
                    push_repair: 1,
                    oracle_check: 1,
                },
            },
            ..SimulatorConfig::default()
        };
        config.fault.fanout_drop_probability = 0.25;
        config.fault.fanout_duplicate_probability = 0.25;
        config.fault.max_fanout_delay_ticks = 9;
        config.fault.node_crash_probability = 0.10;
        config.fault.storage.dropped_write_probability = 0.10;
        config.push.devices = 10;
        config.push.queue_produce_lost_probability = 0.20;
        config.push.provider_retryable_probability = 0.20;

        let outcome = shrink_with_runner(
            config,
            1,
            |config| async move { controlled_outcome(config) },
        )
        .await
        .unwrap()
        .expect("fixture should fail before and after shrinking");

        assert_eq!(outcome.config.ticks, 7);
        assert_eq!(outcome.config.max_operations, Some(3));
        assert_eq!(outcome.config.max_faults, Some(1));
        assert_eq!(outcome.config.nodes, 2);
        assert_eq!(outcome.config.clients, 2);
        assert_eq!(outcome.config.channels, 1);
        assert_eq!(outcome.config.users, 1);
        assert_eq!(outcome.config.push.devices, 1);
        assert_eq!(outcome.config.workload.weights.publish_message, 1);
        assert_eq!(outcome.config.workload.weights.create_versioned_message, 0);
        assert_eq!(outcome.config.fault.fanout_duplicate_probability, 0.0);
        assert_eq!(outcome.config.fault.max_fanout_delay_ticks, 0);
        assert_eq!(outcome.config.push.provider_retryable_probability, 0.0);
        assert!(outcome.steps.iter().any(|step| step.field == "max_faults"));
    }

    #[test]
    fn artifact_preserves_seed_derived_profile_and_replay_command() {
        let mut generated = SimulatorConfig {
            seed: 0x51a9,
            ..SimulatorConfig::default()
        };
        generated.apply_swarm_profile();
        let mut shrunk = generated.clone();
        shrunk.ticks = 9;

        let outcome = ShrinkOutcome {
            original_config: generated.clone(),
            original_error: "original failure".to_string(),
            original_observation: observation(32, 12, 4, &generated),
            config: shrunk,
            error: "shrunk failure".to_string(),
            final_observation: observation(9, 3, 1, &generated),
            steps: vec![ShrinkStep {
                field: "ticks".to_string(),
                before: "32".to_string(),
                after: "9".to_string(),
            }],
        };

        let path = Path::new("/tmp/sockudo sim failure.json");
        let artifact = outcome.artifact(path, Some(SeedDerivedProfile::swarm(&generated)));
        let encoded = serde_json::to_string(&artifact).unwrap();

        assert!(artifact.replay_command.contains("--corpus-file"));
        assert!(
            artifact
                .replay_command
                .contains("'/tmp/sockudo sim failure.json'")
        );
        assert!(encoded.contains("seedDerivedProfile"));
        assert_eq!(
            artifact.seed_derived_profile.as_ref().unwrap().kind,
            SeedDerivedProfileKind::Swarm
        );
        assert_eq!(artifact.config.ticks, 9);
    }

    fn controlled_outcome(config: SimulatorConfig) -> RunOutcome {
        if config.ticks < 7
            || config.max_operations.unwrap_or(config.ticks) < 3
            || config.max_faults.unwrap_or(4) < 1
            || config.nodes < 2
            || config.clients < 2
            || config.channels < 1
            || config.users < 1
            || config.push.devices < 1
            || config.workload.weights.publish_message == 0
            || config.fault.fanout_drop_probability == 0.0
            || config.fault.node_crash_probability == 0.0
            || config.fault.storage.dropped_write_probability == 0.0
            || config.push.queue_produce_lost_probability == 0.0
        {
            return RunOutcome::Passed;
        }

        RunOutcome::Failed(FailureRun {
            error: "controlled failure".to_string(),
            observation: observation(
                config.ticks,
                config.max_operations.unwrap_or(3).min(3),
                config.max_faults.unwrap_or(1).min(1),
                &config,
            ),
        })
    }

    fn observation(
        tick: u64,
        operations: u64,
        fault_events: u64,
        config: &SimulatorConfig,
    ) -> FailureObservation {
        FailureObservation {
            tick,
            operations,
            fault_events,
            suppressed_fault_events: 0,
            nodes: config.nodes,
            clients: config.clients,
            channels: config.channels,
            users: config.users,
            workload_actions: WorkloadActionCounts {
                publish_message: operations,
                ..WorkloadActionCounts::default()
            },
        }
    }
}
