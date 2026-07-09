//! Deterministic workload simulator for Sockudo durable realtime invariants.
//!
//! The simulator is deliberately seed-driven. It drives real Sockudo durable
//! core stores, injects deterministic node/network/operator faults, maintains a
//! predictive shadow model, and checks cheap oracles during the run plus a full
//! quiesce oracle at the end.

mod config;
mod error;
mod io;
mod push_lab;
mod real_subsystems;
mod shrink;
mod simulator;
mod workload;

pub use config::{
    FaultConfig, LivenessConfig, SimulatorConfig, SimulatorMode, StorageFaultConfig, UpgradeConfig,
    UpgradeDataPhase,
};
pub use error::{SimulatorError, SimulatorResult};
pub use push_lab::{PushLabConfig, PushSimulationReport};
pub use shrink::{
    FailureArtifact, FailureRun, RunOutcome, SeedDerivedProfile, SeedDerivedProfileKind,
    ShrinkArtifact, ShrinkError, ShrinkOutcome, ShrinkStep, replay_command_for_artifact,
    run_simulator_for_failure, shrink_failure, shrink_with_runner,
};
pub use simulator::{
    DeterministicSimulator, FailureObservation, ProtocolOracleReport, SimulationReport,
    UpgradeSimulationReport,
};
pub use workload::{ActionWeights, WorkloadAction, WorkloadActionCounts, WorkloadConfig};
