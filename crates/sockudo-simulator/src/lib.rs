//! Deterministic workload simulator for Sockudo durable realtime invariants.
//!
//! The simulator is deliberately seed-driven. It drives real Sockudo durable
//! core stores, injects deterministic node/network/operator faults, maintains a
//! predictive shadow model, and checks cheap oracles during the run plus a full
//! quiesce oracle at the end.

mod config;
mod error;
mod simulator;

pub use config::{FaultConfig, SimulatorConfig};
pub use error::{SimulatorError, SimulatorResult};
pub use simulator::{DeterministicSimulator, SimulationReport};
