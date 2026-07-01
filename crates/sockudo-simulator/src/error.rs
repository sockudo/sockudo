use thiserror::Error;

pub type SimulatorResult<T> = std::result::Result<T, SimulatorError>;

#[derive(Debug, Error)]
pub enum SimulatorError {
    #[error("invalid simulator config: {0}")]
    Config(String),
    #[error("simulator invariant failed at tick {tick} (seed={seed}): {message}")]
    Invariant {
        seed: u64,
        tick: u64,
        message: String,
    },
    #[error(transparent)]
    Core(#[from] sockudo_core::error::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
