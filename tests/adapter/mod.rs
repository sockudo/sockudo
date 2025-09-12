pub mod handler;
pub mod transports;

#[cfg(test)]
mod horizontal_adapter_helpers;

#[cfg(test)]
mod horizontal_adapter_base_test;

#[cfg(test)]
mod horizontal_adapter_integration;

#[cfg(test)]
mod horizontal_adapter_failure_tests;

#[cfg(test)]
mod horizontal_adapter_race_tests;

#[cfg(test)]
mod cluster_health_tests;
