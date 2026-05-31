pub mod handler;
pub mod transports;

#[cfg(test)]
mod auth_test;

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

#[cfg(test)]
mod cluster_health_integration_tests;

#[cfg(test)]
mod presence_synchronization_tests;

#[cfg(test)]
mod dead_node_detection_tests;

#[cfg(test)]
mod cross_node_sync_tests;

#[cfg(test)]
mod redis_adapter_cluster_health_tests;

#[cfg(test)]
mod redis_cluster_adapter_health_tests;

#[cfg(test)]
mod nats_adapter_cluster_health_tests;

#[cfg(test)]
mod presence_state_consistency_tests;

#[cfg(test)]
mod local_adapter_fallback_tests;

#[cfg(test)]
mod single_node_optimization_tests;

#[cfg(test)]
mod graceful_departure_tests;

#[cfg(test)]
mod local_channel_members_tests;

#[cfg(test)]
mod user_has_connections_tests;

#[cfg(test)]
mod presence_state_sync_default_test;

#[cfg(test)]
mod active_channels_gauge_tests;

#[cfg(test)]
mod clustered_active_channels_gauge_tests;

#[cfg(test)]
mod filter_index_disconnect_cleanup_tests;
