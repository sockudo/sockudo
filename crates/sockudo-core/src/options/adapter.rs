use super::*;
use ahash::AHashMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AdapterConfig {
    pub driver: AdapterDriver,
    pub redis: RedisAdapterConfig,
    pub cluster: RedisClusterAdapterConfig,
    pub nats: NatsAdapterConfig,
    pub pulsar: PulsarAdapterConfig,
    pub rabbitmq: RabbitMqAdapterConfig,
    pub google_pubsub: GooglePubSubAdapterConfig,
    pub kafka: KafkaAdapterConfig,
    pub iggy: IggyConfig,
    #[serde(default = "default_buffer_multiplier_per_cpu")]
    pub buffer_multiplier_per_cpu: usize,
    pub cluster_health: ClusterHealthConfig,
    #[serde(default = "default_enable_socket_counting")]
    pub enable_socket_counting: bool,
    #[serde(default = "default_fallback_to_local")]
    pub fallback_to_local: bool,
    /// Tier 1A: maintain cluster-wide channel counts locally via gossip so count
    /// reads (subscription_count, /channels, occupancy) become local with zero
    /// cross-node fan-out. Off by default; falls back to request/reply when off.
    #[serde(default = "default_aggregate_counts")]
    pub aggregate_counts: bool,
    /// Use the replicated presence registry for first-join/last-leave transition
    /// checks instead of request/reply. Faster under high churn, but registry
    /// state is eventually consistent, so strict webhook/history behavior keeps
    /// this off by default.
    #[serde(default = "default_fast_presence_transitions")]
    pub fast_presence_transitions: bool,
}

fn default_aggregate_counts() -> bool {
    false
}

fn default_fast_presence_transitions() -> bool {
    false
}

fn default_enable_socket_counting() -> bool {
    true
}

fn default_fallback_to_local() -> bool {
    true
}

fn default_buffer_multiplier_per_cpu() -> usize {
    64
}

impl Default for AdapterConfig {
    fn default() -> Self {
        Self {
            driver: AdapterDriver::default(),
            redis: RedisAdapterConfig::default(),
            cluster: RedisClusterAdapterConfig::default(),
            nats: NatsAdapterConfig::default(),
            pulsar: PulsarAdapterConfig::default(),
            rabbitmq: RabbitMqAdapterConfig::default(),
            google_pubsub: GooglePubSubAdapterConfig::default(),
            kafka: KafkaAdapterConfig::default(),
            iggy: IggyConfig::default(),
            buffer_multiplier_per_cpu: default_buffer_multiplier_per_cpu(),
            cluster_health: ClusterHealthConfig::default(),
            enable_socket_counting: default_enable_socket_counting(),
            fallback_to_local: default_fallback_to_local(),
            aggregate_counts: default_aggregate_counts(),
            fast_presence_transitions: default_fast_presence_transitions(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RedisAdapterConfig {
    pub requests_timeout: u64,
    pub prefix: String,
    pub redis_pub_options: AHashMap<String, sonic_rs::Value>,
    pub redis_sub_options: AHashMap<String, sonic_rs::Value>,
    pub cluster_mode: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RedisClusterAdapterConfig {
    pub nodes: Vec<String>,
    pub prefix: String,
    pub request_timeout_ms: u64,
    pub use_connection_manager: bool,
    #[serde(default)]
    pub use_sharded_pubsub: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct NatsAdapterConfig {
    pub servers: Vec<String>,
    pub prefix: String,
    pub request_timeout_ms: u64,
    pub username: Option<String>,
    pub password: Option<String>,
    pub token: Option<String>,
    pub connection_timeout_ms: u64,
    pub nodes_number: Option<u32>,
    pub discovery_max_wait_ms: u64,
    pub discovery_idle_wait_ms: u64,
    pub subscription_capacity: Option<usize>,
    pub client_capacity: Option<usize>,
    pub max_reconnects: Option<usize>,
    pub presence_sync_chunk_size: Option<usize>,
    pub no_echo: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PulsarAdapterConfig {
    pub url: String,
    pub prefix: String,
    pub request_timeout_ms: u64,
    pub token: Option<String>,
    pub nodes_number: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RabbitMqAdapterConfig {
    pub url: String,
    pub prefix: String,
    pub request_timeout_ms: u64,
    pub connection_timeout_ms: u64,
    pub nodes_number: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct GooglePubSubAdapterConfig {
    pub project_id: String,
    pub prefix: String,
    pub request_timeout_ms: u64,
    pub emulator_host: Option<String>,
    pub nodes_number: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct KafkaAdapterConfig {
    pub brokers: Vec<String>,
    pub prefix: String,
    pub request_timeout_ms: u64,
    pub security_protocol: Option<String>,
    pub sasl_mechanism: Option<String>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
    pub nodes_number: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct IggyConfig {
    pub connection_string: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub consumer_name: Option<String>,
    pub stream: String,
    pub topic_prefix: String,
    pub queue_topic_prefix: String,
    pub consumer_group_prefix: String,
    pub request_timeout_ms: u64,
    pub poll_interval_ms: u64,
    pub poll_batch_size: u32,
    pub partitions_count: u32,
    pub partition_id: u32,
    pub auto_create: bool,
    pub start_from_latest: bool,
    pub nodes_number: Option<u32>,
}

impl Default for RedisAdapterConfig {
    fn default() -> Self {
        Self {
            requests_timeout: 5000,
            prefix: "sockudo_adapter:".to_string(),
            redis_pub_options: AHashMap::new(),
            redis_sub_options: AHashMap::new(),
            cluster_mode: false,
        }
    }
}

impl Default for RedisClusterAdapterConfig {
    fn default() -> Self {
        Self {
            nodes: vec![],
            prefix: "sockudo_adapter:".to_string(),
            request_timeout_ms: 1000,
            use_connection_manager: true,
            use_sharded_pubsub: false,
        }
    }
}

impl Default for NatsAdapterConfig {
    fn default() -> Self {
        Self {
            servers: vec!["nats://localhost:4222".to_string()],
            prefix: "sockudo_adapter:".to_string(),
            request_timeout_ms: 5000,
            username: None,
            password: None,
            token: None,
            connection_timeout_ms: 5000,
            nodes_number: None,
            discovery_max_wait_ms: 1000,
            discovery_idle_wait_ms: 150,
            subscription_capacity: None,
            client_capacity: None,
            max_reconnects: None,
            presence_sync_chunk_size: None,
            no_echo: true,
        }
    }
}

impl Default for PulsarAdapterConfig {
    fn default() -> Self {
        Self {
            url: "pulsar://127.0.0.1:6650".to_string(),
            prefix: "sockudo-adapter".to_string(),
            request_timeout_ms: 5000,
            token: None,
            nodes_number: None,
        }
    }
}

impl Default for RabbitMqAdapterConfig {
    fn default() -> Self {
        Self {
            url: "amqp://guest:guest@127.0.0.1:5672/%2f".to_string(),
            prefix: "sockudo_adapter".to_string(),
            request_timeout_ms: 5000,
            connection_timeout_ms: 5000,
            nodes_number: None,
        }
    }
}

impl Default for GooglePubSubAdapterConfig {
    fn default() -> Self {
        Self {
            project_id: "".to_string(),
            prefix: "sockudo-adapter".to_string(),
            request_timeout_ms: 5000,
            emulator_host: None,
            nodes_number: None,
        }
    }
}

impl Default for KafkaAdapterConfig {
    fn default() -> Self {
        Self {
            brokers: vec!["localhost:9092".to_string()],
            prefix: "sockudo_adapter".to_string(),
            request_timeout_ms: 5000,
            security_protocol: None,
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            nodes_number: None,
        }
    }
}

impl Default for IggyConfig {
    fn default() -> Self {
        Self {
            connection_string: "iggy://iggy:iggy@127.0.0.1:8090".to_string(),
            username: None,
            password: None,
            consumer_name: None,
            stream: "sockudo".to_string(),
            topic_prefix: "sockudo-adapter".to_string(),
            queue_topic_prefix: "sockudo-queue".to_string(),
            consumer_group_prefix: "sockudo-workers".to_string(),
            request_timeout_ms: 5000,
            poll_interval_ms: 50,
            poll_batch_size: 100,
            partitions_count: 1,
            partition_id: 0,
            auto_create: true,
            start_from_latest: true,
            nodes_number: None,
        }
    }
}
