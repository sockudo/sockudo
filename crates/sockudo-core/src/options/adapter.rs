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
    pub omq: OmqAdapterConfig,
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
            omq: OmqAdapterConfig::default(),
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
    /// TLS settings for cluster data connections and shard listeners.
    pub tls: super::RedisTlsOptions,
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
    /// Additional Apache Iggy cluster nodes (`host:port`) dialed in order when the
    /// `connection_string` node is unreachable. They only bootstrap the connection: once
    /// connected, the SDK learns the full roster from cluster metadata, follows the leader,
    /// and fails over across the roster on its own.
    pub cluster_seeds: Vec<String>,
    /// Bound on the TCP dial to each node while `cluster_seeds` is non-empty, so a dead seed
    /// is skipped quickly. Without seeds the SDK's own reconnection policy applies.
    pub connect_timeout_ms: u64,
    /// How long a broadcast or queue publish, or the sign-in to a reachable seed, waits for
    /// the Iggy cluster to elect a leader or fail over before giving up. Must outlast the
    /// cluster's `heartbeat_timeout` plus its election.
    pub failover_timeout_ms: u64,
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
    /// Message durability for topics Sockudo auto-creates on a VSR cluster.
    pub durability: IggyDurability,
    /// Consumer-offset durability for topics Sockudo auto-creates on a VSR cluster.
    pub consumer_offset_durability: IggyDurability,
}

/// Apache Iggy VSR topic durability policy. Single-node servers accept both values.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum IggyDurability {
    /// Quorum commit and local application, without an extra stable-storage barrier.
    #[default]
    Replicated,
    /// Quorum commit backed by recoverable stable-storage copies at the required quorum.
    Persisted,
}

impl IggyDurability {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Replicated => "replicated",
            Self::Persisted => "persisted",
        }
    }
}

impl std::fmt::Display for IggyDurability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for IggyDurability {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "replicated" => Ok(Self::Replicated),
            "persisted" => Ok(Self::Persisted),
            _ => Err(format!(
                "Unknown Apache Iggy durability '{s}', expected 'replicated' or 'persisted'"
            )),
        }
    }
}

/// One Apache Iggy node to dial during bootstrap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IggyConnectionCandidate {
    /// The `host:port` dialed. Safe to log.
    pub address: String,
    /// Full connection string, including credentials. Never log it.
    pub connection_string: String,
}

impl IggyConnectionCandidate {
    /// Whether the candidate uses the TCP transport, the only one a plain TCP
    /// reachability probe can check.
    pub fn is_tcp(&self) -> bool {
        self.connection_string.starts_with("iggy://")
            || self.connection_string.starts_with("iggy+tcp://")
    }
}

impl IggyConfig {
    /// Nodes to dial in order: `connection_string` first, then one per `cluster_seeds`
    /// entry. Each seed reuses the credentials and query options of `connection_string`
    /// with only the `host:port` swapped.
    pub fn connection_candidates(&self) -> Result<Vec<IggyConnectionCandidate>, String> {
        let (scheme, rest) = self
            .connection_string
            .split_once("://")
            .ok_or_else(|| "Apache Iggy connection_string must include a scheme".to_string())?;
        let (authority, query) = match rest.split_once('?') {
            Some((authority, query)) => (authority, Some(query)),
            None => (rest, None),
        };
        let (credentials, primary) = match authority.rsplit_once('@') {
            Some((credentials, host)) => (Some(credentials), host),
            None => (None, authority),
        };

        let mut candidates = vec![IggyConnectionCandidate {
            address: primary.to_string(),
            connection_string: self.connection_string.clone(),
        }];
        for seed in &self.cluster_seeds {
            let seed = seed.trim();
            validate_iggy_seed(seed)?;
            if candidates
                .iter()
                .any(|candidate| candidate.address.eq_ignore_ascii_case(seed))
            {
                continue;
            }

            let mut connection_string = format!("{scheme}://");
            if let Some(credentials) = credentials {
                connection_string.push_str(credentials);
                connection_string.push('@');
            }
            connection_string.push_str(seed);
            if let Some(query) = query {
                connection_string.push('?');
                connection_string.push_str(query);
            }
            candidates.push(IggyConnectionCandidate {
                address: seed.to_string(),
                connection_string,
            });
        }
        Ok(candidates)
    }
}

fn validate_iggy_seed(seed: &str) -> Result<(), String> {
    let valid = !seed.is_empty()
        && !seed.contains(['/', '@', '?', '#', ','])
        && !seed.chars().any(char::is_whitespace)
        && seed
            .rsplit_once(':')
            .is_some_and(|(host, port)| !host.is_empty() && port.parse::<u16>().is_ok());
    if valid {
        Ok(())
    } else {
        Err(format!(
            "Invalid Apache Iggy cluster seed '{seed}', expected host:port"
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OmqAdapterConfig {
    pub bind_endpoint: String,
    pub connect_endpoints: Vec<String>,
    pub prefix: String,
    pub request_timeout_ms: u64,
    pub nodes_number: Option<u32>,
    pub io_threads: usize,
    pub send_hwm: u32,
    pub recv_hwm: u32,
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
            tls: super::RedisTlsOptions::default(),
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
            cluster_seeds: Vec::new(),
            connect_timeout_ms: 2_000,
            failover_timeout_ms: 15_000,
            username: None,
            password: None,
            consumer_name: None,
            stream: "sockudo".to_string(),
            topic_prefix: "sockudo-adapter".to_string(),
            queue_topic_prefix: "sockudo-queue".to_string(),
            consumer_group_prefix: "sockudo-workers".to_string(),
            request_timeout_ms: 5000,
            poll_interval_ms: 5,
            poll_batch_size: 100,
            partitions_count: 1,
            partition_id: 0,
            auto_create: true,
            start_from_latest: true,
            nodes_number: None,
            durability: IggyDurability::Replicated,
            consumer_offset_durability: IggyDurability::Replicated,
        }
    }
}

impl Default for OmqAdapterConfig {
    fn default() -> Self {
        Self {
            bind_endpoint: "tcp://127.0.0.1:5556".to_string(),
            connect_endpoints: Vec::new(),
            prefix: "sockudo_adapter".to_string(),
            request_timeout_ms: 5000,
            nodes_number: None,
            io_threads: 1,
            send_hwm: 100_000,
            recv_hwm: 100_000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{IggyConfig, IggyConnectionCandidate, IggyDurability};

    #[test]
    fn iggy_polling_defaults_to_low_latency_interval() {
        assert_eq!(IggyConfig::default().poll_interval_ms, 5);
    }

    #[test]
    fn iggy_candidates_without_seeds_is_the_connection_string() {
        let config = IggyConfig::default();
        assert_eq!(
            config.connection_candidates().unwrap(),
            vec![IggyConnectionCandidate {
                address: "127.0.0.1:8090".to_string(),
                connection_string: config.connection_string.clone(),
            }]
        );
    }

    #[test]
    fn iggy_seeds_reuse_credentials_and_options() {
        let config = IggyConfig {
            connection_string: "iggy://user:p%40ss@iggy-1:8090?reconnection_retries=5&tls=true"
                .to_string(),
            cluster_seeds: vec![" iggy-2:8090 ".to_string(), "10.0.0.3:8091".to_string()],
            ..Default::default()
        };
        let candidates = config.connection_candidates().unwrap();
        assert_eq!(
            candidates
                .iter()
                .map(|candidate| candidate.address.as_str())
                .collect::<Vec<_>>(),
            vec!["iggy-1:8090", "iggy-2:8090", "10.0.0.3:8091"]
        );
        assert_eq!(
            candidates
                .iter()
                .map(|candidate| candidate.connection_string.as_str())
                .collect::<Vec<_>>(),
            vec![
                "iggy://user:p%40ss@iggy-1:8090?reconnection_retries=5&tls=true",
                "iggy://user:p%40ss@iggy-2:8090?reconnection_retries=5&tls=true",
                "iggy://user:p%40ss@10.0.0.3:8091?reconnection_retries=5&tls=true",
            ]
        );
    }

    #[test]
    fn iggy_seeds_skip_duplicates_of_known_nodes() {
        let config = IggyConfig {
            connection_string: "iggy+tcp://iggy:iggy@Iggy-1:8090".to_string(),
            cluster_seeds: vec![
                "iggy-1:8090".to_string(),
                "iggy-2:8090".to_string(),
                "IGGY-2:8090".to_string(),
            ],
            ..Default::default()
        };
        assert_eq!(
            config
                .connection_candidates()
                .unwrap()
                .into_iter()
                .map(|candidate| candidate.connection_string)
                .collect::<Vec<_>>(),
            vec![
                "iggy+tcp://iggy:iggy@Iggy-1:8090".to_string(),
                "iggy+tcp://iggy:iggy@iggy-2:8090".to_string(),
            ]
        );
    }

    #[test]
    fn iggy_candidates_report_tcp_transport() {
        for (connection_string, tcp) in [
            ("iggy://iggy:iggy@iggy-1:8090", true),
            ("iggy+tcp://iggy:iggy@iggy-1:8090", true),
            ("iggy+quic://iggy:iggy@iggy-1:8080", false),
            ("iggy+ws://iggy:iggy@iggy-1:8092", false),
        ] {
            let candidate = IggyConnectionCandidate {
                address: "iggy-1:8090".to_string(),
                connection_string: connection_string.to_string(),
            };
            assert_eq!(candidate.is_tcp(), tcp, "{connection_string}");
        }
    }

    #[test]
    fn iggy_seeds_reject_values_that_are_not_host_port() {
        for seed in [
            "",
            "iggy-2",
            ":8090",
            "iggy-2:port",
            "iggy-2:70000",
            "iggy://iggy-2:8090",
            "user@iggy-2:8090",
            "iggy-2:8090,iggy-3:8090",
            "iggy 2:8090",
        ] {
            let config = IggyConfig {
                cluster_seeds: vec![seed.to_string()],
                ..Default::default()
            };
            assert!(
                config.connection_candidates().is_err(),
                "seed {seed:?} should be rejected"
            );
        }
    }

    #[test]
    fn iggy_durability_parses_and_round_trips() {
        assert_eq!(IggyConfig::default().durability, IggyDurability::Replicated);
        assert_eq!(
            IggyConfig::default().consumer_offset_durability,
            IggyDurability::Replicated
        );
        assert_eq!(
            " Persisted ".parse::<IggyDurability>().unwrap(),
            IggyDurability::Persisted
        );
        assert_eq!(IggyDurability::Persisted.to_string(), "persisted");
        assert!("quorum".parse::<IggyDurability>().is_err());

        let config: IggyConfig = sonic_rs::from_str(
            r#"{"durability":"persisted","cluster_seeds":["iggy-2:8090"],"connect_timeout_ms":2500,"failover_timeout_ms":20000}"#,
        )
        .unwrap();
        assert_eq!(config.durability, IggyDurability::Persisted);
        assert_eq!(
            config.consumer_offset_durability,
            IggyDurability::Replicated
        );
        assert_eq!(config.cluster_seeds, vec!["iggy-2:8090".to_string()]);
        assert_eq!(config.connect_timeout_ms, 2500);
        assert_eq!(config.failover_timeout_ms, 20_000);
    }
}
