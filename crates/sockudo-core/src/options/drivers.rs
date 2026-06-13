use serde::{Deserialize, Serialize};
use std::str::FromStr;

// --- Enums for Driver Types ---

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum AdapterDriver {
    #[default]
    Local,
    Redis,
    #[serde(rename = "redis-cluster")]
    RedisCluster,
    Nats,
    Pulsar,
    RabbitMq,
    #[serde(rename = "google-pubsub")]
    GooglePubSub,
    Kafka,
    Iggy,
}

impl FromStr for AdapterDriver {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "local" => Ok(AdapterDriver::Local),
            "redis" => Ok(AdapterDriver::Redis),
            "redis-cluster" => Ok(AdapterDriver::RedisCluster),
            "nats" => Ok(AdapterDriver::Nats),
            "pulsar" => Ok(AdapterDriver::Pulsar),
            "rabbitmq" | "rabbit-mq" => Ok(AdapterDriver::RabbitMq),
            "google-pubsub" | "gcp-pubsub" | "pubsub" => Ok(AdapterDriver::GooglePubSub),
            "kafka" => Ok(AdapterDriver::Kafka),
            "iggy" | "apache-iggy" | "apache_iggy" => Ok(AdapterDriver::Iggy),
            _ => Err(format!("Unknown adapter driver: {s}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum AppManagerDriver {
    #[default]
    Memory,
    Mysql,
    Dynamodb,
    PgSql,
    SurrealDb,
    ScyllaDb,
}
impl FromStr for AppManagerDriver {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "memory" => Ok(AppManagerDriver::Memory),
            "mysql" => Ok(AppManagerDriver::Mysql),
            "dynamodb" => Ok(AppManagerDriver::Dynamodb),
            "pgsql" | "postgres" | "postgresql" => Ok(AppManagerDriver::PgSql),
            "surreal" | "surrealdb" => Ok(AppManagerDriver::SurrealDb),
            "scylladb" | "scylla" => Ok(AppManagerDriver::ScyllaDb),
            _ => Err(format!("Unknown app manager driver: {s}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum CacheDriver {
    #[default]
    Memory,
    Redis,
    #[serde(rename = "redis-cluster")]
    RedisCluster,
    None,
}

impl FromStr for CacheDriver {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "memory" => Ok(CacheDriver::Memory),
            "redis" => Ok(CacheDriver::Redis),
            "redis-cluster" => Ok(CacheDriver::RedisCluster),
            "none" => Ok(CacheDriver::None),
            _ => Err(format!("Unknown cache driver: {s}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum QueueDriver {
    Memory,
    #[default]
    Redis,
    #[serde(rename = "redis-cluster")]
    RedisCluster,
    Nats,
    Pulsar,
    RabbitMq,
    #[serde(rename = "google-pubsub")]
    GooglePubSub,
    Kafka,
    Iggy,
    Sqs,
    Sns,
    None,
}

impl FromStr for QueueDriver {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "memory" => Ok(QueueDriver::Memory),
            "redis" => Ok(QueueDriver::Redis),
            "redis-cluster" => Ok(QueueDriver::RedisCluster),
            "nats" => Ok(QueueDriver::Nats),
            "pulsar" => Ok(QueueDriver::Pulsar),
            "rabbitmq" | "rabbit-mq" => Ok(QueueDriver::RabbitMq),
            "google-pubsub" | "gcp-pubsub" | "pubsub" => Ok(QueueDriver::GooglePubSub),
            "kafka" => Ok(QueueDriver::Kafka),
            "iggy" | "apache-iggy" | "apache_iggy" => Ok(QueueDriver::Iggy),
            "sqs" => Ok(QueueDriver::Sqs),
            "sns" => Ok(QueueDriver::Sns),
            "none" => Ok(QueueDriver::None),
            _ => Err(format!("Unknown queue driver: {s}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum DeltaCoordinationBackend {
    #[default]
    Auto,
    None,
    Redis,
    RedisCluster,
    Nats,
}

impl FromStr for DeltaCoordinationBackend {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "none" => Ok(Self::None),
            "redis" => Ok(Self::Redis),
            "redis-cluster" | "redis_cluster" => Ok(Self::RedisCluster),
            "nats" => Ok(Self::Nats),
            _ => Err(format!("Unknown delta coordination backend: {s}")),
        }
    }
}

impl AsRef<str> for QueueDriver {
    fn as_ref(&self) -> &str {
        match self {
            QueueDriver::Memory => "memory",
            QueueDriver::Redis => "redis",
            QueueDriver::RedisCluster => "redis-cluster",
            QueueDriver::Nats => "nats",
            QueueDriver::Pulsar => "pulsar",
            QueueDriver::RabbitMq => "rabbitmq",
            QueueDriver::GooglePubSub => "google-pubsub",
            QueueDriver::Kafka => "kafka",
            QueueDriver::Iggy => "iggy",
            QueueDriver::Sqs => "sqs",
            QueueDriver::Sns => "sns",
            QueueDriver::None => "none",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum MetricsDriver {
    #[default]
    Prometheus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum LogOutputFormat {
    #[default]
    Human,
    Json,
}

impl FromStr for LogOutputFormat {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "human" => Ok(LogOutputFormat::Human),
            "json" => Ok(LogOutputFormat::Json),
            _ => Err(format!("Unknown log output format: {s}")),
        }
    }
}

impl FromStr for MetricsDriver {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "prometheus" => Ok(MetricsDriver::Prometheus),
            _ => Err(format!("Unknown metrics driver: {s}")),
        }
    }
}

impl AsRef<str> for MetricsDriver {
    fn as_ref(&self) -> &str {
        match self {
            MetricsDriver::Prometheus => "prometheus",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DeltaCoordinationBackend, QueueDriver};
    use std::str::FromStr;

    #[test]
    fn queue_driver_parses_broker_backends() {
        assert_eq!(
            QueueDriver::from_str("rabbitmq").unwrap(),
            QueueDriver::RabbitMq
        );
        assert_eq!(QueueDriver::from_str("kafka").unwrap(), QueueDriver::Kafka);
        assert_eq!(
            QueueDriver::from_str("pulsar").unwrap(),
            QueueDriver::Pulsar
        );
        assert_eq!(
            QueueDriver::from_str("google-pubsub").unwrap(),
            QueueDriver::GooglePubSub
        );
    }

    #[test]
    fn delta_coordination_backend_parses_expected_values() {
        assert_eq!(
            DeltaCoordinationBackend::from_str("auto").unwrap(),
            DeltaCoordinationBackend::Auto
        );
        assert_eq!(
            DeltaCoordinationBackend::from_str("redis-cluster").unwrap(),
            DeltaCoordinationBackend::RedisCluster
        );
        assert_eq!(
            DeltaCoordinationBackend::from_str("nats").unwrap(),
            DeltaCoordinationBackend::Nats
        );
    }
}
