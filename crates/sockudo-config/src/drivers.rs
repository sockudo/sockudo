use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum AdapterDriver {
    #[default]
    Local,
    Redis,
    #[serde(rename = "redis-cluster")]
    RedisCluster,
    Nats,
}

impl FromStr for AdapterDriver {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "local" => Ok(Self::Local),
            "redis" => Ok(Self::Redis),
            "redis-cluster" => Ok(Self::RedisCluster),
            "nats" => Ok(Self::Nats),
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
    ScyllaDb,
}

impl FromStr for AppManagerDriver {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "memory" => Ok(Self::Memory),
            "mysql" => Ok(Self::Mysql),
            "dynamodb" => Ok(Self::Dynamodb),
            "pgsql" | "postgres" | "postgresql" => Ok(Self::PgSql),
            "scylladb" | "scylla" => Ok(Self::ScyllaDb),
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
            "memory" => Ok(Self::Memory),
            "redis" => Ok(Self::Redis),
            "redis-cluster" => Ok(Self::RedisCluster),
            "none" => Ok(Self::None),
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
    Sqs,
    None,
}

impl FromStr for QueueDriver {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "memory" => Ok(Self::Memory),
            "redis" => Ok(Self::Redis),
            "redis-cluster" => Ok(Self::RedisCluster),
            "sqs" => Ok(Self::Sqs),
            "none" => Ok(Self::None),
            _ => Err(format!("Unknown queue driver: {s}")),
        }
    }
}

impl AsRef<str> for QueueDriver {
    fn as_ref(&self) -> &str {
        match self {
            Self::Memory => "memory",
            Self::Redis => "redis",
            Self::RedisCluster => "redis-cluster",
            Self::Sqs => "sqs",
            Self::None => "none",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum MetricsDriver {
    #[default]
    Prometheus,
}

impl FromStr for MetricsDriver {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "prometheus" => Ok(Self::Prometheus),
            _ => Err(format!("Unknown metrics driver: {s}")),
        }
    }
}

impl AsRef<str> for MetricsDriver {
    fn as_ref(&self) -> &str {
        match self {
            Self::Prometheus => "prometheus",
        }
    }
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
            "human" => Ok(Self::Human),
            "json" => Ok(Self::Json),
            _ => Err(format!("Unknown log output format: {s}")),
        }
    }
}
