use super::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DynamoDbSettings {
    pub region: String,
    pub table_name: String,
    pub endpoint_url: Option<String>,
    pub aws_access_key_id: Option<String>,
    pub aws_secret_access_key: Option<String>,
    pub aws_profile_name: Option<String>,
}

impl Default for DynamoDbSettings {
    fn default() -> Self {
        Self {
            region: "us-east-1".to_string(),
            table_name: "sockudo-applications".to_string(),
            endpoint_url: None,
            aws_access_key_id: None,
            aws_secret_access_key: None,
            aws_profile_name: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ScyllaDbSettings {
    pub nodes: Vec<String>,
    pub keyspace: String,
    pub table_name: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub replication_class: String,
    pub replication_factor: u32,
}

impl Default for ScyllaDbSettings {
    fn default() -> Self {
        Self {
            nodes: vec!["127.0.0.1:9042".to_string()],
            keyspace: "sockudo".to_string(),
            table_name: "applications".to_string(),
            username: None,
            password: None,
            replication_class: "SimpleStrategy".to_string(),
            replication_factor: 3,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SurrealDbSettings {
    pub url: String,
    pub namespace: String,
    pub database: String,
    pub username: String,
    pub password: String,
    pub table_name: String,
    pub cache_ttl: u64,
    pub cache_max_capacity: u64,
}

impl Default for SurrealDbSettings {
    fn default() -> Self {
        Self {
            url: "ws://127.0.0.1:8000".to_string(),
            namespace: "sockudo".to_string(),
            database: "sockudo".to_string(),
            username: "root".to_string(),
            password: "root".to_string(),
            table_name: "applications".to_string(),
            cache_ttl: 300,
            cache_max_capacity: 100,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct DatabaseConfig {
    pub mysql: DatabaseConnection,
    pub postgres: DatabaseConnection,
    pub redis: RedisConnection,
    pub dynamodb: DynamoDbSettings,
    pub surrealdb: SurrealDbSettings,
    pub scylladb: ScyllaDbSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DatabaseConnection {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: String,
    pub table_name: String,
    pub connection_pool_size: u32,
    pub pool_min: Option<u32>,
    pub pool_max: Option<u32>,
    pub cache_ttl: u64,
    pub cache_cleanup_interval: u64,
    pub cache_max_capacity: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DatabasePooling {
    pub enabled: bool,
    pub min: u32,
    pub max: u32,
}

impl Default for DatabaseConnection {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 3306,
            username: "root".to_string(),
            password: "".to_string(),
            database: "sockudo".to_string(),
            table_name: "applications".to_string(),
            connection_pool_size: 10,
            pool_min: None,
            pool_max: None,
            cache_ttl: 300,
            cache_cleanup_interval: 60,
            cache_max_capacity: 100,
        }
    }
}

impl Default for DatabasePooling {
    fn default() -> Self {
        Self {
            enabled: true,
            min: 2,
            max: 10,
        }
    }
}
