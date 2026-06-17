use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RedisConnection {
    pub host: String,
    pub port: u16,
    pub db: u32,
    pub username: Option<String>,
    pub password: Option<String>,
    pub key_prefix: String,
    pub sentinels: Vec<RedisSentinel>,
    pub sentinel_password: Option<String>,
    pub name: String,
    pub cluster: RedisClusterConnection,
    /// Legacy field kept for backward compatibility. Prefer `database.redis.cluster.nodes`.
    pub cluster_nodes: Vec<ClusterNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RedisSentinel {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct RedisClusterConnection {
    pub nodes: Vec<ClusterNode>,
    pub username: Option<String>,
    pub password: Option<String>,
    #[serde(alias = "useTLS")]
    pub use_tls: bool,
}

impl RedisConnection {
    /// Returns true if Redis Sentinel is configured.
    pub fn is_sentinel_configured(&self) -> bool {
        !self.sentinels.is_empty()
    }

    /// Builds a Redis connection URL based on the configuration.
    pub fn to_url(&self) -> String {
        if self.is_sentinel_configured() {
            self.build_sentinel_url()
        } else {
            self.build_standard_url()
        }
    }

    fn build_standard_url(&self) -> String {
        // Extract scheme from host if present, otherwise default to redis://
        let (scheme, host) = if self.host.starts_with("rediss://") {
            ("rediss://", self.host.trim_start_matches("rediss://"))
        } else if self.host.starts_with("redis://") {
            ("redis://", self.host.trim_start_matches("redis://"))
        } else {
            ("redis://", self.host.as_str())
        };

        let mut url = String::from(scheme);

        if let Some(ref username) = self.username {
            url.push_str(username);
            if let Some(ref password) = self.password {
                url.push(':');
                url.push_str(&urlencoding::encode(password));
            }
            url.push('@');
        } else if let Some(ref password) = self.password {
            url.push(':');
            url.push_str(&urlencoding::encode(password));
            url.push('@');
        }

        url.push_str(host);
        url.push(':');
        url.push_str(&self.port.to_string());
        url.push('/');
        url.push_str(&self.db.to_string());

        url
    }

    fn build_sentinel_url(&self) -> String {
        let mut url = String::from("redis+sentinel://");

        if let Some(ref sentinel_password) = self.sentinel_password {
            url.push(':');
            url.push_str(&urlencoding::encode(sentinel_password));
            url.push('@');
        }

        let sentinel_hosts: Vec<String> = self
            .sentinels
            .iter()
            .map(|s| format!("{}:{}", s.host, s.port))
            .collect();
        url.push_str(&sentinel_hosts.join(","));

        url.push('/');
        url.push_str(&self.name);
        url.push('/');
        url.push_str(&self.db.to_string());

        let mut params = Vec::new();
        if let Some(ref password) = self.password {
            params.push(format!("password={}", urlencoding::encode(password)));
        }
        if let Some(ref username) = self.username {
            params.push(format!("username={}", urlencoding::encode(username)));
        }

        if !params.is_empty() {
            url.push('?');
            url.push_str(&params.join("&"));
        }

        url
    }

    /// Returns true when cluster nodes are configured via either the new (`cluster.nodes`)
    /// or legacy (`cluster_nodes`) field.
    pub fn has_cluster_nodes(&self) -> bool {
        !self.cluster.nodes.is_empty() || !self.cluster_nodes.is_empty()
    }

    /// Returns normalized Redis Cluster seed URLs from the canonical cluster configuration.
    /// Falls back to legacy `cluster_nodes` for backward compatibility.
    pub fn cluster_node_urls(&self) -> Vec<String> {
        if !self.cluster.nodes.is_empty() {
            return self.build_cluster_urls(&self.cluster.nodes);
        }
        self.build_cluster_urls(&self.cluster_nodes)
    }

    /// Normalizes any list of seed strings (`host:port`, `redis://...`, `rediss://...`) using
    /// shared cluster auth/TLS options.
    pub fn normalize_cluster_seed_urls(&self, seeds: &[String]) -> Vec<String> {
        self.build_cluster_urls(
            &seeds
                .iter()
                .filter_map(|seed| ClusterNode::from_seed(seed))
                .collect::<Vec<ClusterNode>>(),
        )
    }

    fn build_cluster_urls(&self, nodes: &[ClusterNode]) -> Vec<String> {
        let username = self
            .cluster
            .username
            .as_deref()
            .or(self.username.as_deref());
        let password = self
            .cluster
            .password
            .as_deref()
            .or(self.password.as_deref());
        let use_tls = self.cluster.use_tls;

        nodes
            .iter()
            .map(|node| node.to_url_with_options(use_tls, username, password))
            .collect()
    }
}

impl RedisSentinel {
    pub fn to_host_port(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ClusterNode {
    pub host: String,
    pub port: u16,
}

impl ClusterNode {
    pub fn to_url(&self) -> String {
        self.to_url_with_options(false, None, None)
    }

    pub fn to_url_with_options(
        &self,
        use_tls: bool,
        username: Option<&str>,
        password: Option<&str>,
    ) -> String {
        let host = self.host.trim();

        if host.starts_with("redis://") || host.starts_with("rediss://") {
            if let Ok(parsed) = Url::parse(host)
                && let Some(host_str) = parsed.host_str()
            {
                let scheme = parsed.scheme();
                let port = parsed.port_or_known_default().unwrap_or(self.port);
                let parsed_username = (!parsed.username().is_empty()).then_some(parsed.username());
                let parsed_password = parsed.password();
                let has_embedded_auth = parsed_username.is_some() || parsed_password.is_some();
                let (effective_username, effective_password) = if has_embedded_auth {
                    (parsed_username, parsed_password)
                } else {
                    (username, password)
                };

                return build_redis_url(
                    scheme,
                    host_str,
                    port,
                    effective_username,
                    effective_password,
                );
            }

            // Fallback for malformed URLs
            let has_port = if let Some(bracket_pos) = host.rfind(']') {
                host[bracket_pos..].contains(':')
            } else {
                host.split(':').count() >= 3
            };
            let base = if has_port {
                host.to_string()
            } else {
                format!("{}:{}", host, self.port)
            };

            if let Ok(parsed) = Url::parse(&base) {
                let parsed_username = (!parsed.username().is_empty()).then_some(parsed.username());
                let parsed_password = parsed.password();
                if let Some(host_str) = parsed.host_str() {
                    let port = parsed.port_or_known_default().unwrap_or(self.port);
                    let has_embedded_auth = parsed_username.is_some() || parsed_password.is_some();
                    let (effective_username, effective_password) = if has_embedded_auth {
                        (parsed_username, parsed_password)
                    } else {
                        (username, password)
                    };
                    return build_redis_url(
                        parsed.scheme(),
                        host_str,
                        port,
                        effective_username,
                        effective_password,
                    );
                }
            }
            return base;
        }

        let (normalized_host, normalized_port) = split_plain_host_and_port(host, self.port);
        let scheme = if use_tls { "rediss" } else { "redis" };
        build_redis_url(
            scheme,
            &normalized_host,
            normalized_port,
            username,
            password,
        )
    }

    pub fn from_seed(seed: &str) -> Option<Self> {
        let trimmed = seed.trim();
        if trimmed.is_empty() {
            return None;
        }

        if trimmed.starts_with("redis://") || trimmed.starts_with("rediss://") {
            let port = Url::parse(trimmed)
                .ok()
                .and_then(|parsed| parsed.port_or_known_default())
                .unwrap_or(6379);
            return Some(Self {
                host: trimmed.to_string(),
                port,
            });
        }

        let (host, port) = split_plain_host_and_port(trimmed, 6379);
        Some(Self { host, port })
    }
}

fn split_plain_host_and_port(raw_host: &str, default_port: u16) -> (String, u16) {
    let host = raw_host.trim();

    // Handle bracketed IPv6: [::1]:6379
    if host.starts_with('[') {
        if let Some(end_bracket) = host.find(']') {
            let host_part = host[1..end_bracket].to_string();
            let remainder = &host[end_bracket + 1..];
            if let Some(port_str) = remainder.strip_prefix(':')
                && let Ok(port) = port_str.parse::<u16>()
            {
                return (host_part, port);
            }
            return (host_part, default_port);
        }
        return (host.to_string(), default_port);
    }

    // Handle hostname/IP with port: host:6379
    if host.matches(':').count() == 1
        && let Some((host_part, port_part)) = host.rsplit_once(':')
        && let Ok(port) = port_part.parse::<u16>()
    {
        return (host_part.to_string(), port);
    }

    (host.to_string(), default_port)
}

fn build_redis_url(
    scheme: &str,
    host: &str,
    port: u16,
    username: Option<&str>,
    password: Option<&str>,
) -> String {
    let mut url = format!("{scheme}://");

    if let Some(user) = username {
        url.push_str(&urlencoding::encode(user));
        if let Some(pass) = password {
            url.push(':');
            url.push_str(&urlencoding::encode(pass));
        }
        url.push('@');
    } else if let Some(pass) = password {
        url.push(':');
        url.push_str(&urlencoding::encode(pass));
        url.push('@');
    }

    if host.contains(':') && !host.starts_with('[') {
        url.push('[');
        url.push_str(host);
        url.push(']');
    } else {
        url.push_str(host);
    }
    url.push(':');
    url.push_str(&port.to_string());
    url
}

impl Default for RedisConnection {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 6379,
            db: 0,
            username: None,
            password: None,
            key_prefix: "sockudo:".to_string(),
            sentinels: Vec::new(),
            sentinel_password: None,
            name: "mymaster".to_string(),
            cluster: RedisClusterConnection::default(),
            cluster_nodes: Vec::new(),
        }
    }
}

impl Default for RedisSentinel {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 26379,
        }
    }
}

impl Default for ClusterNode {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 7000,
        }
    }
}

#[cfg(test)]
mod redis_connection_tests {
    use super::{ClusterNode, RedisClusterConnection, RedisConnection, RedisSentinel};

    #[test]
    fn test_standard_url_basic() {
        let conn = RedisConnection {
            host: "127.0.0.1".to_string(),
            port: 6379,
            db: 0,
            username: None,
            password: None,
            key_prefix: "sockudo:".to_string(),
            sentinels: Vec::new(),
            sentinel_password: None,
            name: "mymaster".to_string(),
            cluster: RedisClusterConnection::default(),
            cluster_nodes: Vec::new(),
        };
        assert_eq!(conn.to_url(), "redis://127.0.0.1:6379/0");
    }

    #[test]
    fn test_standard_url_with_password() {
        let conn = RedisConnection {
            host: "127.0.0.1".to_string(),
            port: 6379,
            db: 2,
            username: None,
            password: Some("secret".to_string()),
            key_prefix: "sockudo:".to_string(),
            sentinels: Vec::new(),
            sentinel_password: None,
            name: "mymaster".to_string(),
            cluster: RedisClusterConnection::default(),
            cluster_nodes: Vec::new(),
        };
        assert_eq!(conn.to_url(), "redis://:secret@127.0.0.1:6379/2");
    }

    #[test]
    fn test_standard_url_with_username_and_password() {
        let conn = RedisConnection {
            host: "redis.example.com".to_string(),
            port: 6380,
            db: 1,
            username: Some("admin".to_string()),
            password: Some("pass123".to_string()),
            key_prefix: "sockudo:".to_string(),
            sentinels: Vec::new(),
            sentinel_password: None,
            name: "mymaster".to_string(),
            cluster: RedisClusterConnection::default(),
            cluster_nodes: Vec::new(),
        };
        assert_eq!(
            conn.to_url(),
            "redis://admin:pass123@redis.example.com:6380/1"
        );
    }

    #[test]
    fn test_standard_url_with_special_chars_in_password() {
        let conn = RedisConnection {
            host: "127.0.0.1".to_string(),
            port: 6379,
            db: 0,
            username: None,
            password: Some("pass@word#123".to_string()),
            key_prefix: "sockudo:".to_string(),
            sentinels: Vec::new(),
            sentinel_password: None,
            name: "mymaster".to_string(),
            cluster: RedisClusterConnection::default(),
            cluster_nodes: Vec::new(),
        };
        assert_eq!(conn.to_url(), "redis://:pass%40word%23123@127.0.0.1:6379/0");
    }

    #[test]
    fn test_is_sentinel_configured_false() {
        let conn = RedisConnection::default();
        assert!(!conn.is_sentinel_configured());
    }

    #[test]
    fn test_is_sentinel_configured_true() {
        let conn = RedisConnection {
            sentinels: vec![RedisSentinel {
                host: "sentinel1".to_string(),
                port: 26379,
            }],
            ..Default::default()
        };
        assert!(conn.is_sentinel_configured());
    }

    #[test]
    fn test_sentinel_url_basic() {
        let conn = RedisConnection {
            host: "127.0.0.1".to_string(),
            port: 6379,
            db: 0,
            username: None,
            password: None,
            key_prefix: "sockudo:".to_string(),
            sentinels: vec![
                RedisSentinel {
                    host: "sentinel1".to_string(),
                    port: 26379,
                },
                RedisSentinel {
                    host: "sentinel2".to_string(),
                    port: 26379,
                },
            ],
            sentinel_password: None,
            name: "mymaster".to_string(),
            cluster: RedisClusterConnection::default(),
            cluster_nodes: Vec::new(),
        };
        assert_eq!(
            conn.to_url(),
            "redis+sentinel://sentinel1:26379,sentinel2:26379/mymaster/0"
        );
    }

    #[test]
    fn test_sentinel_url_with_sentinel_password() {
        let conn = RedisConnection {
            host: "127.0.0.1".to_string(),
            port: 6379,
            db: 0,
            username: None,
            password: None,
            key_prefix: "sockudo:".to_string(),
            sentinels: vec![RedisSentinel {
                host: "sentinel1".to_string(),
                port: 26379,
            }],
            sentinel_password: Some("sentinelpass".to_string()),
            name: "mymaster".to_string(),
            cluster: RedisClusterConnection::default(),
            cluster_nodes: Vec::new(),
        };
        assert_eq!(
            conn.to_url(),
            "redis+sentinel://:sentinelpass@sentinel1:26379/mymaster/0"
        );
    }

    #[test]
    fn test_sentinel_url_with_master_password() {
        let conn = RedisConnection {
            host: "127.0.0.1".to_string(),
            port: 6379,
            db: 1,
            username: None,
            password: Some("masterpass".to_string()),
            key_prefix: "sockudo:".to_string(),
            sentinels: vec![RedisSentinel {
                host: "sentinel1".to_string(),
                port: 26379,
            }],
            sentinel_password: None,
            name: "mymaster".to_string(),
            cluster: RedisClusterConnection::default(),
            cluster_nodes: Vec::new(),
        };
        assert_eq!(
            conn.to_url(),
            "redis+sentinel://sentinel1:26379/mymaster/1?password=masterpass"
        );
    }

    #[test]
    fn test_sentinel_url_with_all_auth() {
        let conn = RedisConnection {
            host: "127.0.0.1".to_string(),
            port: 6379,
            db: 2,
            username: Some("redisuser".to_string()),
            password: Some("redispass".to_string()),
            key_prefix: "sockudo:".to_string(),
            sentinels: vec![
                RedisSentinel {
                    host: "sentinel1".to_string(),
                    port: 26379,
                },
                RedisSentinel {
                    host: "sentinel2".to_string(),
                    port: 26380,
                },
            ],
            sentinel_password: Some("sentinelauth".to_string()),
            name: "production-master".to_string(),
            cluster: RedisClusterConnection::default(),
            cluster_nodes: Vec::new(),
        };
        assert_eq!(
            conn.to_url(),
            "redis+sentinel://:sentinelauth@sentinel1:26379,sentinel2:26380/production-master/2?password=redispass&username=redisuser"
        );
    }

    #[test]
    fn test_sentinel_to_host_port() {
        let sentinel = RedisSentinel {
            host: "sentinel.example.com".to_string(),
            port: 26379,
        };
        assert_eq!(sentinel.to_host_port(), "sentinel.example.com:26379");
    }

    #[test]
    fn test_cluster_node_urls_with_shared_cluster_auth_and_tls() {
        let conn = RedisConnection {
            cluster: RedisClusterConnection {
                nodes: vec![
                    ClusterNode {
                        host: "node1.secure-cluster.com".to_string(),
                        port: 7000,
                    },
                    ClusterNode {
                        host: "redis://node2.secure-cluster.com:7001".to_string(),
                        port: 7001,
                    },
                    ClusterNode {
                        host: "rediss://node3.secure-cluster.com".to_string(),
                        port: 7002,
                    },
                ],
                username: None,
                password: Some("cluster-secret".to_string()),
                use_tls: true,
            },
            ..Default::default()
        };

        assert_eq!(
            conn.cluster_node_urls(),
            vec![
                "rediss://:cluster-secret@node1.secure-cluster.com:7000",
                "redis://:cluster-secret@node2.secure-cluster.com:7001",
                "rediss://:cluster-secret@node3.secure-cluster.com:7002",
            ]
        );
    }

    #[test]
    fn test_cluster_node_urls_fallback_to_legacy_nodes() {
        let conn = RedisConnection {
            password: Some("fallback-secret".to_string()),
            cluster_nodes: vec![ClusterNode {
                host: "legacy-node.example.com".to_string(),
                port: 7000,
            }],
            ..Default::default()
        };

        assert_eq!(
            conn.cluster_node_urls(),
            vec!["redis://:fallback-secret@legacy-node.example.com:7000"]
        );
    }

    #[test]
    fn test_normalize_cluster_seed_urls() {
        let conn = RedisConnection {
            cluster: RedisClusterConnection {
                nodes: Vec::new(),
                username: Some("svc-user".to_string()),
                password: Some("svc-pass".to_string()),
                use_tls: true,
            },
            ..Default::default()
        };

        let seeds = vec![
            "node1.example.com:7000".to_string(),
            "redis://node2.example.com:7001".to_string(),
            "rediss://node3.example.com".to_string(),
        ];

        assert_eq!(
            conn.normalize_cluster_seed_urls(&seeds),
            vec![
                "rediss://svc-user:svc-pass@node1.example.com:7000",
                "redis://svc-user:svc-pass@node2.example.com:7001",
                "rediss://svc-user:svc-pass@node3.example.com:6379",
            ]
        );
    }
}

#[cfg(test)]
mod cluster_node_tests {
    use super::ClusterNode;

    #[test]
    fn test_to_url_basic_host() {
        let node = ClusterNode {
            host: "localhost".to_string(),
            port: 6379,
        };
        assert_eq!(node.to_url(), "redis://localhost:6379");
    }

    #[test]
    fn test_to_url_ip_address() {
        let node = ClusterNode {
            host: "127.0.0.1".to_string(),
            port: 6379,
        };
        assert_eq!(node.to_url(), "redis://127.0.0.1:6379");
    }

    #[test]
    fn test_to_url_with_redis_protocol() {
        let node = ClusterNode {
            host: "redis://example.com".to_string(),
            port: 6379,
        };
        assert_eq!(node.to_url(), "redis://example.com:6379");
    }

    #[test]
    fn test_to_url_with_rediss_protocol() {
        let node = ClusterNode {
            host: "rediss://secure.example.com".to_string(),
            port: 6379,
        };
        assert_eq!(node.to_url(), "rediss://secure.example.com:6379");
    }

    #[test]
    fn test_to_url_with_rediss_protocol_and_port_in_url() {
        let node = ClusterNode {
            host: "rediss://secure.example.com:7000".to_string(),
            port: 6379,
        };
        assert_eq!(node.to_url(), "rediss://secure.example.com:7000");
    }

    #[test]
    fn test_to_url_with_redis_protocol_and_port_in_url() {
        let node = ClusterNode {
            host: "redis://example.com:7001".to_string(),
            port: 6379,
        };
        assert_eq!(node.to_url(), "redis://example.com:7001");
    }

    #[test]
    fn test_to_url_with_trailing_whitespace() {
        let node = ClusterNode {
            host: "  rediss://secure.example.com  ".to_string(),
            port: 6379,
        };
        assert_eq!(node.to_url(), "rediss://secure.example.com:6379");
    }

    #[test]
    fn test_to_url_custom_port() {
        let node = ClusterNode {
            host: "redis-cluster.example.com".to_string(),
            port: 7000,
        };
        assert_eq!(node.to_url(), "redis://redis-cluster.example.com:7000");
    }

    #[test]
    fn test_to_url_plain_host_with_port_in_host_field() {
        let node = ClusterNode {
            host: "redis-cluster.example.com:7010".to_string(),
            port: 7000,
        };
        assert_eq!(node.to_url(), "redis://redis-cluster.example.com:7010");
    }

    #[test]
    fn test_to_url_with_options_adds_auth_and_tls() {
        let node = ClusterNode {
            host: "node.example.com".to_string(),
            port: 7000,
        };
        assert_eq!(
            node.to_url_with_options(true, Some("svc-user"), Some("secret")),
            "rediss://svc-user:secret@node.example.com:7000"
        );
    }

    #[test]
    fn test_to_url_with_options_keeps_embedded_auth() {
        let node = ClusterNode {
            host: "rediss://:node-secret@node.example.com:7000".to_string(),
            port: 7000,
        };
        assert_eq!(
            node.to_url_with_options(true, Some("global-user"), Some("global-secret")),
            "rediss://:node-secret@node.example.com:7000"
        );
    }

    #[test]
    fn test_from_seed_parses_plain_host_port() {
        let node = ClusterNode::from_seed("cluster-node-1:7005").expect("node should parse");
        assert_eq!(node.host, "cluster-node-1");
        assert_eq!(node.port, 7005);
    }

    #[test]
    fn test_from_seed_keeps_scheme_urls() {
        let node =
            ClusterNode::from_seed("rediss://secure.example.com:7005").expect("node should parse");
        assert_eq!(node.host, "rediss://secure.example.com:7005");
        assert_eq!(node.port, 7005);
    }

    #[test]
    fn test_to_url_aws_elasticache_hostname() {
        let node = ClusterNode {
            host: "rediss://my-cluster.use1.cache.amazonaws.com".to_string(),
            port: 6379,
        };
        assert_eq!(
            node.to_url(),
            "rediss://my-cluster.use1.cache.amazonaws.com:6379"
        );
    }

    #[test]
    fn test_to_url_with_ipv6_no_port() {
        let node = ClusterNode {
            host: "rediss://[::1]".to_string(),
            port: 6379,
        };
        assert_eq!(node.to_url(), "rediss://[::1]:6379");
    }

    #[test]
    fn test_to_url_with_ipv6_and_port_in_url() {
        let node = ClusterNode {
            host: "rediss://[::1]:7000".to_string(),
            port: 6379,
        };
        assert_eq!(node.to_url(), "rediss://[::1]:7000");
    }

    #[test]
    fn test_to_url_with_ipv6_full_address_no_port() {
        let node = ClusterNode {
            host: "rediss://[2001:db8::1]".to_string(),
            port: 6379,
        };
        assert_eq!(node.to_url(), "rediss://[2001:db8::1]:6379");
    }

    #[test]
    fn test_to_url_with_ipv6_full_address_with_port() {
        let node = ClusterNode {
            host: "rediss://[2001:db8::1]:7000".to_string(),
            port: 6379,
        };
        assert_eq!(node.to_url(), "rediss://[2001:db8::1]:7000");
    }

    #[test]
    fn test_to_url_with_redis_protocol_ipv6() {
        let node = ClusterNode {
            host: "redis://[::1]".to_string(),
            port: 6379,
        };
        assert_eq!(node.to_url(), "redis://[::1]:6379");
    }
}
