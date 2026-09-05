// Probe-only direct Redis construction. CacheManager operations are copied unchanged
// from the production Redis driver; no Sentinel/TLS or reconnect behavior is measured.
pub mod options {
    #[derive(Clone, Debug, Default)] pub struct RedisTlsOptions;
    #[derive(Clone, Debug)] pub struct SentinelSpec;
}
pub mod redis_client {
    use super::options::*;
    use redis::IntoConnectionInfo;
    pub struct RedisClient(redis::Client);
    pub struct RedisClientOptions { pub sentinel: Option<SentinelSpec>, pub tls: RedisTlsOptions, pub response_timeout: Option<std::time::Duration> }
    impl RedisClient {
        pub async fn connect_with_options(url: &str, _: RedisClientOptions) -> Result<Self,redis::RedisError> {Ok(Self(redis::Client::open(url.into_connection_info()?.set_tcp_settings(redis::io::tcp::TcpSettings::default().set_nodelay(true)))?))}
        pub async fn command_connection(&self) -> Result<redis::aio::ConnectionManager,redis::RedisError> {self.0.get_connection_manager().await}
        pub async fn events_connection(&self) -> Result<redis::aio::ConnectionManager,redis::RedisError> {self.0.get_connection_manager().await}
    }
}
