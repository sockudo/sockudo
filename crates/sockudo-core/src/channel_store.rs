use ahash::AHashMap;
use async_trait::async_trait;
use sockudo_types::presence::PresenceMemberInfo;
use sockudo_types::socket::SocketId;

#[async_trait]
pub trait ChannelStore: Send + Sync {
    type Error: Send + Sync + 'static;

    async fn is_in_channel(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<bool, Self::Error>;

    async fn get_channel_socket_count(&self, app_id: &str, channel: &str) -> usize;

    async fn add_to_channel(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<bool, Self::Error>;

    async fn get_channel_members(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<AHashMap<String, PresenceMemberInfo>, Self::Error>;

    async fn get_channel_sockets(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<Vec<SocketId>, Self::Error>;

    async fn remove_channel(&self, app_id: &str, channel: &str);

    async fn remove_from_channel(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<bool, Self::Error>;
}
