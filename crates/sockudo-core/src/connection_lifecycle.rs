use crate::channel_store::ChannelStore;
use async_trait::async_trait;
use sockudo_types::socket::SocketId;

#[async_trait]
pub trait ConnectionLifecycleStore: ChannelStore + Send + Sync {
    type Message: Send + Sync + 'static;

    async fn remove_connection(
        &self,
        socket_id: &SocketId,
        app_id: &str,
    ) -> Result<(), Self::Error>;

    async fn remove_user_socket(
        &self,
        user_id: &str,
        socket_id: &SocketId,
        app_id: &str,
    ) -> Result<(), Self::Error>;

    async fn count_user_connections_in_channel(
        &self,
        user_id: &str,
        app_id: &str,
        channel: &str,
        excluding_socket: Option<&SocketId>,
    ) -> Result<usize, Self::Error>;

    async fn send(
        &self,
        channel: &str,
        message: Self::Message,
        except: Option<&SocketId>,
        app_id: &str,
        start_time_ms: Option<f64>,
    ) -> Result<(), Self::Error>;

    async fn broadcast_presence_join(
        &self,
        app_id: &str,
        channel: &str,
        user_id: &str,
        socket_id: &str,
        user_info: Option<sonic_rs::Value>,
    ) -> Result<(), Self::Error> {
        let _ = (app_id, channel, user_id, socket_id, user_info);
        Ok(())
    }

    async fn broadcast_presence_leave(
        &self,
        app_id: &str,
        channel: &str,
        user_id: &str,
        socket_id: &str,
    ) -> Result<(), Self::Error> {
        let _ = (app_id, channel, user_id, socket_id);
        Ok(())
    }
}
