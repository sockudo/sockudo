#![allow(async_fn_in_trait)]

mod buffer;
mod capabilities;
mod connection;
mod reference;
mod sender;
mod socket_id;
mod state;

pub use buffer::{BufferLimit, BufferedRewindMessage, ByteCounter, WebSocketBufferConfig};
pub use capabilities::{ConnectionCapabilities, UserInfo};
pub use connection::WebSocket;
pub use reference::{BufferStats, PerChannelState, WebSocketExt, WebSocketRef};
pub use sender::MessageSender;
pub use socket_id::SocketId;
pub use state::{ConnectionState, ConnectionStatus, ConnectionTimeouts, DisconnectCause};

use sockudo_ws::axum_integration::WebSocketReader;
use std::time::Duration;

/// Shard count for maps that exist once per socket and hold a few entries (per-channel state,
/// rewind gates, rate-limit keys). `DashMap::new()` creates `4 * available_parallelism()`
/// cache-padded 128-byte shards, which is 16 KiB per map on a 32-thread host.
pub const PER_SOCKET_MAP_SHARDS: usize = 4;

/// Outer guard for [`finish_peer_close`]. sockudo-ws bounds the Close reply
/// with its own `close_timeout` (5 s by default); this only protects cleanup
/// from a driver that never reports terminal state.
const PEER_CLOSE_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Keep a split reader alive after it yielded a peer Close until sockudo-ws
/// has written the Close reply and shut the transport down.
///
/// The reply is written by the connection driver, and dropping either split
/// half cancels that driver. Cleaning up right after reading Close can reset
/// the TCP connection before the reply leaves, so clients observe 1006
/// instead of a completed close handshake.
pub async fn finish_peer_close(reader: &mut WebSocketReader) {
    let drain = async { while reader.next().await.is_some() {} };
    let _ = tokio::time::timeout(PEER_CLOSE_DRAIN_TIMEOUT, drain).await;
}

#[cfg(test)]
mod tests;
