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
pub use reference::{BufferStats, WebSocketExt, WebSocketRef};
pub use sender::MessageSender;
pub use socket_id::SocketId;
pub use state::{ConnectionState, ConnectionStatus, ConnectionTimeouts};

#[cfg(test)]
mod tests;
