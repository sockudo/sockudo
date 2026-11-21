//! Real client/server WebSocket pairs for fanout tests.
//!
//! The fanout paths must be exercised against actual frames, not mocks, so the
//! tests here pay the cost of a loopback handshake per socket.

use futures_util::StreamExt;
use sockudo_core::websocket::{SocketId, WebSocket, WebSocketBufferConfig, WebSocketRef};
use sockudo_protocol::{AppendMode, ProtocolVersion, WireFormat};
use sockudo_ws::client::WebSocketClient;
use sockudo_ws::{Config as WsConfig, Http1, Stream as WsStream, WebSocketStream};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};

pub(crate) type ClientWs = WebSocketStream<WsStream<Http1>>;

pub(crate) async fn create_ws_pair() -> (sockudo_ws::axum_integration::WebSocketWriter, ClientWs) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        sockudo_ws::handshake::server_handshake(&mut stream)
            .await
            .unwrap();
        let ws = sockudo_ws::axum_integration::WebSocket::from_tcp(stream, WsConfig::default());
        let (mut reader, writer) = ws.split();
        tokio::spawn(async move { while reader.next().await.is_some() {} });
        writer
    });

    let client_stream = TcpStream::connect(addr).await.unwrap();
    let client = WebSocketClient::<Http1>::new(WsConfig::default());
    let (client_ws, _) = client
        .connect(client_stream, &addr.to_string(), "/", None)
        .await
        .unwrap();

    (server_task.await.unwrap(), client_ws)
}

pub(crate) fn socket_ref(
    writer: sockudo_ws::axum_integration::WebSocketWriter,
    wire_format: WireFormat,
    append_mode: AppendMode,
) -> WebSocketRef {
    let mut ws =
        WebSocket::with_buffer_config(SocketId::new(), writer, WebSocketBufferConfig::default());
    ws.state.protocol_version = ProtocolVersion::V2;
    ws.state.wire_format = wire_format;
    ws.state.append_mode = append_mode;
    WebSocketRef::new(ws)
}

pub(crate) async fn next_frame(client: &mut ClientWs) -> sockudo_ws::Message {
    tokio::time::timeout(Duration::from_secs(2), client.next())
        .await
        .expect("timed out waiting for frame")
        .expect("stream ended")
        .expect("client read error")
}
