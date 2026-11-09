//! Test harness: a scriptable Pusher/Sockudo WebSocket server and helpers.
#![allow(dead_code, clippy::unwrap_used)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use parking_lot::Mutex;
use sockudo_ws::{Http1, Message};
use sonic_rs::{JsonValueTrait, Value};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

pub const WAIT: Duration = Duration::from_secs(5);

/// A server that hands every accepted WebSocket to the test.
pub struct MockServer {
    pub addr: SocketAddr,
    connections: mpsc::UnboundedReceiver<MockConn>,
}

impl MockServer {
    pub async fn start() -> Self {
        Self::start_with(false).await
    }

    /// A server that negotiates permessage-deflate when the client offers it.
    pub async fn start_with_deflate() -> Self {
        Self::start_with(true).await
    }

    async fn start_with(deflate: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (sender, connections) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                let sender = sender.clone();
                tokio::spawn(async move {
                    let Some((path, extensions, socket)) = accept(tcp, deflate).await else {
                        return;
                    };
                    let (out_tx, out_rx) = mpsc::unbounded_channel();
                    let (in_tx, in_rx) = mpsc::unbounded_channel();
                    let closed = Arc::new(Mutex::new(None));
                    match socket {
                        Socket::Plain(ws) => {
                            tokio::spawn(pump(ws, out_rx, in_tx, Arc::clone(&closed)));
                        }
                        Socket::Deflate(ws) => {
                            tokio::spawn(pump(ws, out_rx, in_tx, Arc::clone(&closed)));
                        }
                    }
                    let _ = sender.send(MockConn {
                        path,
                        extensions,
                        out: out_tx,
                        inbound: in_rx,
                        closed,
                    });
                });
            }
        });
        Self { addr, connections }
    }

    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    /// Wait for the next client connection.
    pub async fn accept(&mut self) -> MockConn {
        tokio::time::timeout(WAIT, self.connections.recv())
            .await
            .expect("timed out waiting for a client connection")
            .expect("server stopped")
    }

    /// Whether a connection arrives within `wait`.
    pub async fn try_accept(&mut self, wait: Duration) -> Option<MockConn> {
        tokio::time::timeout(wait, self.connections.recv())
            .await
            .ok()
            .flatten()
    }
}

enum Socket {
    Plain(sockudo_ws::WebSocketStream<sockudo_ws::Stream<Http1>>),
    Deflate(sockudo_ws::CompressedWebSocketStream<sockudo_ws::Stream<Http1>>),
}

fn ws_config() -> sockudo_ws::Config {
    sockudo_ws::Config {
        auto_ping: false,
        idle_timeout: 0,
        ..sockudo_ws::Config::default()
    }
}

/// Server side of the upgrade, negotiating permessage-deflate when enabled.
async fn accept(
    mut tcp: tokio::net::TcpStream,
    deflate: bool,
) -> Option<(String, Option<String>, Socket)> {
    use sockudo_ws::handshake;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut buf = bytes::BytesMut::with_capacity(4096);
    let (path, key, offers, consumed) = loop {
        if tcp.read_buf(&mut buf).await.ok()? == 0 {
            return None;
        }
        if let Some((request, consumed)) = handshake::parse_request(&buf).ok()? {
            break (
                request.path.to_string(),
                request.key.to_owned(),
                request.extensions.map(str::to_owned),
                consumed,
            );
        }
    };
    let negotiation = offers.as_deref().filter(|_| deflate).and_then(|offers| {
        sockudo_ws::deflate::negotiate_server_deflate(offers, &sockudo_ws::DeflateConfig::default())
    });
    let response_extensions = negotiation.as_ref().map(|n| n.to_response_header());
    let response = handshake::build_response(
        &handshake::generate_accept_key(&key),
        None,
        response_extensions.as_deref(),
    );
    tcp.write_all(&response).await.ok()?;
    let leftover = (consumed < buf.len()).then(|| buf.split_off(consumed).freeze());
    let inner = sockudo_ws::Stream::<Http1>::new(tcp);
    let socket = match negotiation {
        Some(negotiation) => {
            Socket::Deflate(sockudo_ws::CompressedWebSocketStream::server_with_leftover(
                inner,
                ws_config(),
                negotiation.config,
                leftover,
            ))
        }
        None => Socket::Plain(sockudo_ws::WebSocketStream::from_raw_with_leftover(
            inner,
            sockudo_ws::Role::Server,
            ws_config(),
            leftover,
        )),
    };
    Some((path, response_extensions, socket))
}

async fn pump<W>(
    mut ws: W,
    mut out: mpsc::UnboundedReceiver<Message>,
    inbound: mpsc::UnboundedSender<Message>,
    closed: Arc<Mutex<Option<u16>>>,
) where
    W: futures_util::Stream<Item = Result<Message, sockudo_ws::Error>>
        + futures_util::Sink<Message, Error = sockudo_ws::Error>
        + Unpin,
{
    loop {
        tokio::select! {
            frame = ws.next() => match frame {
                Some(Ok(Message::Close(reason))) => {
                    *closed.lock() = Some(reason.map_or(1005, |r| r.code));
                    let _ = inbound.send(Message::Close(None));
                    break;
                }
                Some(Ok(message)) => {
                    if inbound.send(message).is_err() {
                        break;
                    }
                }
                _ => {
                    *closed.lock() = Some(1006);
                    let _ = inbound.send(Message::Close(None));
                    break;
                }
            },
            message = out.recv() => match message {
                Some(Message::Close(Some(reason))) => {
                    let _ = ws.send(Message::Close(Some(reason))).await;
                    // Drain until the client acknowledges.
                    let _ = tokio::time::timeout(Duration::from_secs(1), async {
                        while let Some(Ok(_)) = ws.next().await {}
                    })
                    .await;
                    break;
                }
                Some(message) => {
                    if ws.send(message).await.is_err() {
                        break;
                    }
                }
                // MockConn dropped: drop the socket without a close handshake.
                None => break,
            },
        }
    }
}

/// One accepted client connection.
pub struct MockConn {
    pub path: String,
    /// The `Sec-WebSocket-Extensions` the server accepted.
    pub extensions: Option<String>,
    out: mpsc::UnboundedSender<Message>,
    inbound: mpsc::UnboundedReceiver<Message>,
    closed: Arc<Mutex<Option<u16>>>,
}

impl MockConn {
    pub fn query(&self, name: &str) -> Option<String> {
        let query = self.path.split_once('?')?.1;
        form_urlencoded::parse(query.as_bytes())
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.into_owned())
    }

    pub fn is_v2(&self) -> bool {
        self.query("protocol").as_deref() == Some("2")
    }

    pub fn event_name(&self, name: &str) -> String {
        if self.is_v2() {
            format!("sockudo:{name}")
        } else {
            format!("pusher:{name}")
        }
    }

    pub fn internal_name(&self, name: &str) -> String {
        if self.is_v2() {
            format!("sockudo_internal:{name}")
        } else {
            format!("pusher_internal:{name}")
        }
    }

    pub fn send_text(&self, text: impl Into<String>) {
        let _ = self.out.send(Message::Text(Bytes::from(text.into())));
    }

    pub fn send_binary(&self, bytes: Vec<u8>) {
        let _ = self.out.send(Message::Binary(Bytes::from(bytes)));
    }

    pub fn send_json(&self, value: &Value) {
        self.send_text(sonic_rs::to_string(value).unwrap());
    }

    /// Send `{"event", "channel"?, "data": "<json string>"}` like a Pusher server.
    pub fn send_event(&self, event: &str, channel: Option<&str>, data: &Value) {
        let mut frame = sonic_rs::json!({ "event": event, "data": data.to_string() });
        if let Some(channel) = channel {
            frame["channel"] = Value::from(channel);
        }
        self.send_json(&frame);
    }

    pub fn establish(&self, socket_id: &str) {
        self.establish_with_timeout(socket_id, 120);
    }

    pub fn establish_with_timeout(&self, socket_id: &str, activity_timeout: u64) {
        let name = self.event_name("connection_established");
        self.send_event(
            &name,
            None,
            &sonic_rs::json!({ "socket_id": socket_id, "activity_timeout": activity_timeout }),
        );
    }

    pub fn subscription_succeeded(&self, channel: &str, data: &Value) {
        let name = self.internal_name("subscription_succeeded");
        self.send_event(&name, Some(channel), data);
    }

    pub fn close(&self, code: u16, reason: &str) {
        let _ = self
            .out
            .send(Message::Close(Some(sockudo_ws::error::CloseReason::new(
                code, reason,
            ))));
    }

    /// Next frame from the client.
    pub async fn recv_frame(&mut self) -> Message {
        tokio::time::timeout(WAIT, self.inbound.recv())
            .await
            .expect("timed out waiting for a client frame")
            .expect("connection closed")
    }

    pub async fn try_recv_frame(&mut self, wait: Duration) -> Option<Message> {
        tokio::time::timeout(wait, self.inbound.recv())
            .await
            .ok()
            .flatten()
    }

    /// Next JSON text frame (skipping native ping/pong).
    pub async fn recv_json(&mut self) -> Value {
        loop {
            match self.recv_frame().await {
                Message::Text(bytes) => return sonic_rs::from_slice(&bytes).unwrap(),
                Message::Ping(_) | Message::Pong(_) => continue,
                other => panic!("expected a text frame, got {other:?}"),
            }
        }
    }

    /// Skip frames until one with `event == name` arrives; return it.
    pub async fn expect_event(&mut self, name: &str) -> Value {
        let deadline = tokio::time::Instant::now() + WAIT;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let frame = tokio::time::timeout(remaining, self.recv_json())
                .await
                .unwrap_or_else(|_| panic!("timed out waiting for '{name}'"));
            if frame.get("event").and_then(|e| e.as_str()) == Some(name) {
                return frame;
            }
        }
    }

    /// Collect JSON frames for `wait`.
    pub async fn drain_json(&mut self, wait: Duration) -> Vec<Value> {
        let mut frames = Vec::new();
        let deadline = tokio::time::Instant::now() + wait;
        while let Some(frame) = self
            .try_recv_frame(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
        {
            if let Message::Text(bytes) = frame {
                frames.push(sonic_rs::from_slice(&bytes).unwrap());
            }
        }
        frames
    }

    /// Wait for the client to close or drop the connection.
    pub async fn wait_closed(&mut self) -> u16 {
        loop {
            if let Some(code) = *self.closed.lock() {
                return code;
            }
            if let Message::Close(_) = self.recv_frame().await {
                return self.closed.lock().unwrap_or(1005);
            }
        }
    }
}

/// `frame.data`, decoding a JSON-string payload.
pub fn data_of(frame: &Value) -> Value {
    match frame.get("data") {
        Some(data) if data.is_str() => sonic_rs::from_str(data.as_str().unwrap()).unwrap(),
        Some(data) => data.clone(),
        None => Value::new_null(),
    }
}

/// Poll until `check` holds.
pub async fn eventually(mut check: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + WAIT;
    while !check() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not met in time"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Collects values pushed from callbacks.
#[derive(Clone)]
pub struct Recorder<T>(pub Arc<Mutex<Vec<T>>>);

impl<T> Default for Recorder<T> {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }
}

impl<T: Clone> Recorder<T> {
    pub fn push(&self, value: T) {
        self.0.lock().push(value);
    }
    pub fn take(&self) -> Vec<T> {
        std::mem::take(&mut *self.0.lock())
    }
    pub fn snapshot(&self) -> Vec<T> {
        self.0.lock().clone()
    }
    pub fn len(&self) -> usize {
        self.0.lock().len()
    }
}

/// Builder for a client pointed at the mock server.
pub fn client_for(server: &MockServer) -> sockudo_client::ClientBuilder {
    sockudo_client::SockudoClient::builder("app-key")
        .ws_host("127.0.0.1")
        .ws_port(server.port())
        .force_tls(false)
        .disabled_transports([sockudo_client::Transport::Wss])
}

/// Serve an axum router on an ephemeral port; returns its base URL.
pub async fn serve_http(router: axum::Router) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    format!("http://{addr}")
}
