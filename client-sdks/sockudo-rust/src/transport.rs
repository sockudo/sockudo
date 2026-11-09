//! TCP / TLS / WebSocket establishment.

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use sockudo_ws::{Http1, Message, Role, WebSocketStream, handshake};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::client::Shared;
use crate::error::{Error, Result};
use crate::options::{Config, Transport};

type Inner = sockudo_ws::Stream<Http1>;

/// The permessage-deflate offer: default windows, and we accept whatever
/// client window the server asks for.
#[cfg(feature = "deflate")]
const DEFLATE_OFFER: &str = "permessage-deflate; client_max_window_bits";

/// Largest handshake response accepted before giving up.
const MAX_HANDSHAKE_RESPONSE: usize = 16 * 1024;

/// An established WebSocket, compressed when the server accepted
/// permessage-deflate. `Stream<Http1>` erases plain/TLS.
pub(crate) enum WsStream {
    Plain(WebSocketStream<Inner>),
    #[cfg(feature = "deflate")]
    Deflate(sockudo_ws::CompressedWebSocketStream<Inner>),
}

impl WsStream {
    /// Next message. Cancel-safe: dropping the future loses nothing.
    pub(crate) async fn next(&mut self) -> Option<std::result::Result<Message, sockudo_ws::Error>> {
        match self {
            Self::Plain(ws) => ws.next().await,
            #[cfg(feature = "deflate")]
            Self::Deflate(ws) => ws.next().await,
        }
    }

    /// Queue a message without flushing.
    pub(crate) async fn feed(
        &mut self,
        message: Message,
    ) -> std::result::Result<(), sockudo_ws::Error> {
        match self {
            Self::Plain(ws) => ws.feed(message).await,
            #[cfg(feature = "deflate")]
            Self::Deflate(ws) => ws.feed(message).await,
        }
    }

    /// Flush queued messages.
    pub(crate) async fn flush(&mut self) -> std::result::Result<(), sockudo_ws::Error> {
        match self {
            Self::Plain(ws) => SinkExt::flush(ws).await,
            #[cfg(feature = "deflate")]
            Self::Deflate(ws) => SinkExt::flush(ws).await,
        }
    }

    /// Send a Close frame and finish the closing handshake.
    pub(crate) async fn close(
        &mut self,
        code: u16,
        reason: &str,
    ) -> std::result::Result<(), sockudo_ws::Error> {
        match self {
            Self::Plain(ws) => ws.close(code, reason).await,
            #[cfg(feature = "deflate")]
            Self::Deflate(ws) => ws.close(code, reason).await,
        }
    }

    /// Whether permessage-deflate was negotiated.
    pub(crate) fn is_compressed(&self) -> bool {
        match self {
            Self::Plain(_) => false,
            #[cfg(feature = "deflate")]
            Self::Deflate(_) => true,
        }
    }
}

/// Percent-encode an RFC 3986 path segment (unreserved characters pass).
fn encode_path_segment(input: &str, out: &mut String) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for byte in input.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(byte));
        } else {
            out.push('%');
            out.push(char::from(HEX[usize::from(byte >> 4)]));
            out.push(char::from(HEX[usize::from(byte & 15)]));
        }
    }
}

/// `{ws_path}/app/{key}?protocol=…&client=rust&…`
pub(crate) fn path_and_query(config: &Config, token: Option<&str>) -> String {
    let mut path = String::with_capacity(96);
    path.push_str(&config.ws_path);
    path.push_str("/app/");
    encode_path_segment(&config.key, &mut path);
    let mut query = form_urlencoded::Serializer::new(String::new());
    query.append_pair("protocol", config.protocol.query_value());
    query.append_pair("client", "rust");
    query.append_pair("version", env!("CARGO_PKG_VERSION"));
    query.append_pair("flash", "false");
    if config.protocol.is_v2() {
        query.append_pair("format", config.wire_format.query_value());
        query.append_pair(
            "echo_messages",
            if config.echo_messages {
                "true"
            } else {
                "false"
            },
        );
        query.append_pair("append_mode", config.append_mode.as_str());
        if let Some(window) = config.append_rollup_window {
            query.append_pair("append_rollup_window", &window.to_string());
        }
        if let Some(token) = token {
            query.append_pair("token", token);
        }
    }
    for (name, value) in &config.query_params {
        query.append_pair(name, value);
    }
    path.push('?');
    path.push_str(&query.finish());
    path
}

/// Host as used in URLs and the `Host` header (IPv6 literals bracketed).
fn authority_host(host: &str) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_owned()
    }
}

/// The connection URL without credentials, for diagnostics.
pub(crate) fn display_url(config: &Config, transport: Transport) -> String {
    let (scheme, port) = match transport {
        Transport::Ws => ("ws", config.ws_port),
        Transport::Wss => ("wss", config.wss_port),
    };
    format!(
        "{scheme}://{}:{port}{}/app/{}",
        authority_host(&config.ws_host),
        config.ws_path,
        config.key
    )
}

fn ws_config(config: &Config) -> sockudo_ws::Config {
    sockudo_ws::Config {
        max_message_size: config.max_message_size,
        max_frame_size: config.max_message_size,
        // Liveness is governed by the Pusher-level ping/pong, not native frames.
        auto_ping: false,
        idle_timeout: 0,
        close_timeout: 2,
        write_coalescing: true,
        ..sockudo_ws::Config::default()
    }
}

/// Open a WebSocket to the server.
pub(crate) async fn connect(
    shared: &Shared,
    transport: Transport,
    token: Option<&str>,
) -> Result<WsStream> {
    let config = &shared.config;
    let port = match transport {
        Transport::Ws => config.ws_port,
        Transport::Wss => config.wss_port,
    };
    let host = config.ws_host.trim_start_matches('[').trim_end_matches(']');
    let path = path_and_query(config, token);
    let host_header = format!("{}:{port}", authority_host(host));
    let headers = (!config.ws_headers.is_empty()).then_some(config.ws_headers.as_slice());

    let offer = deflate_offer(config);
    let tcp = TcpStream::connect((host, port)).await?;
    tcp.set_nodelay(true)?;
    match transport {
        Transport::Ws => upgrade(tcp, config, &host_header, &path, headers, offer).await,
        Transport::Wss => {
            let tls = tls_connect(shared, host, tcp).await?;
            upgrade(tls, config, &host_header, &path, headers, offer).await
        }
    }
}

fn deflate_offer(config: &Config) -> Option<&'static str> {
    #[cfg(feature = "deflate")]
    if config.permessage_deflate {
        return Some(DEFLATE_OFFER);
    }
    let _ = config;
    None
}

/// Perform the HTTP/1.1 upgrade and wrap the stream, compressed when the
/// server accepted the deflate offer.
async fn upgrade<S>(
    mut stream: S,
    config: &Config,
    host: &str,
    path: &str,
    headers: Option<&[(String, String)]>,
    offer: Option<&str>,
) -> Result<WsStream>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (extensions, leftover) = client_handshake(&mut stream, host, path, headers, offer).await?;
    let inner = Inner::new(stream);
    let ws_config = ws_config(config);
    match extensions {
        None => Ok(WsStream::Plain(WebSocketStream::from_raw_with_leftover(
            inner,
            Role::Client,
            ws_config,
            leftover,
        ))),
        Some(accepted) => {
            #[cfg(feature = "deflate")]
            {
                let params =
                    sockudo_ws::deflate::parse_deflate_offer(&accepted).ok_or_else(|| {
                        Error::WebSocket(format!(
                            "server accepted an unsupported extension: {accepted}"
                        ))
                    })?;
                let deflate = sockudo_ws::DeflateConfig::from_params(&params)?;
                Ok(WsStream::Deflate(
                    sockudo_ws::CompressedWebSocketStream::client_with_leftover(
                        inner, ws_config, deflate, leftover,
                    ),
                ))
            }
            #[cfg(not(feature = "deflate"))]
            {
                drop((inner, ws_config, leftover));
                Err(Error::WebSocket(format!(
                    "server selected an extension that was not offered: {accepted}"
                )))
            }
        }
    }
}

/// Client side of the upgrade. Returns the accepted extensions and any
/// frame bytes read together with the response.
async fn client_handshake<S>(
    stream: &mut S,
    host: &str,
    path: &str,
    headers: Option<&[(String, String)]>,
    offer: Option<&str>,
) -> Result<(Option<String>, Option<Bytes>)>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let key = handshake::generate_key();
    let request = handshake::build_request_with_headers(host, path, &key, None, offer, headers)?;
    stream.write_all(&request).await?;
    stream.flush().await?;
    let mut buf = BytesMut::with_capacity(1024);
    loop {
        if stream.read_buf(&mut buf).await? == 0 {
            return Err(Error::WebSocket(
                "connection closed during the handshake".into(),
            ));
        }
        if let Some((response, consumed)) = handshake::parse_response(&buf)? {
            let accept = response
                .accept
                .ok_or_else(|| Error::WebSocket("missing Sec-WebSocket-Accept".into()))?;
            if !handshake::validate_accept_key(&key, accept) {
                return Err(Error::WebSocket("invalid Sec-WebSocket-Accept".into()));
            }
            if response.protocol.is_some() {
                return Err(Error::WebSocket(
                    "server selected an unrequested subprotocol".into(),
                ));
            }
            let extensions = response
                .extensions
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned);
            if extensions.is_some() && offer.is_none() {
                return Err(Error::WebSocket(
                    "server selected an extension that was not offered".into(),
                ));
            }
            let leftover = (consumed < buf.len()).then(|| buf.split_off(consumed).freeze());
            return Ok((extensions, leftover));
        }
        if buf.len() > MAX_HANDSHAKE_RESPONSE {
            return Err(Error::WebSocket("handshake response too large".into()));
        }
    }
}

#[cfg(feature = "tls")]
async fn tls_connect(
    shared: &Shared,
    host: &str,
    tcp: TcpStream,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let connector = tokio_rustls::TlsConnector::from(shared.tls_config()?);
    let server_name = rustls::pki_types::ServerName::try_from(host.to_owned())
        .map_err(|error| Error::Tls(format!("invalid server name '{host}': {error}")))?;
    connector
        .connect(server_name, tcp)
        .await
        .map_err(|error| Error::Tls(error.to_string()))
}

#[cfg(not(feature = "tls"))]
async fn tls_connect(_shared: &Shared, _host: &str, _tcp: TcpStream) -> Result<TcpStream> {
    Err(Error::unsupported(
        "wss:// requires the `tls` feature (or set force_tls(false) and use ws://)",
    ))
}

/// Default rustls configuration: aws-lc-rs, safe protocol versions, the
/// operating-system verifier (or bundled webpki roots), ALPN `http/1.1`.
#[cfg(feature = "tls")]
pub(crate) fn default_tls_config() -> Result<std::sync::Arc<rustls::ClientConfig>> {
    let tls_error = |error: rustls::Error| Error::Tls(error.to_string());
    let provider = std::sync::Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(tls_error)?;
    #[cfg(feature = "tls-webpki-roots")]
    let mut config = {
        let roots = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };
        builder.with_root_certificates(roots).with_no_client_auth()
    };
    #[cfg(not(feature = "tls-webpki-roots"))]
    let mut config = {
        use rustls_platform_verifier::BuilderVerifierExt;
        builder
            .with_platform_verifier()
            .map_err(tls_error)?
            .with_no_client_auth()
    };
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(std::sync::Arc::new(config))
}

/// Exponential-ish reconnect delay: `attempts²` seconds, capped, with jitter.
pub(crate) fn reconnect_delay(attempts: u32, max_gap: Duration, jitter: f64) -> Duration {
    let base = Duration::from_secs(u64::from(attempts).saturating_mul(u64::from(attempts)));
    let delay = base.min(max_gap);
    if jitter <= 0.0 || delay.is_zero() {
        return delay;
    }
    let cut = delay.as_secs_f64() * jitter * fastrand::f64();
    Duration::from_secs_f64((delay.as_secs_f64() - cut).max(0.0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::ClientBuilder;
    use crate::protocol::ProtocolVersion;

    fn config(builder: ClientBuilder) -> Config {
        builder.into_config().unwrap()
    }

    #[test]
    fn builds_v2_query() {
        let config = config(
            ClientBuilder::new("app key")
                .ws_host("h")
                .ws_path("/ws")
                .echo_messages(false)
                .append_rollup_window(40)
                .query_param("tenant", "a&b"),
        );
        let path = path_and_query(&config, Some("tok/+="));
        assert!(path.starts_with("/ws/app/app%20key?protocol=2&client=rust&version="));
        assert!(path.contains("&flash=false&format=json&echo_messages=false&append_mode=delta"));
        assert!(path.contains("&append_rollup_window=40&token=tok%2F%2B%3D&tenant=a%26b"));
    }

    #[test]
    fn v1_query_omits_v2_parameters() {
        let config = config(
            ClientBuilder::new("k")
                .ws_host("h")
                .protocol_version(ProtocolVersion::V1),
        );
        let path = path_and_query(&config, None);
        assert!(path.starts_with("/app/k?protocol=7&client=rust"));
        assert!(!path.contains("format="));
        assert!(!path.contains("echo_messages"));
    }

    #[test]
    fn display_url_hides_query_and_brackets_ipv6() {
        let config = config(
            ClientBuilder::new("k")
                .ws_host("::1")
                .force_tls(false)
                .ws_port(6001),
        );
        assert_eq!(display_url(&config, Transport::Ws), "ws://[::1]:6001/app/k");
    }

    #[test]
    fn reconnect_delay_is_quadratic_capped_and_jittered() {
        let gap = Duration::from_secs(120);
        assert_eq!(reconnect_delay(0, gap, 0.0), Duration::ZERO);
        assert_eq!(reconnect_delay(1, gap, 0.0), Duration::from_secs(1));
        assert_eq!(reconnect_delay(3, gap, 0.0), Duration::from_secs(9));
        assert_eq!(reconnect_delay(50, gap, 0.0), gap);
        for _ in 0..100 {
            let delay = reconnect_delay(4, gap, 0.5);
            assert!(delay >= Duration::from_secs(8) && delay <= Duration::from_secs(16));
        }
    }

    #[cfg(feature = "tls")]
    #[test]
    fn default_tls_config_builds() {
        let config = default_tls_config().unwrap();
        assert_eq!(config.alpn_protocols, vec![b"http/1.1".to_vec()]);
    }
}
