use super::SockudoServer;
use axum::extract::Request;
#[cfg(unix)]
use axum::extract::connect_info::{self};
use axum::http::header;
use axum::http::uri::Authority;
use axum::http::{StatusCode, Uri};
use axum::response::Redirect;
#[cfg(unix)]
use axum::serve::IncomingStream;
use axum::{BoxError, Router, ServiceExt};
use axum_server::tls_rustls::{RustlsAcceptor, RustlsConfig};
use futures_util::future::join_all;
use sockudo_core::error::{Error, Result};
use sockudo_core::websocket::WebSocketRef;
use std::net::SocketAddr;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::signal;
use tower::Layer;
#[cfg(unix)]
use tracing::debug;
use tracing::{error, info, warn};

#[cfg(unix)]
#[derive(Clone, Debug)]
struct UdsConnectInfo {
    peer_addr: Arc<tokio::net::unix::SocketAddr>,
    peer_cred: tokio::net::unix::UCred,
}

#[cfg(unix)]
/// Convert octal permission mode to human-readable string (e.g., 0o755 -> "rwxr-xr-x")
fn format_permission_string(mode: u32) -> String {
    let owner = [
        (mode & 0o400) != 0,
        (mode & 0o200) != 0,
        (mode & 0o100) != 0,
    ];
    let group = [
        (mode & 0o040) != 0,
        (mode & 0o020) != 0,
        (mode & 0o010) != 0,
    ];
    let other = [
        (mode & 0o004) != 0,
        (mode & 0o002) != 0,
        (mode & 0o001) != 0,
    ];

    [owner, group, other]
        .iter()
        .map(|perms| {
            format!(
                "{}{}{}",
                if perms[0] { 'r' } else { '-' },
                if perms[1] { 'w' } else { '-' },
                if perms[2] { 'x' } else { '-' }
            )
        })
        .collect::<Vec<_>>()
        .join("")
}

#[cfg(unix)]
impl connect_info::Connected<IncomingStream<'_, UnixListener>> for UdsConnectInfo {
    fn connect_info(stream: IncomingStream<'_, UnixListener>) -> Self {
        let peer_addr = stream
            .io()
            .peer_addr()
            .expect("Failed to get peer address from Unix socket");
        let peer_cred = stream
            .io()
            .peer_cred()
            .expect("Failed to get peer credentials from Unix socket");
        Self {
            peer_addr: Arc::new(peer_addr),
            peer_cred,
        }
    }
}

/// Normalize URI path by removing trailing slashes (except for root "/")
fn normalize_uri_path(path: &str) -> String {
    if path.len() > 1 && path.ends_with('/') {
        path[..path.len() - 1].to_string()
    } else {
        path.to_string()
    }
}

/// Common logic for URI normalization
fn normalize_request_uri<B>(mut req: Request<B>) -> Request<B> {
    let uri = req.uri();
    let normalized_path = normalize_uri_path(uri.path());

    if normalized_path != uri.path() {
        let mut parts = uri.clone().into_parts();
        if let Some(path_and_query) = &parts.path_and_query {
            let query = path_and_query
                .query()
                .map(|q| format!("?{q}"))
                .unwrap_or_default();
            let new_path_and_query = format!("{normalized_path}{query}");
            if let Ok(new_pq) = new_path_and_query.parse() {
                parts.path_and_query = Some(new_pq);
                if let Ok(new_uri) = Uri::from_parts(parts) {
                    *req.uri_mut() = new_uri;
                }
            }
        }
    }
    req
}

/// Request URI rewriter for SSL connections (axum_server with TLS)
fn rewrite_request_uri_ssl(req: Request<hyper::body::Incoming>) -> Request<hyper::body::Incoming> {
    let (mut parts, body) = req.into_parts();
    let normalized_path = normalize_uri_path(parts.uri.path());

    if normalized_path != parts.uri.path()
        && let Some(path_and_query) = &parts.uri.path_and_query()
    {
        let query = path_and_query
            .query()
            .map(|q| format!("?{q}"))
            .unwrap_or_default();
        let new_path_and_query = format!("{normalized_path}{query}");
        if let Ok(new_pq) = new_path_and_query.parse() {
            let mut uri_parts = parts.uri.clone().into_parts();
            uri_parts.path_and_query = Some(new_pq);
            if let Ok(new_uri) = Uri::from_parts(uri_parts) {
                parts.uri = new_uri;
            }
        }
    }
    Request::from_parts(parts, body)
}

/// Request URI rewriter for non-SSL connections (regular axum)
fn rewrite_request_uri(req: Request<axum::body::Body>) -> Request<axum::body::Body> {
    normalize_request_uri(req)
}

/// Request URI rewriter for HTTP connections with axum_server (uses hyper::body::Incoming)
fn rewrite_request_uri_http(req: Request<hyper::body::Incoming>) -> Request<hyper::body::Incoming> {
    let (mut parts, body) = req.into_parts();
    let normalized_path = normalize_uri_path(parts.uri.path());

    if normalized_path != parts.uri.path()
        && let Some(path_and_query) = &parts.uri.path_and_query()
    {
        let query = path_and_query
            .query()
            .map(|q| format!("?{q}"))
            .unwrap_or_default();
        let new_path_and_query = format!("{normalized_path}{query}");
        if let Ok(new_pq) = new_path_and_query.parse() {
            let mut uri_parts = parts.uri.clone().into_parts();
            uri_parts.path_and_query = Some(new_pq);
            if let Ok(new_uri) = Uri::from_parts(uri_parts) {
                parts.uri = new_uri;
            }
        }
    }
    Request::from_parts(parts, body)
}

impl SockudoServer {
    async fn get_http_addr(&self) -> SocketAddr {
        sockudo_core::utils::resolve_socket_addr(&self.config.host, self.config.port, "HTTP server")
            .await
    }

    async fn get_metrics_addr(&self) -> SocketAddr {
        sockudo_core::utils::resolve_socket_addr(
            &self.config.metrics.host,
            self.config.metrics.port,
            "Metrics server",
        )
        .await
    }

    pub(crate) async fn start(&self) -> Result<()> {
        info!("Starting Sockudo server services (after init)...");

        // Start metrics server (always runs independently)
        if self.config.metrics.enabled {
            let metrics_router = self.configure_metrics_routes();
            let metrics_addr = self.get_metrics_addr().await;

            match TcpListener::bind(metrics_addr).await {
                Ok(metrics_listener) => {
                    info!(listen_addr = %metrics_addr, "metrics server listening");
                    let metrics_router_clone = metrics_router.clone();
                    tokio::spawn(async move {
                        if let Err(e) =
                            axum::serve(metrics_listener, metrics_router_clone.into_make_service())
                                .await
                        {
                            error!(error = %e, "metrics server error");
                        }
                    });
                }
                Err(e) => {
                    warn!(listen_addr = %metrics_addr, error = %e, "failed to bind metrics server, metrics will not be available");
                }
            }
        }

        let http_router = self.configure_http_routes();

        // Embedded MCP surface drives this same router in-process, so it is
        // built from (and optionally merged into) the finished API router.
        #[cfg(feature = "mcp")]
        let http_router = match self.build_mcp(&http_router)? {
            Some(mcp) => match mcp.dedicated {
                Some((host, port)) => {
                    let addr =
                        sockudo_core::utils::resolve_socket_addr(&host, port, "MCP server").await;
                    match TcpListener::bind(addr).await {
                        Ok(listener) => {
                            info!(listen_addr = %addr, path = %self.config.mcp.path, "mcp server listening");
                            let mcp_router = mcp.router;
                            tokio::spawn(async move {
                                if let Err(e) = axum::serve(
                                    listener,
                                    mcp_router.into_make_service_with_connect_info::<SocketAddr>(),
                                )
                                .await
                                {
                                    error!(error = %e, "mcp server error");
                                }
                            });
                        }
                        Err(e) => {
                            return Err(Error::Internal(format!(
                                "failed to bind MCP listener on {addr}: {e}"
                            )));
                        }
                    }
                    http_router
                }
                None => {
                    info!(path = %self.config.mcp.path, "mcp served on the main http listener");
                    http_router.merge(mcp.router)
                }
            },
            None => http_router,
        };

        #[cfg(not(feature = "mcp"))]
        if self.config.mcp.enabled {
            warn!(
                "mcp.enabled is set but this binary was built without the `mcp` Cargo feature; rebuild with --features mcp to serve MCP"
            );
        }

        // Choose between Unix socket OR HTTP/HTTPS for main server
        #[cfg(unix)]
        if self.config.unix_socket.enabled {
            return self.start_unix_socket_server(http_router).await;
        }

        // Fail fast if Unix socket is requested on non-Unix platform
        #[cfg(not(unix))]
        if self.config.unix_socket.enabled {
            error!(
                "Unix socket support is only available on Unix-like systems (Linux, macOS, BSD)."
            );
            error!(
                "Please disable unix_socket.enabled in your configuration to use HTTP/HTTPS instead."
            );
            return Err(Error::Configuration(
                "Unix sockets are not supported on this platform (Windows). Please set unix_socket.enabled to false.".to_string()
            ));
        }

        self.start_http_server(http_router).await
    }

    #[cfg(unix)]
    async fn start_unix_socket_server(&self, http_router: Router) -> Result<()> {
        info!(listen_addr = %self.config.unix_socket.path, "starting unix socket server");
        let path = std::path::PathBuf::from(&self.config.unix_socket.path);

        if path.exists() {
            match tokio::fs::remove_file(&path).await {
                Ok(()) => {
                    debug!(socket_path = %path.display(), "removed existing unix socket file");
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    debug!(socket_path = %path.display(), "unix socket file was already removed");
                }
                Err(e) => {
                    warn!(error = %e, "failed to remove existing unix socket file");
                }
            }
        }

        if let Some(parent) = path.parent() {
            if let Err(e) = tokio::fs::create_dir_all(parent).await {
                error!(error = %e, "failed to create parent directory for unix socket");
                return Err(Error::Internal(format!(
                    "Failed to create Unix socket directory: {}",
                    e
                )));
            }

            if let Err(e) = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o755))
            {
                warn!(error = %e, "failed to set secure permissions on unix socket parent directory");
            } else {
                info!(socket_dir = %parent.display(), "set secure permissions on unix socket parent directory");
            }
        }

        let uds = UnixListener::bind(&path)
            .map_err(|e| Error::Internal(format!("Failed to bind Unix socket: {}", e)))?;

        if let Err(e) = std::fs::set_permissions(
            &path,
            std::fs::Permissions::from_mode(self.config.unix_socket.permission_mode),
        ) {
            warn!(error = %e, "failed to set unix socket permissions");
        } else {
            info!(
                permission_mode = self.config.unix_socket.permission_mode,
                permission_str = %format_permission_string(self.config.unix_socket.permission_mode),
                "set unix socket permissions"
            );
        }

        let middleware = tower::util::MapRequestLayer::new(rewrite_request_uri);
        let router_with_middleware = middleware.layer(http_router);

        info!(listen_addr = %path.display(), "unix socket server listening");
        let app = router_with_middleware.into_make_service_with_connect_info::<UdsConnectInfo>();

        self.serve_until_shutdown(axum::serve(uds, app), "unix socket")
            .await;

        info!("Unix socket server stopped. Initiating final stop sequence.");
        Ok(())
    }

    async fn start_http_server(&self, http_router: Router) -> Result<()> {
        let http_addr = self.get_http_addr().await;

        let middleware = tower::util::MapRequestLayer::new(rewrite_request_uri);
        let router_with_middleware = middleware.layer(http_router.clone());

        let middleware_ssl = tower::util::MapRequestLayer::new(rewrite_request_uri_ssl);
        let router_with_middleware_ssl = middleware_ssl.layer(http_router.clone());

        let middleware_http = tower::util::MapRequestLayer::new(rewrite_request_uri_http);
        let router_with_middleware_http = middleware_http.layer(http_router);

        if self.config.ssl.enabled
            && !self.config.ssl.cert_path.is_empty()
            && !self.config.ssl.key_path.is_empty()
        {
            info!("SSL is enabled, starting HTTPS server");
            let tls_config = self.load_tls_config().await?;

            if self.config.ssl.redirect_http {
                let http_port = self.config.ssl.http_port.unwrap_or(80);
                let host_ip = self
                    .config
                    .host
                    .parse::<std::net::IpAddr>()
                    .unwrap_or_else(|_| "0.0.0.0".parse().unwrap());
                let redirect_addr = SocketAddr::from((host_ip, http_port));
                info!(listen_addr = %redirect_addr, "starting http to https redirect server");

                let https_port = self.config.port;
                let redirect_app =
                    Router::new().fallback(move |headers: axum::http::HeaderMap, uri: Uri| async move {
                        let host = headers
                            .get(header::HOST)
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("localhost");
                        match make_https(host, uri, https_port) {
                            Ok(uri_https) => Ok(Redirect::permanent(&uri_https.to_string())),
                            Err(error) => {
                                error!(error = ?error, "failed to convert URI to HTTPS for redirect");
                                Err(StatusCode::BAD_REQUEST)
                            }
                        }
                    });

                match TcpListener::bind(redirect_addr).await {
                    Ok(redirect_listener) => {
                        tokio::spawn(async move {
                            if let Err(e) = axum::serve(
                                redirect_listener,
                                redirect_app.into_make_service_with_connect_info::<SocketAddr>(),
                            )
                            .await
                            {
                                error!(error = %e, "http redirect server error");
                            }
                        });
                    }
                    Err(e) => {
                        warn!(listen_addr = %redirect_addr, error = %e, "failed to bind http redirect server, redirect will not be available")
                    }
                }
            }

            info!(listen_addr = %http_addr, "https server listening");
            let server = axum_server::bind(http_addr)
                .acceptor(
                    RustlsAcceptor::new(tls_config)
                        .acceptor(axum_server::accept::NoDelayAcceptor::new()),
                )
                .serve(
                    router_with_middleware_ssl.into_make_service_with_connect_info::<SocketAddr>(),
                );

            self.serve_until_shutdown(server, "https").await;
        } else {
            info!("SSL is not enabled, starting HTTP server");
            info!(listen_addr = %http_addr, "http server listening");

            let http_server = axum_server::bind(http_addr)
                .acceptor(axum_server::accept::NoDelayAcceptor::new())
                .serve(
                    router_with_middleware_http.into_make_service_with_connect_info::<SocketAddr>(),
                );

            self.serve_until_shutdown(http_server, "http").await;
        }

        info!("HTTP server stopped. Initiating final stop sequence.");
        Ok(())
    }

    async fn load_tls_config(&self) -> Result<RustlsConfig> {
        let cert_path = std::path::PathBuf::from(&self.config.ssl.cert_path);
        let key_path = std::path::PathBuf::from(&self.config.ssl.key_path);
        if !cert_path.exists() {
            return Err(Error::ConfigFile(format!(
                "SSL cert_path not found: {cert_path:?}"
            )));
        }
        if !key_path.exists() {
            return Err(Error::ConfigFile(format!(
                "SSL key_path not found: {key_path:?}"
            )));
        }
        RustlsConfig::from_pem_file(cert_path, key_path)
            .await
            .map_err(|e| Error::Internal(format!("Failed to load TLS configuration: {e}")))
    }

    async fn serve_until_shutdown<F>(&self, server: F, name: &str)
    where
        F: IntoFuture<Output = std::io::Result<()>>,
    {
        serve_until_shutdown(
            server,
            || self.shutdown_signal(),
            &self.state.running,
            Duration::from_secs(self.config.shutdown_grace_period),
            Some(|| {
                let server = self;
                async move { server.close_all_connections().await }
            }),
            name,
        )
        .await;
    }

    async fn shutdown_signal(&self) {
        let ctrl_c = async {
            signal::ctrl_c()
                .await
                .expect("Failed to install Ctrl+C handler");
        };

        #[cfg(unix)]
        let terminate = async {
            signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("Failed to install signal handler")
                .recv()
                .await;
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => info!("Ctrl+C received, initiating shutdown..."),
            _ = terminate => info!("Terminate signal received, initiating shutdown..."),
        }
    }

    /// Close every live websocket with `4200 Server shutting down` so clients
    /// reconnect immediately instead of waiting for the listener grace period
    /// or an LB deregistration cutoff.
    pub(crate) async fn close_all_connections(&self) {
        let mut connections_to_cleanup: Vec<(String, WebSocketRef)> = Vec::new();

        match self.state.connection_manager.get_namespaces().await {
            Ok(namespaces_vec) => {
                for (app_id, namespace_obj) in namespaces_vec {
                    match namespace_obj.get_sockets().await {
                        Ok(sockets_vec) => {
                            for (_socket_id, ws_raw_obj) in sockets_vec {
                                connections_to_cleanup.push((app_id.clone(), ws_raw_obj.clone()));
                            }
                        }
                        Err(e) => {
                            warn!(%app_id, error = %e, "failed to get sockets for namespace during shutdown");
                        }
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "failed to get namespaces during shutdown");
            }
        }

        info!(
            connection_count = connections_to_cleanup.len(),
            "collected connections to cleanup"
        );

        if !connections_to_cleanup.is_empty() {
            let cleanup_futures =
                connections_to_cleanup
                    .into_iter()
                    .map(|(_app_id, ws_raw_obj)| async move {
                        let mut ws = ws_raw_obj.inner.lock().await;
                        if let Err(e) = ws.close(4200, "Server shutting down".to_string()).await {
                            error!(error = %e, "failed to close websocket");
                        }
                    });

            join_all(cleanup_futures).await;
            info!("All connection cleanup tasks have been processed.");
        } else {
            info!("No connections to cleanup.");
        }
    }

    pub(crate) async fn stop(&self) -> Result<()> {
        info!("Stopping server...");
        self.state.running.store(false, Ordering::SeqCst);
        self.handler.shutdown_ai_workers().await;

        // Connections were already closed with 4200 by the first shutdown
        // signal; this covers direct stop() callers that skip the signal path.
        self.close_all_connections().await;

        // Keep adapters, caches and queue workers alive until every disconnect producer
        // has finished. Presence leases retain their reconnect suppression deadlines.
        let drain_timeout =
            Duration::from_millis(self.config.queue.reliability.shutdown_timeout_ms);
        match tokio::time::timeout(drain_timeout, self.drain_shutdown_webhooks()).await {
            Ok(Ok(job_count)) => info!(job_count, "shutdown webhook drain completed"),
            Ok(Err(error)) => error!(error = %error, "shutdown webhook drain failed"),
            Err(error) => warn!(
                error = %error,
                timeout_ms = self.config.queue.reliability.shutdown_timeout_ms,
                "shutdown webhook drain timed out, pending lifecycle events may be lost"
            ),
        }
        self.state.cleanup_cancellation.cancel();

        // Keep this node's presence visible to peers until its leases and reconnect
        // checks finish. NodeDead removes its replicated presence from peer registries.
        if !self.config.server_role.is_api()
            && let Err(e) = self
                .state
                .connection_manager
                .announce_node_departure()
                .await
        {
            warn!(error = %e, "failed to announce node departure");
        }

        #[cfg(all(feature = "push", feature = "monolith"))]
        {
            for handle in &self.state.push_worker_handles {
                handle.abort();
            }
            info!(
                worker_count = self.state.push_worker_handles.len(),
                "aborted push worker supervisor tasks"
            );
        }

        // Stop delta compression cleanup task gracefully
        #[cfg(feature = "delta")]
        self.state.delta_compression.stop_cleanup_task().await;

        // Disconnect from backend services
        if let Some(queue_manager_arc) = &self.state.queue_manager
            && let Err(e) = queue_manager_arc.disconnect().await
        {
            warn!(error = %e, "error disconnecting queue manager");
        }
        if let Err(e) = self.state.cache_manager.disconnect().await {
            warn!(error = %e, "error disconnecting cache manager");
        }

        // Clean up Unix socket file if it exists (with race condition handling)
        #[cfg(unix)]
        if self.config.unix_socket.enabled {
            let path = std::path::PathBuf::from(&self.config.unix_socket.path);
            match tokio::fs::remove_file(&path).await {
                Ok(()) => {
                    info!(socket_path = %self.config.unix_socket.path, "removed unix socket file");
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    debug!(socket_path = %path.display(), "unix socket file was already removed");
                }
                Err(e) => {
                    warn!(error = %e, "failed to remove unix socket file during shutdown");
                }
            }
        }

        info!("Server stopped");
        Ok(())
    }

    async fn drain_shutdown_webhooks(&self) -> Result<usize> {
        self.handler.drain_connection_cleanup().await;
        self.state.cleanup_cancellation.cancel();
        let workers = std::mem::take(&mut *self.state.cleanup_worker_handles.lock().await);
        for worker in workers {
            worker
                .await
                .map_err(|error| Error::Internal(format!("cleanup worker failed: {error}")))?;
        }
        self.handler
            .presence_manager()
            .drain_pending_removals()
            .await;
        self.state.webhooks_integration.shutdown().await
    }
}

/// Runs `server` until a shutdown signal, closes existing connections via
/// `on_shutdown` (typically with code 4200 so clients reconnect immediately),
/// then keeps the listener open for `grace_period` so load balancers can
/// observe the 503s from `/up` and `/ready` before it closes. A second signal
/// ends the grace period early.
async fn serve_until_shutdown<F, S, T>(
    server: F,
    shutdown: impl Fn() -> S,
    running: &AtomicBool,
    grace_period: Duration,
    on_shutdown: Option<impl FnOnce() -> T>,
    name: &str,
) where
    F: IntoFuture<Output = std::io::Result<()>>,
    S: Future<Output = ()>,
    T: Future<Output = ()>,
{
    let server = server.into_future();
    tokio::pin!(server);

    tokio::select! {
        result = &mut server => {
            if let Err(err) = result {
                error!(error = %err, "{name} server error");
            }
            return;
        }
        _ = shutdown() => {
            info!("Shutdown signal received, stopping {name} server...");
            running.store(false, Ordering::SeqCst);
        }
    }

    // Close existing connections before the grace period so clients get the
    // WebSocket Close frame (4200) instead of an LB cutoff or process kill.
    if let Some(on_shutdown) = on_shutdown {
        on_shutdown().await;
    }

    if grace_period.is_zero() {
        return;
    }

    info!(
        grace_period_s = grace_period.as_secs(),
        "reporting not ready before closing {name} listener"
    );
    tokio::select! {
        result = &mut server => {
            if let Err(err) = result {
                error!(error = %err, "{name} server error");
            }
        }
        _ = tokio::time::sleep(grace_period) => {}
        _ = shutdown() => {
            info!("Second shutdown signal received, ending grace period early");
        }
    }
}

fn make_https(host: &str, uri: Uri, https_port: u16) -> core::result::Result<Uri, BoxError> {
    let mut parts = uri.into_parts();
    parts.scheme = Some(http::uri::Scheme::HTTPS);

    if parts.path_and_query.is_none() {
        parts.path_and_query = Some("/".parse().unwrap());
    }

    let authority_val: Authority = host
        .parse()
        .map_err(|e| format!("Failed to parse host '{host}' into authority: {e}"))?;

    let bare_host_str = authority_val.host();

    parts.authority = Some(
        format!("{bare_host_str}:{https_port}")
            .parse()
            .map_err(|e| {
                format!("Failed to create new authority '{bare_host_str}:{https_port}': {e}")
            })?,
    );

    Uri::from_parts(parts).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;
    use std::time::Instant;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    async fn ready_status_line(addr: SocketAddr) -> std::io::Result<String> {
        let mut stream = TcpStream::connect(addr).await?;
        stream
            .write_all(b"GET /ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await?;
        let mut response = String::new();
        stream.read_to_string(&mut response).await?;
        Ok(response.lines().next().unwrap_or_default().to_string())
    }

    struct TestServer {
        addr: SocketAddr,
        running: Arc<AtomicBool>,
        signal: Arc<tokio::sync::Notify>,
        handle: tokio::task::JoinHandle<()>,
    }

    async fn spawn_test_server(grace_period: Duration) -> TestServer {
        let running = Arc::new(AtomicBool::new(true));
        let flag = running.clone();
        let app = Router::new().route(
            "/ready",
            get(move || {
                let flag = flag.clone();
                async move {
                    if flag.load(Ordering::SeqCst) {
                        StatusCode::OK
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE
                    }
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let signal = Arc::new(tokio::sync::Notify::new());

        let server_running = running.clone();
        let server_signal = signal.clone();
        let handle = tokio::spawn(async move {
            serve_until_shutdown(
                axum::serve(listener, app),
                || {
                    let signal = server_signal.clone();
                    async move { signal.notified().await }
                },
                &server_running,
                grace_period,
                None::<fn() -> std::future::Ready<()>>,
                "test",
            )
            .await;
        });

        TestServer {
            addr,
            running,
            signal,
            handle,
        }
    }

    async fn wait_until_draining(running: &AtomicBool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while running.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("running flag was not cleared after the shutdown signal");
    }

    #[tokio::test]
    async fn listener_stays_open_and_reports_draining_during_grace_period() {
        let grace_period = Duration::from_millis(500);
        let server = spawn_test_server(grace_period).await;

        assert_eq!(
            ready_status_line(server.addr).await.unwrap(),
            "HTTP/1.1 200 OK"
        );

        let signalled_at = Instant::now();
        server.signal.notify_one();
        wait_until_draining(&server.running).await;

        assert_eq!(
            ready_status_line(server.addr).await.unwrap(),
            "HTTP/1.1 503 Service Unavailable"
        );

        server.handle.await.unwrap();
        assert!(signalled_at.elapsed() >= grace_period);
        assert!(ready_status_line(server.addr).await.is_err());
    }

    #[tokio::test]
    async fn second_signal_ends_grace_period_early() {
        let server = spawn_test_server(Duration::from_secs(60)).await;

        server.signal.notify_one();
        wait_until_draining(&server.running).await;
        server.signal.notify_one();

        tokio::time::timeout(Duration::from_secs(5), server.handle)
            .await
            .expect("grace period did not end on the second signal")
            .unwrap();
    }

    /// The on_shutdown hook must run before the grace period elapses so
    /// existing connections receive their Close frame (4200) while the
    /// listener is still open, instead of being cut by a process kill or LB
    /// deregistration first.
    #[tokio::test]
    async fn on_shutdown_closes_connections_before_grace_period_ends() {
        let closed_at: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
        let grace_period = Duration::from_millis(500);

        let running = Arc::new(AtomicBool::new(true));
        let signal = Arc::new(tokio::sync::Notify::new());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_running = running.clone();
        let server_signal = signal.clone();
        let hook_closed = closed_at.clone();
        let handle = tokio::spawn(async move {
            serve_until_shutdown(
                axum::serve(
                    listener,
                    Router::new()
                        .route("/ready", get(|| async { StatusCode::SERVICE_UNAVAILABLE })),
                ),
                || {
                    let signal = server_signal.clone();
                    async move { signal.notified().await }
                },
                &server_running,
                grace_period,
                Some(move || {
                    let hook_closed = hook_closed.clone();
                    async move {
                        hook_closed.store(true, Ordering::SeqCst);
                    }
                }),
                "test",
            )
            .await;
        });

        signal.notify_one();
        wait_until_draining(&running).await;

        // The hook must have completed long before the 500 ms grace period ends.
        tokio::time::timeout(Duration::from_millis(100), async {
            while !closed_at.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("on_shutdown hook did not run before the grace period elapsed");

        // Listener must still be serving during the grace period.
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert_eq!(
            response.lines().next().unwrap_or_default(),
            "HTTP/1.1 503 Service Unavailable"
        );

        handle.await.unwrap();
    }
}
