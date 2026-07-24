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
use std::sync::atomic::Ordering;
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
        let running = &self.state.running;

        tokio::select! {
            result = axum::serve(uds, app) => {
                if let Err(err) = result {
                    error!(error = %err, "unix socket server error");
                }
            }
            _ = self.shutdown_signal() => {
                info!("Shutdown signal received, stopping Unix socket server...");
                running.store(false, Ordering::SeqCst);
            }
        }

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
            let running = &self.state.running;
            let server = axum_server::bind(http_addr).acceptor(
                RustlsAcceptor::new(tls_config)
                    .acceptor(axum_server::accept::NoDelayAcceptor::new()),
            );

            tokio::select! {
                result = server.serve(router_with_middleware_ssl.into_make_service_with_connect_info::<SocketAddr>()) => {
                    if let Err(err) = result {
                        error!(error = %err, "https server error");
                    }
                }
                _ = self.shutdown_signal() => {
                    info!("Shutdown signal received, stopping HTTPS server...");
                    running.store(false, Ordering::SeqCst);
                }
            }
        } else {
            info!("SSL is not enabled, starting HTTP server");
            info!(listen_addr = %http_addr, "http server listening");

            let running = &self.state.running;
            let http_server = axum_server::bind(http_addr)
                .acceptor(axum_server::accept::NoDelayAcceptor::new())
                .serve(
                    router_with_middleware_http.into_make_service_with_connect_info::<SocketAddr>(),
                );

            tokio::select! {
                res = http_server => {
                    if let Err(err) = res {
                        error!(error = %err, "http server error");
                    }
                }
                _ = self.shutdown_signal() => {
                    info!("Shutdown signal received, stopping HTTP server...");
                    running.store(false, Ordering::SeqCst);
                }
            }
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

    pub(crate) async fn stop(&self) -> Result<()> {
        info!("Stopping server...");
        self.state.running.store(false, Ordering::SeqCst);
        self.handler.shutdown_ai_workers().await;

        // Tell cluster peers this node is leaving and no responses are expected
        if let Err(e) = self
            .state
            .connection_manager
            .announce_node_departure()
            .await
        {
            warn!(error = %e, "failed to announce node departure");
        }

        let mut connections_to_cleanup: Vec<(String, WebSocketRef)> = Vec::new();

        {
            match self.state.connection_manager.get_namespaces().await {
                Ok(namespaces_vec) => {
                    for (app_id, namespace_obj) in namespaces_vec {
                        match namespace_obj.get_sockets().await {
                            Ok(sockets_vec) => {
                                for (_socket_id, ws_raw_obj) in sockets_vec {
                                    connections_to_cleanup
                                        .push((app_id.clone(), ws_raw_obj.clone()));
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

        if self.state.cleanup_worker_handles.is_some() {
            info!("Cleanup system will shutdown when server process ends");
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
        if let Err(e) = self.state.cache_manager.disconnect().await {
            warn!(error = %e, "error disconnecting cache manager");
        }
        if let Some(queue_manager_arc) = &self.state.queue_manager
            && let Err(e) = queue_manager_arc.disconnect().await
        {
            warn!(error = %e, "error disconnecting queue manager");
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

        info!(
            grace_period_s = self.config.shutdown_grace_period,
            "waiting for shutdown grace period"
        );
        tokio::time::sleep(Duration::from_secs(self.config.shutdown_grace_period)).await;
        info!("Server stopped");
        Ok(())
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
