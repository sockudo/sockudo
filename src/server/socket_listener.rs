use std::path::PathBuf;
#[cfg(unix)]
use tokio::net::{UnixListener};
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::net::unix::{SocketAddr, UCred};
use tracing::{info};
use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub enum ServerBinding {
    Tcp { host: String, port: u16 },

    Unix { path: PathBuf },
}

#[derive(Debug, Clone)]
pub enum ConnectionInfo {
    Tcp(std::net::SocketAddr),
    #[cfg(unix)]
    Unix(UnixConnectionInfo),
}

#[cfg(unix)]
#[derive(Clone, Debug)]
pub struct UnixConnectionInfo {
    pub peer_addr: Arc<UnixSocketAddr>,
    pub peer_cred: UCred,
}

#[cfg(unix)]
impl connect_info::Connected<IncomingStream<'_, UnixListener>> for UnixConnectionInfo {
    fn connect_info(stream: IncomingStream<'_, UnixListener>) -> Self {
        let peer_addr = stream.io().peer_addr().unwrap();
        let peer_cred = stream.io().peer_cred().unwrap();
        Self {
            peer_addr: Arc::new(peer_addr),
            peer_cred,
        }
    }
}

pub struct SocketListener;

impl SocketListener {
    /// Create the appropriate listener based on configuration
    pub async fn bind(binding: &ServerBinding) -> Result<ServerListener> {
        match binding {
            ServerBinding::Tcp { host, port } => {
                let addr = format!("{}:{}", host, port);
                info!("Binding to TCP socket: {}", addr);

                let listener = TcpListener::bind(&addr).await
                    .map_err(|e| Error::Internal(format!("Failed to bind TCP socket {}: {}", addr, e)))?;

                let local_addr = listener.local_addr()
                    .map_err(|e| Error::Internal(format!("Failed to get local address: {}", e)))?;

                info!("Successfully bound to TCP address: {}", local_addr);
                Ok(ServerListener::Tcp(listener))
            }
            #[cfg(unix)]
            ServerBinding::Unix { path } => {
                info!("Binding to Unix socket: {}", path.display());

                // Remove existing socket file if it exists
                if path.exists() {
                    tokio::fs::remove_file(path).await
                        .map_err(|e| Error::Internal(format!("Failed to remove existing socket file: {}", e)))?;
                }

                // Create parent directory if it doesn't exist
                if let Some(parent) = path.parent() {
                    tokio::fs::create_dir_all(parent).await
                        .map_err(|e| Error::Internal(format!("Failed to create socket directory: {}", e)))?;
                }

                let listener = UnixListener::bind(path)
                    .map_err(|e| Error::Internal(format!("Failed to bind Unix socket {}: {}", path.display(), e)))?;

                info!("Successfully bound to Unix socket: {}", path.display());
                Ok(ServerListener::Unix(listener))
            },
            #[cfg(not(unix))]
            ServerBinding::Unix { .. } => {
                Err(Error::Configuration("Unix sockets are not supported on this platform".to_string()))
            }
        }
    }
}

pub enum ServerListener {
    Tcp(TcpListener),
    #[cfg(unix)]
    Unix(UnixListener),
}