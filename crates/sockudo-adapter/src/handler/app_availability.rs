//! Closes live connections of applications that were disabled or removed.
//!
//! The app manager only gates new connections. Without this sweep, sockets
//! opened before an app was disabled (abuse, unpaid account, leaked
//! credentials) would stay connected indefinitely.

use super::ConnectionHandler;
use sockudo_core::error::Error;
use sockudo_core::websocket::DisconnectCause;
use tracing::{debug, info, warn};

/// Outcome of one availability sweep over this node's local connections.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct AppAvailabilitySweep {
    /// Apps with local connections that were checked.
    pub apps_checked: usize,
    /// Apps that were found disabled or missing.
    pub apps_unavailable: usize,
    /// Local connections closed by this sweep.
    pub connections_closed: usize,
}

impl ConnectionHandler {
    /// Checks every app with local connections against the app manager and
    /// closes the local sockets of apps that are now disabled (`4003`) or no
    /// longer exist (`4001`). Both codes are in Pusher's `4000-4099` range,
    /// so clients do not reconnect.
    ///
    /// Lookup errors fail open: a transient app-store outage must not
    /// disconnect every tenant.
    pub async fn close_connections_of_unavailable_apps(&self) -> AppAvailabilitySweep {
        let mut sweep = AppAvailabilitySweep::default();
        let namespaces = match self.connection_manager.get_namespaces().await {
            Ok(namespaces) => namespaces,
            Err(error) => {
                warn!(error = %error, "app availability sweep could not list namespaces");
                return sweep;
            }
        };

        for (app_id, namespace) in namespaces {
            if namespace.sockets.is_empty() {
                continue;
            }
            sweep.apps_checked += 1;
            let error = match self.app_manager.find_by_id(&app_id).await {
                Ok(Some(app)) if app.enabled => continue,
                Ok(Some(_)) => Error::ApplicationDisabled,
                Ok(None) => Error::ApplicationNotFound,
                Err(error) => {
                    debug!(%app_id, error = %error, "app availability lookup failed; keeping connections");
                    continue;
                }
            };
            sweep.apps_unavailable += 1;

            let sockets = match namespace.get_sockets().await {
                Ok(sockets) => sockets,
                Err(error) => {
                    warn!(%app_id, error = %error, "app availability sweep could not list sockets");
                    continue;
                }
            };
            let code = error.close_code();
            let reason = error.to_string();
            let mut closed = 0usize;
            for (socket_id, socket) in sockets {
                let mut connection = socket.inner.lock().await;
                connection
                    .state
                    .record_disconnect_cause(DisconnectCause::AdministrativeClose);
                match connection.close(code, reason.clone()).await {
                    Ok(()) => closed += 1,
                    Err(error) => {
                        debug!(%app_id, %socket_id, error = %error, "failed to close connection of unavailable app");
                    }
                }
            }
            sweep.connections_closed += closed;
            info!(
                %app_id,
                close_code = code,
                connections_closed = closed,
                "closed live connections of unavailable app"
            );
        }

        sweep
    }
}
