use crate::adapter::ConnectionHandler;

use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use fastwebsockets::upgrade;
use serde::Deserialize;
use std::sync::Arc;
use tracing::log::error;

#[derive(Debug, Deserialize)]
pub struct ConnectionQuery {
    protocol: Option<u8>,
    client: Option<String>,
    version: Option<String>,
}

// WebSocket upgrade handler
pub async fn handle_ws_upgrade(
    Path(app_key): Path<String>,
    Query(params): Query<ConnectionQuery>,
    ws: upgrade::IncomingUpgrade,
    State(handler): State<Arc<ConnectionHandler>>,
) -> impl IntoResponse {
    let (response, fut) = ws.upgrade().unwrap();
    tokio::task::spawn(async move {
        if let Err(e) = handler.handle_socket(fut, app_key).await {
            error!("{}", format!("Error handling socket: {e}"));
        }
    });
    response
}
