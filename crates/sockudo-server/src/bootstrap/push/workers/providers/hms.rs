use super::static_push_token_provider;
use sockudo_core::options::ServerOptions;
use std::{env, sync::Arc, time::Duration};
use tracing::warn;

#[cfg(all(feature = "push", feature = "monolith", feature = "push-hms"))]
pub(in crate::bootstrap::push::workers) fn start_hms_provider_workers(
    config: &ServerOptions,
    queue: sockudo_push::DynPushQueue,
) {
    let app_id = env::var("HMS_APP_ID").or_else(|_| env::var("PUSH_HMS_APP_ID"));
    let Ok(app_id) = app_id else {
        warn!(
            "push.hms_enabled is true but HMS_APP_ID/PUSH_HMS_APP_ID is not set; HMS dispatch worker not started"
        );
        return;
    };
    if app_id.trim().is_empty() {
        warn!("HMS_APP_ID/PUSH_HMS_APP_ID is empty; HMS dispatch worker not started");
        return;
    }

    let token_provider =
        match static_push_token_provider("HMS", &["HMS_PROVIDER_TOKEN", "PUSH_HMS_PROVIDER_TOKEN"])
        {
            Ok(provider) => provider,
            Err(error) => {
                warn!(error = %error, "HMS dispatch worker not started");
                return;
            }
        };
    let endpoint = env::var("HMS_ENDPOINT")
        .or_else(|_| env::var("PUSH_HMS_ENDPOINT"))
        .ok();

    for worker_index in 0..config.push.dispatch_worker_count {
        let http = match sockudo_push::ReqwestProviderHttpClient::new() {
            Ok(http) => Arc::new(http),
            Err(error) => {
                warn!(error = %error, "failed to create HMS HTTP client");
                continue;
            }
        };
        let mut dispatcher =
            sockudo_push::HmsDispatcher::new(app_id.clone(), token_provider.clone(), http);
        if let Some(endpoint) = endpoint.clone() {
            dispatcher = dispatcher.with_base_url(endpoint);
        }
        let mut worker = sockudo_push::ProviderDispatchWorker::new(
            sockudo_push::PushProviderKind::Hms,
            queue.clone(),
            Arc::new(dispatcher),
        );
        tokio::spawn(async move {
            let group = format!("sockudo-monolith-hms-{worker_index}");
            warn!(worker = %group, "HMS dispatch worker started");
            loop {
                match worker.run_once(&group).await {
                    Ok(processed) if processed > 0 => {
                        warn!(worker = %group, processed, "HMS dispatch worker processed messages");
                    }
                    Ok(_) => {}
                    Err(error) => {
                        warn!(worker = %group, error = %error, "HMS dispatch worker tick failed");
                    }
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        });
    }
}
