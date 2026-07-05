use super::super::spawn_supervised_worker;
use super::static_push_token_provider;
use sockudo_core::options::ServerOptions;
use std::sync::Arc;
use tracing::warn;

#[cfg(all(feature = "push", feature = "monolith", feature = "push-wns"))]
pub(in crate::bootstrap::push::workers) fn start_wns_provider_workers(
    config: &ServerOptions,
    queue: sockudo_push::DynPushQueue,
) -> Vec<tokio::task::JoinHandle<()>> {
    let token_provider =
        match static_push_token_provider("WNS", &["WNS_PROVIDER_TOKEN", "PUSH_WNS_PROVIDER_TOKEN"])
        {
            Ok(provider) => provider,
            Err(error) => {
                warn!(error = %error, "WNS dispatch worker not started");
                return Vec::new();
            }
        };

    let mut handles = Vec::new();
    let max_outbound = config.push.dispatch_max_outbound_requests;
    for worker_index in 0..config.push.dispatch_worker_count {
        let http = match sockudo_push::ReqwestProviderHttpClient::new() {
            Ok(http) => Arc::new(http),
            Err(error) => {
                warn!(error = %error, "failed to create WNS HTTP client");
                continue;
            }
        };
        let dispatcher = sockudo_push::WnsDispatcher::new(token_provider.clone(), http);
        handles.push(spawn_supervised_worker(
            "provider",
            format!("sockudo-monolith-wns-{worker_index}"),
            {
                let queue = queue.clone();
                move |group| {
                    let queue = queue.clone();
                    let dispatcher = dispatcher.clone();
                    async move {
                        let mut worker = sockudo_push::ProviderDispatchWorker::new(
                            sockudo_push::PushProviderKind::Wns,
                            queue,
                            Arc::new(dispatcher),
                        )
                        .with_max_outbound_requests(max_outbound);
                        warn!(worker = %group, "WNS dispatch worker started");
                        loop {
                            match worker.run_once(&group).await {
                                Ok(processed) if processed > 0 => {
                                    warn!(worker = %group, processed, "WNS dispatch worker processed messages");
                                }
                                Ok(_) => {}
                                Err(error) => {
                                    warn!(worker = %group, error = %error, "WNS dispatch worker tick failed");
                                }
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                        }
                    }
                }
            },
        ));
    }
    handles
}
