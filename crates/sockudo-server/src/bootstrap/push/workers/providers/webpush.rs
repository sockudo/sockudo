use super::super::spawn_supervised_worker;
use sockudo_core::options::ServerOptions;
use std::{env, sync::Arc};
use tracing::warn;

#[cfg(all(feature = "push", feature = "monolith", feature = "push-webpush"))]
pub(in crate::bootstrap::push::workers) fn start_webpush_provider_workers(
    config: &ServerOptions,
    queue: sockudo_push::DynPushQueue,
) -> Vec<tokio::task::JoinHandle<()>> {
    let vapid_private_key =
        env::var("VAPID_PRIVATE_KEY").or_else(|_| env::var("PUSH_WEBPUSH_VAPID_PRIVATE_KEY"));
    let Ok(vapid_private_key) = vapid_private_key else {
        warn!(
            "push.webpush_enabled is true but VAPID_PRIVATE_KEY/PUSH_WEBPUSH_VAPID_PRIVATE_KEY is not set; Web Push dispatch worker not started"
        );
        return Vec::new();
    };
    let vapid_contact = env::var("VAPID_CONTACT")
        .or_else(|_| env::var("PUSH_WEBPUSH_VAPID_CONTACT"))
        .unwrap_or_else(|_| "mailto:sockudo-webpush@example.com".to_owned());

    let mut handles = Vec::new();
    let max_outbound = config.push.dispatch_max_outbound_requests;
    for worker_index in 0..config.push.dispatch_worker_count {
        let http = match sockudo_push::ReqwestProviderHttpClient::new() {
            Ok(http) => Arc::new(http),
            Err(error) => {
                warn!(error = %error, "failed to create Web Push HTTP client");
                continue;
            }
        };
        let dispatcher = sockudo_push::WebPushDispatcher::new(
            "webpush",
            sockudo_push::CachedTokenProvider::new(Arc::new(sockudo_push::StaticTokenSource::new(
                sockudo_push::SecretString::new("unused-for-vapid")
                    .expect("static webpush fallback token is non-empty"),
                u64::MAX,
            ))),
            Arc::new(sockudo_push::NativeWebPushCrypto::new(
                vapid_private_key.clone(),
                vapid_contact.clone(),
            )),
            http,
        );
        handles.push(spawn_supervised_worker(
            "provider",
            format!("sockudo-monolith-webpush-{worker_index}"),
            {
                let queue = queue.clone();
                move |group| {
                    let queue = queue.clone();
                    let dispatcher = dispatcher.clone();
                    async move {
                        let mut worker = sockudo_push::ProviderDispatchWorker::new(
                            sockudo_push::PushProviderKind::WebPush,
                            queue,
                            Arc::new(dispatcher),
                        )
                        .with_max_outbound_requests(max_outbound);
                        warn!(worker = %group, "Web Push dispatch worker started");
                        loop {
                            match worker.run_once(&group).await {
                                Ok(processed) if processed > 0 => {
                                    warn!(worker = %group, processed, "Web Push dispatch worker processed messages");
                                }
                                Ok(_) => {}
                                Err(error) => {
                                    warn!(worker = %group, error = %error, "Web Push dispatch worker tick failed");
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
