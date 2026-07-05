use super::super::spawn_supervised_worker;
use super::static_push_token_provider;
use crate::push_http::decrypt_credential_secret;
use sockudo_core::error::{Error, Result};
use sockudo_core::options::ServerOptions;
use std::{env, fs, sync::Arc};
use tracing::warn;

#[cfg(all(feature = "push", feature = "monolith", feature = "push-fcm"))]
pub(in crate::bootstrap::push::workers) fn start_fcm_provider_workers(
    config: &ServerOptions,
    store: sockudo_push::DynPushStore,
    queue: sockudo_push::DynPushQueue,
) -> Vec<tokio::task::JoinHandle<()>> {
    let configured_project_id = match optional_env("FCM_PROJECT_ID", "PUSH_FCM_PROJECT_ID") {
        Ok(value) => value,
        Err(error) => {
            warn!(error = %error, "FCM dispatch worker not started");
            return Vec::new();
        }
    };
    let endpoint = env::var("FCM_ENDPOINT")
        .or_else(|_| env::var("PUSH_FCM_ENDPOINT"))
        .ok();
    let stored_app_id = env::var("FCM_APP_ID").or_else(|_| env::var("PUSH_FCM_APP_ID"));
    let stored_credential_id = env::var("FCM_CREDENTIAL_ID")
        .or_else(|_| env::var("PUSH_FCM_CREDENTIAL_ID"))
        .unwrap_or_else(|_| "fcm".to_owned());

    if let Ok(stored_app_id) = stored_app_id {
        if stored_app_id.trim().is_empty() {
            warn!("FCM_APP_ID/PUSH_FCM_APP_ID is empty; FCM dispatch worker not started");
            return Vec::new();
        }
        let mut handles = Vec::new();
        let max_outbound = config.push.dispatch_max_outbound_requests;
        for worker_index in 0..config.push.dispatch_worker_count {
            let store = store.clone();
            let queue = queue.clone();
            let endpoint = endpoint.clone();
            let app_id = stored_app_id.clone();
            let credential_id = stored_credential_id.clone();
            let configured_project_id = configured_project_id.clone();
            handles.push(spawn_supervised_worker(
                "provider",
                format!("sockudo-monolith-fcm-{worker_index}"),
                move |group| {
                    let store = store.clone();
                    let queue = queue.clone();
                    let endpoint = endpoint.clone();
                    let app_id = app_id.clone();
                    let credential_id = credential_id.clone();
                    let configured_project_id = configured_project_id.clone();
                    async move {
                        let dispatcher = match create_stored_fcm_dispatcher(
                            &store,
                            &app_id,
                            &credential_id,
                            configured_project_id.as_deref(),
                            endpoint.as_deref(),
                        )
                        .await
                        {
                            Ok(dispatcher) => dispatcher,
                            Err(error) => {
                                warn!(worker = %group, app_id = %app_id, credential_id = %credential_id, error = %error, "FCM dispatch worker not started");
                                return;
                            }
                        };
                        let mut worker = sockudo_push::ProviderDispatchWorker::new(
                            sockudo_push::PushProviderKind::Fcm,
                            queue,
                            Arc::new(dispatcher),
                        )
                        .with_max_outbound_requests(max_outbound);
                        warn!(worker = %group, app_id = %app_id, credential_id = %credential_id, "FCM dispatch worker started with stored credential");
                        loop {
                            match worker.run_once(&group).await {
                                Ok(processed) if processed > 0 => {
                                    warn!(worker = %group, processed, "FCM dispatch worker processed messages");
                                }
                                Ok(_) => {}
                                Err(error) => {
                                    warn!(worker = %group, error = %error, "FCM dispatch worker tick failed");
                                }
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                        }
                    }
                },
            ));
        }
        return handles;
    }

    let (token_provider, service_account_project_id) = match create_fcm_token_provider() {
        Ok(provider) => provider,
        Err(error) => {
            warn!(error = %error, "FCM dispatch worker not started");
            return Vec::new();
        }
    };
    let project_id = match configured_project_id.or(service_account_project_id) {
        Some(project_id) => project_id,
        None => {
            warn!(
                "FCM dispatch requires FCM_PROJECT_ID/PUSH_FCM_PROJECT_ID or project_id in the FCM service account JSON; FCM dispatch worker not started"
            );
            return Vec::new();
        }
    };

    let mut handles = Vec::new();
    let max_outbound = config.push.dispatch_max_outbound_requests;
    for worker_index in 0..config.push.dispatch_worker_count {
        let http = match sockudo_push::ReqwestProviderHttpClient::new() {
            Ok(http) => Arc::new(http),
            Err(error) => {
                warn!(error = %error, "failed to create FCM HTTP client");
                continue;
            }
        };
        let mut dispatcher =
            sockudo_push::FcmDispatcher::new(project_id.clone(), token_provider.clone(), http);
        if let Some(endpoint) = endpoint.clone() {
            dispatcher = dispatcher.with_base_url(endpoint);
        }
        handles.push(spawn_supervised_worker(
            "provider",
            format!("sockudo-monolith-fcm-{worker_index}"),
            {
                let queue = queue.clone();
                move |group| {
                    let queue = queue.clone();
                    let dispatcher = dispatcher.clone();
                    async move {
                        let mut worker = sockudo_push::ProviderDispatchWorker::new(
                            sockudo_push::PushProviderKind::Fcm,
                            queue,
                            Arc::new(dispatcher),
                        )
                        .with_max_outbound_requests(max_outbound);
                        warn!(worker = %group, "FCM dispatch worker started");
                        loop {
                            match worker.run_once(&group).await {
                                Ok(processed) if processed > 0 => {
                                    warn!(worker = %group, processed, "FCM dispatch worker processed messages");
                                }
                                Ok(_) => {}
                                Err(error) => {
                                    warn!(worker = %group, error = %error, "FCM dispatch worker tick failed");
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

#[cfg(all(feature = "push", feature = "monolith", feature = "push-fcm"))]
fn optional_env(primary: &str, alias: &str) -> Result<Option<String>> {
    match env::var(primary).or_else(|_| env::var(alias)) {
        Ok(value) if value.trim().is_empty() => Err(Error::Internal(format!(
            "{primary}/{alias} is set but empty"
        ))),
        Ok(value) => Ok(Some(value)),
        Err(_) => Ok(None),
    }
}

#[cfg(all(feature = "push", feature = "monolith", feature = "push-fcm"))]
fn fcm_service_account_json_from_env() -> Result<Option<String>> {
    if let Some(value) = optional_env("FCM_SERVICE_ACCOUNT_JSON", "PUSH_FCM_SERVICE_ACCOUNT_JSON")?
    {
        return Ok(Some(value));
    }

    let path = optional_env(
        "FCM_SERVICE_ACCOUNT_JSON_PATH",
        "PUSH_FCM_SERVICE_ACCOUNT_JSON_PATH",
    )?
    .or_else(|| env::var("GOOGLE_APPLICATION_CREDENTIALS").ok());

    let Some(path) = path else {
        return Ok(None);
    };
    if path.trim().is_empty() {
        return Err(Error::Internal(
            "GOOGLE_APPLICATION_CREDENTIALS is set but empty".to_owned(),
        ));
    }
    fs::read_to_string(&path).map(Some).map_err(|error| {
        Error::Internal(format!(
            "failed to read FCM service account JSON from {path:?}: {error}"
        ))
    })
}

#[cfg(all(feature = "push", feature = "monolith", feature = "push-fcm"))]
fn create_fcm_token_provider() -> Result<(sockudo_push::CachedTokenProvider, Option<String>)> {
    if let Some(service_account_json) = fcm_service_account_json_from_env()? {
        return create_fcm_service_account_token_provider(&service_account_json);
    }

    let provider =
        static_push_token_provider("FCM", &["FCM_PROVIDER_TOKEN", "PUSH_FCM_PROVIDER_TOKEN"])?;
    warn!(
        "FCM_PROVIDER_TOKEN/PUSH_FCM_PROVIDER_TOKEN is a static bearer token and cannot be refreshed; use FCM_SERVICE_ACCOUNT_JSON_PATH or PUSH_FCM_SERVICE_ACCOUNT_JSON_PATH for long-running workers"
    );
    Ok((provider, None))
}

#[cfg(all(feature = "push", feature = "monolith", feature = "push-fcm"))]
fn create_fcm_service_account_token_provider(
    service_account_json: &str,
) -> Result<(sockudo_push::CachedTokenProvider, Option<String>)> {
    let http = Arc::new(
        sockudo_push::ReqwestProviderHttpClient::new().map_err(|error| {
            Error::Internal(format!("failed to create FCM OAuth HTTP client: {error}"))
        })?,
    );
    let source = sockudo_push::FcmServiceAccountTokenSource::from_json(service_account_json, http)
        .map_err(|error| {
            Error::Internal(format!("invalid FCM service account: {}", error.reason))
        })?;
    let project_id = source.project_id().map(str::to_owned);
    Ok((
        sockudo_push::CachedTokenProvider::new(Arc::new(source)),
        project_id,
    ))
}

#[cfg(all(feature = "push", feature = "monolith", feature = "push-fcm"))]
async fn create_stored_fcm_dispatcher(
    store: &sockudo_push::DynPushStore,
    app_id: &str,
    credential_id: &str,
    configured_project_id: Option<&str>,
    endpoint: Option<&str>,
) -> Result<sockudo_push::FcmDispatcher> {
    let credential = store
        .get_credential(app_id, credential_id)
        .await
        .map_err(|error| Error::Internal(format!("failed to load FCM credential: {error}")))?
        .ok_or_else(|| {
            Error::Internal(format!(
                "FCM credential {credential_id:?} was not found for app {app_id:?}"
            ))
        })?;
    if credential.provider != sockudo_push::PushProviderKind::Fcm {
        return Err(Error::Internal(format!(
            "stored credential {credential_id:?} for app {app_id:?} is {:?}, not FCM",
            credential.provider
        )));
    }

    let sockudo_push::ProviderCredentialMaterial::Fcm {
        service_account_json,
    } = credential.material
    else {
        return Err(Error::Internal(
            "stored credential material is not FCM".to_owned(),
        ));
    };

    let service_account_json =
        decrypt_credential_secret(&service_account_json).map_err(|error| {
            Error::Internal(format!(
                "failed to decrypt FCM service account JSON: {error}"
            ))
        })?;
    let http = Arc::new(
        sockudo_push::ReqwestProviderHttpClient::new().map_err(|error| {
            Error::Internal(format!("failed to create FCM HTTP client: {error}"))
        })?,
    );
    let source =
        sockudo_push::FcmServiceAccountTokenSource::from_json(&service_account_json, http.clone())
            .map_err(|error| {
                Error::Internal(format!("invalid FCM service account: {}", error.reason))
            })?;
    let project_id = configured_project_id
        .map(str::to_owned)
        .or_else(|| source.project_id().map(str::to_owned))
        .ok_or_else(|| {
            Error::Internal(
                "FCM credential requires project_id or FCM_PROJECT_ID/PUSH_FCM_PROJECT_ID"
                    .to_owned(),
            )
        })?;
    let token_provider = sockudo_push::CachedTokenProvider::new(Arc::new(source));
    let mut dispatcher = sockudo_push::FcmDispatcher::new(project_id, token_provider, http);
    if let Some(endpoint) = endpoint {
        dispatcher = dispatcher.with_base_url(endpoint.to_owned());
    }
    Ok(dispatcher)
}
