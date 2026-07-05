use super::super::spawn_supervised_worker;
use crate::push_http::decrypt_credential_secret;
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD as BASE64_STANDARD, URL_SAFE_NO_PAD},
};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use sockudo_core::error::{Error, Result};
use sockudo_core::options::ServerOptions;
use std::{env, fs, sync::Arc};
use tracing::warn;

#[cfg(all(feature = "push", feature = "monolith", feature = "push-apns"))]
pub(in crate::bootstrap::push::workers) fn start_apns_provider_workers(
    config: &ServerOptions,
    store: sockudo_push::DynPushStore,
    queue: sockudo_push::DynPushQueue,
) -> Vec<tokio::task::JoinHandle<()>> {
    let topic = env::var("APNS_TOPIC").or_else(|_| env::var("PUSH_APNS_TOPIC"));
    let Ok(topic) = topic else {
        warn!(
            "push.apns_enabled is true but APNS_TOPIC/PUSH_APNS_TOPIC is not set; APNs dispatch worker not started"
        );
        return Vec::new();
    };
    if topic.trim().is_empty() {
        warn!("APNS_TOPIC/PUSH_APNS_TOPIC is empty; APNs dispatch worker not started");
        return Vec::new();
    }

    let endpoint = env::var("APNS_ENDPOINT")
        .or_else(|_| env::var("PUSH_APNS_ENDPOINT"))
        .unwrap_or_else(|_| "https://api.push.apple.com".to_owned());
    let stored_app_id = env::var("APNS_APP_ID").or_else(|_| env::var("PUSH_APNS_APP_ID"));
    let stored_credential_id = env::var("APNS_CREDENTIAL_ID")
        .or_else(|_| env::var("PUSH_APNS_CREDENTIAL_ID"))
        .unwrap_or_else(|_| "apns".to_owned());

    if let Ok(stored_app_id) = stored_app_id {
        if stored_app_id.trim().is_empty() {
            warn!("APNS_APP_ID/PUSH_APNS_APP_ID is empty; APNs dispatch worker not started");
            return Vec::new();
        }
        let mut handles = Vec::new();
        let max_outbound = config.push.dispatch_max_outbound_requests;
        for worker_index in 0..config.push.dispatch_worker_count {
            let store = store.clone();
            let queue = queue.clone();
            let topic = topic.clone();
            let endpoint = endpoint.clone();
            let app_id = stored_app_id.clone();
            let credential_id = stored_credential_id.clone();
            handles.push(spawn_supervised_worker(
                "provider",
                format!("sockudo-monolith-apns-{worker_index}"),
                move |group| {
                    let store = store.clone();
                    let queue = queue.clone();
                    let topic = topic.clone();
                    let endpoint = endpoint.clone();
                    let app_id = app_id.clone();
                    let credential_id = credential_id.clone();
                    async move {
                        let dispatcher = match create_stored_apns_dispatcher(
                            &store,
                            &app_id,
                            &credential_id,
                            &topic,
                            &endpoint,
                        )
                        .await
                        {
                            Ok(dispatcher) => dispatcher,
                            Err(error) => {
                                warn!(worker = %group, app_id = %app_id, credential_id = %credential_id, error = %error, "APNs dispatch worker not started");
                                return;
                            }
                        };
                        let mut worker = sockudo_push::ProviderDispatchWorker::new(
                            sockudo_push::PushProviderKind::Apns,
                            queue,
                            Arc::new(dispatcher),
                        )
                        .with_max_outbound_requests(max_outbound);
                        warn!(worker = %group, app_id = %app_id, credential_id = %credential_id, "APNs dispatch worker started with stored credential");
                        loop {
                            match worker.run_once(&group).await {
                                Ok(processed) if processed > 0 => {
                                    warn!(worker = %group, processed, "APNs dispatch worker processed messages");
                                }
                                Ok(_) => {}
                                Err(error) => {
                                    warn!(worker = %group, error = %error, "APNs dispatch worker tick failed");
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

    let token_provider = match create_apns_token_provider() {
        Ok(provider) => provider,
        Err(error) => {
            warn!(error = %error, "APNs dispatch worker not started");
            return Vec::new();
        }
    };

    let mut handles = Vec::new();
    let max_outbound = config.push.dispatch_max_outbound_requests;
    for worker_index in 0..config.push.dispatch_worker_count {
        let http = match sockudo_push::ReqwestProviderHttpClient::new() {
            Ok(http) => Arc::new(http),
            Err(error) => {
                warn!(error = %error, "failed to create APNs HTTP client");
                continue;
            }
        };
        let dispatcher =
            sockudo_push::ApnsDispatcher::new(topic.clone(), token_provider.clone(), http)
                .with_base_url(endpoint.clone());
        handles.push(spawn_supervised_worker(
            "provider",
            format!("sockudo-monolith-apns-{worker_index}"),
            {
                let queue = queue.clone();
                move |group| {
                    let queue = queue.clone();
                    let dispatcher = dispatcher.clone();
                    async move {
                        let mut worker = sockudo_push::ProviderDispatchWorker::new(
                            sockudo_push::PushProviderKind::Apns,
                            queue,
                            Arc::new(dispatcher),
                        )
                        .with_max_outbound_requests(max_outbound);
                        warn!(worker = %group, "APNs dispatch worker started");
                        loop {
                            match worker.run_once(&group).await {
                                Ok(processed) if processed > 0 => {
                                    warn!(worker = %group, processed, "APNs dispatch worker processed messages");
                                }
                                Ok(_) => {}
                                Err(error) => {
                                    warn!(worker = %group, error = %error, "APNs dispatch worker tick failed");
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

#[cfg(all(feature = "push", feature = "monolith", feature = "push-apns"))]
async fn create_stored_apns_dispatcher(
    store: &sockudo_push::DynPushStore,
    app_id: &str,
    credential_id: &str,
    topic: &str,
    endpoint: &str,
) -> Result<sockudo_push::ApnsDispatcher> {
    let credential = store
        .get_credential(app_id, credential_id)
        .await
        .map_err(|error| Error::Internal(format!("failed to load APNs credential: {error}")))?
        .ok_or_else(|| {
            Error::Internal(format!(
                "APNs credential {credential_id:?} was not found for app {app_id:?}"
            ))
        })?;
    if credential.provider != sockudo_push::PushProviderKind::Apns {
        return Err(Error::Internal(format!(
            "stored credential {credential_id:?} for app {app_id:?} is {:?}, not APNs",
            credential.provider
        )));
    }

    let sockudo_push::ProviderCredentialMaterial::Apns {
        p12,
        p12_password,
        pem,
        team_id,
        key_id,
        private_key,
    } = credential.material
    else {
        return Err(Error::Internal(
            "stored credential material is not APNs".to_owned(),
        ));
    };

    if let (Some(team_id), Some(key_id), Some(private_key)) = (team_id, key_id, private_key) {
        let private_key = decrypt_credential_secret(&private_key)
            .map_err(|error| {
                Error::Internal(format!("failed to decrypt APNs private key: {error}"))
            })?
            .replace("\\n", "\n");
        let token_provider = sockudo_push::CachedTokenProvider::new(Arc::new(
            ApnsJwtTokenSource::new(team_id, key_id, private_key)?,
        ));
        let http = Arc::new(
            sockudo_push::ReqwestProviderHttpClient::new().map_err(|error| {
                Error::Internal(format!("failed to create APNs HTTP client: {error}"))
            })?,
        );
        return Ok(
            sockudo_push::ApnsDispatcher::new(topic.to_owned(), token_provider, http)
                .with_base_url(endpoint.to_owned()),
        );
    }

    if let Some(pem) = pem {
        let pem = decrypt_credential_secret(&pem)
            .map_err(|error| Error::Internal(format!("failed to decrypt APNs PEM: {error}")))?;
        let http = Arc::new(
            sockudo_push::ReqwestProviderHttpClient::new_with_pem_identity(&pem).map_err(
                |error| Error::Internal(format!("failed to create APNs PEM HTTP client: {error}")),
            )?,
        );
        return Ok(
            sockudo_push::ApnsDispatcher::new_with_tls_identity(topic.to_owned(), http)
                .with_base_url(endpoint.to_owned()),
        );
    }

    if let Some(p12) = p12 {
        let p12 = decrypt_credential_secret(&p12)
            .map_err(|error| Error::Internal(format!("failed to decrypt APNs p12: {error}")))?;
        let p12_password = p12_password
            .as_ref()
            .map(decrypt_credential_secret)
            .transpose()
            .map_err(|error| {
                Error::Internal(format!("failed to decrypt APNs p12 password: {error}"))
            })?
            .unwrap_or_default();
        let der = decode_apns_p12(&p12)?;
        let http = Arc::new(
            sockudo_push::ReqwestProviderHttpClient::new_with_pkcs12_identity(&der, &p12_password)
                .map_err(|error| {
                    Error::Internal(format!(
                        "failed to create APNs PKCS#12 HTTP client: {error}"
                    ))
                })?,
        );
        return Ok(
            sockudo_push::ApnsDispatcher::new_with_tls_identity(topic.to_owned(), http)
                .with_base_url(endpoint.to_owned()),
        );
    }

    Err(Error::Internal(
        "APNs credential requires p12, pem, or teamId/keyId/privateKey material".to_owned(),
    ))
}

#[cfg(all(feature = "push", feature = "monolith", feature = "push-apns"))]
fn decode_apns_p12(value: &str) -> Result<Vec<u8>> {
    let compact = value.lines().map(str::trim).collect::<Vec<_>>().join("");
    BASE64_STANDARD
        .decode(&compact)
        .or_else(|_| URL_SAFE_NO_PAD.decode(&compact))
        .map_err(|error| Error::Internal(format!("APNs p12 must be base64-encoded DER: {error}")))
}

#[cfg(all(feature = "push", feature = "monolith", feature = "push-apns"))]
fn create_apns_token_provider() -> Result<sockudo_push::CachedTokenProvider> {
    if let Ok(token) =
        env::var("APNS_PROVIDER_TOKEN").or_else(|_| env::var("PUSH_APNS_PROVIDER_TOKEN"))
    {
        if token.trim().is_empty() {
            return Err(Error::Internal(
                "APNS_PROVIDER_TOKEN/PUSH_APNS_PROVIDER_TOKEN is empty".to_owned(),
            ));
        }
        return Ok(sockudo_push::CachedTokenProvider::new(Arc::new(
            sockudo_push::StaticTokenSource::new(
                sockudo_push::SecretString::new(token).map_err(|error| {
                    Error::Internal(format!("invalid APNs provider token: {error}"))
                })?,
                u64::MAX,
            ),
        )));
    }

    let team_id = env::var("APNS_TEAM_ID").or_else(|_| env::var("PUSH_APNS_TEAM_ID"));
    let key_id = env::var("APNS_KEY_ID").or_else(|_| env::var("PUSH_APNS_KEY_ID"));
    let private_key = env::var("APNS_PRIVATE_KEY").or_else(|_| env::var("PUSH_APNS_PRIVATE_KEY"));
    let private_key_path =
        env::var("APNS_PRIVATE_KEY_PATH").or_else(|_| env::var("PUSH_APNS_PRIVATE_KEY_PATH"));

    let team_id = team_id.map_err(|_| {
        Error::Internal(
            "APNs token auth requires APNS_TEAM_ID/PUSH_APNS_TEAM_ID or APNS_PROVIDER_TOKEN"
                .to_owned(),
        )
    })?;
    let key_id = key_id.map_err(|_| {
        Error::Internal(
            "APNs token auth requires APNS_KEY_ID/PUSH_APNS_KEY_ID or APNS_PROVIDER_TOKEN"
                .to_owned(),
        )
    })?;
    let private_key = match (private_key, private_key_path) {
        (Ok(value), _) => value,
        (Err(_), Ok(path)) => fs::read_to_string(path).map_err(|error| {
            Error::Internal(format!("failed to read APNs private key: {error}"))
        })?,
        (Err(_), Err(_)) => {
            return Err(Error::Internal(
                "APNs token auth requires APNS_PRIVATE_KEY/PUSH_APNS_PRIVATE_KEY, APNS_PRIVATE_KEY_PATH/PUSH_APNS_PRIVATE_KEY_PATH, or APNS_PROVIDER_TOKEN".to_owned(),
            ));
        }
    };
    let private_key = private_key.replace("\\n", "\n");

    Ok(sockudo_push::CachedTokenProvider::new(Arc::new(
        ApnsJwtTokenSource::new(team_id, key_id, private_key)?,
    )))
}

#[cfg(all(feature = "push", feature = "monolith", feature = "push-apns"))]
struct ApnsJwtTokenSource {
    team_id: String,
    key_id: String,
    encoding_key: EncodingKey,
}

#[cfg(all(feature = "push", feature = "monolith", feature = "push-apns"))]
impl ApnsJwtTokenSource {
    fn new(team_id: String, key_id: String, private_key: String) -> Result<Self> {
        if team_id.trim().is_empty() {
            return Err(Error::Internal("APNS_TEAM_ID is empty".to_owned()));
        }
        if key_id.trim().is_empty() {
            return Err(Error::Internal("APNS_KEY_ID is empty".to_owned()));
        }
        let encoding_key = EncodingKey::from_ec_pem(private_key.as_bytes())
            .map_err(|error| Error::Internal(format!("invalid APNs .p8 private key: {error}")))?;
        Ok(Self {
            team_id,
            key_id,
            encoding_key,
        })
    }
}

#[cfg(all(feature = "push", feature = "monolith", feature = "push-apns"))]
#[async_trait::async_trait]
impl sockudo_push::ProviderTokenSource for ApnsJwtTokenSource {
    async fn fetch_token(
        &self,
        now_ms: u64,
    ) -> std::result::Result<sockudo_push::ProviderAccessToken, sockudo_push::ProviderAuthError>
    {
        #[derive(serde::Serialize)]
        struct Claims<'a> {
            iss: &'a str,
            iat: u64,
        }

        let issued_at_secs = now_ms / 1_000;
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(self.key_id.clone());
        let _ = jsonwebtoken::crypto::aws_lc::DEFAULT_PROVIDER.install_default();
        let token = jsonwebtoken::encode(
            &header,
            &Claims {
                iss: &self.team_id,
                iat: issued_at_secs,
            },
            &self.encoding_key,
        )
        .map_err(|error| sockudo_push::ProviderAuthError {
            class: "auth_failure",
            reason: format!("failed to sign APNs provider token: {error}"),
        })?;

        let token = sockudo_push::SecretString::new(token).map_err(|error| {
            sockudo_push::ProviderAuthError {
                class: "auth_failure",
                reason: error.to_string(),
            }
        })?;

        Ok(sockudo_push::ProviderAccessToken {
            token,
            expires_at_ms: now_ms.saturating_add(55 * 60 * 1_000),
        })
    }
}
