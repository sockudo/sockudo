use std::{env, fs, sync::Arc};

use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD as BASE64_STANDARD, URL_SAFE_NO_PAD},
};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use sockudo_core::error::{Error, Result};
use sockudo_core::options::{PushApnsConfig, ServerOptions};

use crate::push_http::decrypt_credential_secret;

pub(crate) async fn create_apns_channel_manager(
    config: &ServerOptions,
    store: &sockudo_push::DynPushStore,
) -> Result<Option<Arc<sockudo_push::ApnsChannelManager>>> {
    if !config.push.apns_enabled || !config.push.apns.broadcast_enabled {
        return Ok(None);
    }
    let manager =
        if let Ok(app_id) = env::var("APNS_APP_ID").or_else(|_| env::var("PUSH_APNS_APP_ID")) {
            let credential_id = env::var("APNS_CREDENTIAL_ID")
                .or_else(|_| env::var("PUSH_APNS_CREDENTIAL_ID"))
                .unwrap_or_else(|_| "apns".to_owned());
            manager_from_store(store, &app_id, &credential_id, &config.push.apns).await?
        } else {
            let http = Arc::new(
                sockudo_push::ReqwestProviderHttpClient::new_for_trusted_provider(http_options(
                    &config.push.apns,
                ))
                .map_err(|error| {
                    Error::Internal(format!("failed to create APNs HTTP client: {error}"))
                })?,
            );
            sockudo_push::ApnsChannelManager::new(
                config.push.apns.resolved_bundle_id(),
                token_provider_from_env()?,
                http,
            )
            .map_err(|error| Error::Internal(error.to_string()))?
        };
    let manager = manager.with_base_url(config.push.apns.management_endpoint.clone());
    Ok(Some(Arc::new(manager)))
}

async fn manager_from_store(
    store: &sockudo_push::DynPushStore,
    app_id: &str,
    credential_id: &str,
    config: &PushApnsConfig,
) -> Result<sockudo_push::ApnsChannelManager> {
    let credential = store
        .get_credential(app_id, credential_id)
        .await
        .map_err(|error| Error::Internal(format!("failed to load APNs credential: {error}")))?
        .ok_or_else(|| Error::Internal("configured APNs credential was not found".to_owned()))?;
    if credential.provider != sockudo_push::PushProviderKind::Apns {
        return Err(Error::Internal(
            "configured channel management credential is not APNs".to_owned(),
        ));
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
        let http = Arc::new(
            sockudo_push::ReqwestProviderHttpClient::new_for_trusted_provider(http_options(config))
                .map_err(|error| {
                    Error::Internal(format!("failed to create APNs HTTP client: {error}"))
                })?,
        );
        return sockudo_push::ApnsChannelManager::new(
            config.resolved_bundle_id(),
            jwt_token_provider(team_id, key_id, private_key)?,
            http,
        )
        .map_err(|error| Error::Internal(error.to_string()));
    }
    if let Some(pem) = pem {
        let pem = decrypt_credential_secret(&pem)
            .map_err(|error| Error::Internal(format!("failed to decrypt APNs PEM: {error}")))?;
        let http = Arc::new(
            sockudo_push::ReqwestProviderHttpClient::new_with_pem_identity_and_options(
                &pem,
                http_options(config),
            )
            .map_err(|error| {
                Error::Internal(format!("failed to create APNs PEM HTTP client: {error}"))
            })?,
        );
        return sockudo_push::ApnsChannelManager::new_with_tls_identity(
            config.resolved_bundle_id(),
            http,
        )
        .map_err(|error| Error::Internal(error.to_string()));
    }
    if let Some(p12) = p12 {
        let p12 = decrypt_credential_secret(&p12)
            .map_err(|error| Error::Internal(format!("failed to decrypt APNs p12: {error}")))?;
        let password = p12_password
            .as_ref()
            .map(decrypt_credential_secret)
            .transpose()
            .map_err(|error| {
                Error::Internal(format!("failed to decrypt APNs p12 password: {error}"))
            })?
            .unwrap_or_default();
        let der = decode_p12(&p12)?;
        let http = Arc::new(
            sockudo_push::ReqwestProviderHttpClient::new_with_pkcs12_identity_and_options(
                &der,
                &password,
                http_options(config),
            )
            .map_err(|error| {
                Error::Internal(format!(
                    "failed to create APNs PKCS#12 HTTP client: {error}"
                ))
            })?,
        );
        return sockudo_push::ApnsChannelManager::new_with_tls_identity(
            config.resolved_bundle_id(),
            http,
        )
        .map_err(|error| Error::Internal(error.to_string()));
    }
    Err(Error::Internal(
        "APNs credential requires p12, pem, or teamId/keyId/privateKey material".to_owned(),
    ))
}

fn decode_p12(value: &str) -> Result<Vec<u8>> {
    let compact = value.lines().map(str::trim).collect::<Vec<_>>().join("");
    BASE64_STANDARD
        .decode(&compact)
        .or_else(|_| URL_SAFE_NO_PAD.decode(&compact))
        .map_err(|error| Error::Internal(format!("APNs p12 must be base64-encoded DER: {error}")))
}

fn token_provider_from_env() -> Result<sockudo_push::CachedTokenProvider> {
    if let Ok(token) =
        env::var("APNS_PROVIDER_TOKEN").or_else(|_| env::var("PUSH_APNS_PROVIDER_TOKEN"))
    {
        return Ok(sockudo_push::CachedTokenProvider::new(Arc::new(
            sockudo_push::StaticTokenSource::new(
                sockudo_push::SecretString::new(token).map_err(|error| {
                    Error::Internal(format!("invalid APNs provider token: {error}"))
                })?,
                u64::MAX,
            ),
        )));
    }
    let team_id = env::var("APNS_TEAM_ID")
        .or_else(|_| env::var("PUSH_APNS_TEAM_ID"))
        .map_err(|_| Error::Internal("APNs broadcast requires APNS_TEAM_ID".to_owned()))?;
    let key_id = env::var("APNS_KEY_ID")
        .or_else(|_| env::var("PUSH_APNS_KEY_ID"))
        .map_err(|_| Error::Internal("APNs broadcast requires APNS_KEY_ID".to_owned()))?;
    let private_key = env::var("APNS_PRIVATE_KEY")
        .or_else(|_| env::var("PUSH_APNS_PRIVATE_KEY"))
        .or_else(|_| {
            env::var("APNS_PRIVATE_KEY_PATH")
                .or_else(|_| env::var("PUSH_APNS_PRIVATE_KEY_PATH"))
                .and_then(|path| fs::read_to_string(path).map_err(|_| env::VarError::NotPresent))
        })
        .map_err(|_| Error::Internal("APNs broadcast requires an APNs private key".to_owned()))?
        .replace("\\n", "\n");
    jwt_token_provider(team_id, key_id, private_key)
}

fn jwt_token_provider(
    team_id: String,
    key_id: String,
    private_key: String,
) -> Result<sockudo_push::CachedTokenProvider> {
    Ok(sockudo_push::CachedTokenProvider::new(Arc::new(
        ApnsJwtTokenSource::new(team_id, key_id, private_key)?,
    )))
}

struct ApnsJwtTokenSource {
    team_id: String,
    key_id: String,
    encoding_key: EncodingKey,
}

impl ApnsJwtTokenSource {
    fn new(team_id: String, key_id: String, private_key: String) -> Result<Self> {
        let encoding_key = EncodingKey::from_ec_pem(private_key.as_bytes())
            .map_err(|error| Error::Internal(format!("invalid APNs .p8 private key: {error}")))?;
        Ok(Self {
            team_id,
            key_id,
            encoding_key,
        })
    }
}

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
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(self.key_id.clone());
        let _ = jsonwebtoken::crypto::aws_lc::DEFAULT_PROVIDER.install_default();
        let token = jsonwebtoken::encode(
            &header,
            &Claims {
                iss: &self.team_id,
                iat: now_ms / 1_000,
            },
            &self.encoding_key,
        )
        .map_err(|error| sockudo_push::ProviderAuthError {
            class: "auth_failure",
            reason: format!("failed to sign APNs provider token: {error}"),
        })?;
        Ok(sockudo_push::ProviderAccessToken {
            token: sockudo_push::SecretString::new(token).map_err(|error| {
                sockudo_push::ProviderAuthError {
                    class: "auth_failure",
                    reason: error.to_string(),
                }
            })?,
            expires_at_ms: now_ms.saturating_add(55 * 60 * 1_000),
        })
    }
}

fn http_options(config: &PushApnsConfig) -> sockudo_push::ProviderHttpClientOptions {
    sockudo_push::ProviderHttpClientOptions {
        connect_timeout_ms: config.connect_timeout_ms,
        request_timeout_ms: config.request_timeout_ms,
        pool_idle_timeout_secs: config.pool_idle_timeout_secs,
        max_idle_connections_per_host: config.max_idle_connections_per_host,
        tcp_keepalive_secs: config.tcp_keepalive_secs,
        http2_keepalive_interval_secs: config.http2_keepalive_interval_secs,
        http2_keepalive_timeout_secs: config.http2_keepalive_timeout_secs,
    }
}
