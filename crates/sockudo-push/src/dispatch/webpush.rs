use std::collections::BTreeMap;
use std::sync::Arc;
#[cfg(feature = "push-webpush")]
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
#[cfg(feature = "push-webpush")]
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::future::join_all;
#[cfg(feature = "push-webpush")]
use serde_json::json;
#[cfg(feature = "push-webpush")]
use url::Url;

use super::auth::{CachedTokenProvider, auth_error};
use super::http::{
    ProviderHttpClient, ProviderHttpMethod, ProviderHttpRequest, ProviderHttpResponse,
    validate_webpush_target,
};
use super::outcome::{
    ProviderClassification, classify_http_result, rejected, render_payload_json, result_from_error,
    retryable,
};
use super::{HealthStatus, PushDispatcher};
use crate::domain::{
    DeliveryBatch, DeliveryJob, DeliveryOutcome, DeliveryResult, ProviderError, PushProviderKind,
    PushRecipient, SecretString,
};
use crate::pipeline::now_ms;

#[derive(Clone)]
pub struct WebPushDispatcher {
    token_provider: CachedTokenProvider,
    crypto: Arc<dyn WebPushCrypto + Send + Sync>,
    http: Arc<dyn ProviderHttpClient + Send + Sync>,
}

pub struct WebPushPreparedRequest {
    pub headers: BTreeMap<String, String>,
    pub authorization: Option<SecretString>,
    pub body: Vec<u8>,
}

#[async_trait]
pub trait WebPushCrypto: Send + Sync {
    async fn prepare_request(
        &self,
        endpoint: &str,
        p256dh: &SecretString,
        auth: &SecretString,
        payload: &[u8],
        fallback_bearer: SecretString,
    ) -> Result<WebPushPreparedRequest, ProviderError>;
}

#[derive(Clone)]
pub struct PassthroughWebPushCrypto;

#[async_trait]
impl WebPushCrypto for PassthroughWebPushCrypto {
    async fn prepare_request(
        &self,
        _endpoint: &str,
        _p256dh: &SecretString,
        _auth: &SecretString,
        payload: &[u8],
        fallback_bearer: SecretString,
    ) -> Result<WebPushPreparedRequest, ProviderError> {
        let mut headers = BTreeMap::new();
        headers.insert("content-encoding".to_owned(), "aes128gcm".to_owned());
        headers.insert("ttl".to_owned(), "2419200".to_owned());
        headers.insert("urgency".to_owned(), "normal".to_owned());
        Ok(WebPushPreparedRequest {
            headers,
            authorization: Some(fallback_bearer),
            body: payload.to_vec(),
        })
    }
}

#[cfg(feature = "push-webpush")]
#[derive(Clone)]
pub struct NativeWebPushCrypto {
    vapid_key: Result<Arc<NativeVapidKey>, NativeWebPushKeyError>,
    contact: String,
    valid_for: std::time::Duration,
}

#[cfg(feature = "push-webpush")]
#[derive(Clone)]
struct NativeVapidKey {
    signing_key: web_push_native::p256::ecdsa::SigningKey,
    public_key: String,
}

#[cfg(feature = "push-webpush")]
#[derive(Clone, Copy)]
enum NativeWebPushKeyError {
    Encoding,
    Key,
}

#[cfg(feature = "push-webpush")]
impl NativeWebPushCrypto {
    pub fn new(vapid_private_key: impl Into<String>, contact: impl Into<String>) -> Self {
        let vapid_private_key = vapid_private_key.into();
        let vapid_key = URL_SAFE_NO_PAD
            .decode(vapid_private_key.as_bytes())
            .map_err(|_| NativeWebPushKeyError::Encoding)
            .and_then(|bytes| {
                use web_push_native::p256::{
                    SecretKey, ecdsa::SigningKey, elliptic_curve::sec1::ToEncodedPoint,
                };

                let secret_key =
                    SecretKey::from_slice(&bytes).map_err(|_| NativeWebPushKeyError::Key)?;
                let public_key = URL_SAFE_NO_PAD
                    .encode(secret_key.public_key().to_encoded_point(false).as_bytes());
                Ok(Arc::new(NativeVapidKey {
                    signing_key: SigningKey::from(secret_key),
                    public_key,
                }))
            });
        Self {
            vapid_key,
            contact: contact.into(),
            valid_for: std::time::Duration::from_secs(12 * 60 * 60),
        }
    }

    pub fn with_valid_for(mut self, valid_for: std::time::Duration) -> Self {
        self.valid_for = valid_for;
        self
    }

    fn vapid_authorization(&self, endpoint: &str) -> Result<SecretString, ProviderError> {
        use web_push_native::p256::ecdsa::{Signature, signature::Signer};

        let vapid_key = self.vapid_key.as_ref().map_err(|error| match error {
            NativeWebPushKeyError::Encoding => ProviderError {
                class: "auth_failure".to_owned(),
                reason: Some("invalid VAPID private key encoding".to_owned()),
                retry_after_ms: None,
            },
            NativeWebPushKeyError::Key => ProviderError {
                class: "auth_failure".to_owned(),
                reason: Some("invalid VAPID private key".to_owned()),
                retry_after_ms: None,
            },
        })?;
        let endpoint_uri = Url::parse(endpoint).map_err(|_| ProviderError {
            class: "invalid_token".to_owned(),
            reason: Some("invalid Web Push endpoint".to_owned()),
            retry_after_ms: None,
        })?;
        let scheme = endpoint_uri.scheme();
        if scheme.is_empty() {
            return Err(ProviderError {
                class: "invalid_token".to_owned(),
                reason: Some("invalid Web Push endpoint scheme".to_owned()),
                retry_after_ms: None,
            });
        }
        let host = endpoint_uri.host_str().ok_or_else(|| ProviderError {
            class: "invalid_token".to_owned(),
            reason: Some("invalid Web Push endpoint host".to_owned()),
            retry_after_ms: None,
        })?;
        let exp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ProviderError {
                class: "auth_failure".to_owned(),
                reason: Some("system clock before Unix epoch".to_owned()),
                retry_after_ms: None,
            })?
            .as_secs()
            .saturating_add(self.valid_for.as_secs());
        let header = URL_SAFE_NO_PAD.encode(br#"{"typ":"JWT","alg":"ES256"}"#);
        let claims = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&json!({
                "aud": format!("{scheme}://{host}"),
                "exp": exp,
                "sub": self.contact,
            }))
            .map_err(|_| ProviderError {
                class: "auth_failure".to_owned(),
                reason: Some("failed to serialize VAPID claims".to_owned()),
                retry_after_ms: None,
            })?,
        );
        let signing_input = format!("{header}.{claims}");
        let signature: Signature = vapid_key.signing_key.sign(signing_input.as_bytes());
        let token = format!(
            "{signing_input}.{}",
            URL_SAFE_NO_PAD.encode(signature.to_bytes())
        );
        SecretString::new(format!("vapid t={token}, k={}", vapid_key.public_key)).map_err(|_| {
            ProviderError {
                class: "auth_failure".to_owned(),
                reason: Some("invalid VAPID authorization header".to_owned()),
                retry_after_ms: None,
            }
        })
    }
}

#[cfg(feature = "push-webpush")]
#[async_trait]
impl WebPushCrypto for NativeWebPushCrypto {
    async fn prepare_request(
        &self,
        endpoint: &str,
        p256dh: &SecretString,
        auth: &SecretString,
        payload: &[u8],
        _fallback_bearer: SecretString,
    ) -> Result<WebPushPreparedRequest, ProviderError> {
        use web_push_native::{Auth, WebPushBuilder, p256::PublicKey};

        let p256dh_bytes = URL_SAFE_NO_PAD
            .decode(p256dh.expose_secret().as_bytes())
            .map_err(|_| ProviderError {
                class: "invalid_token".to_owned(),
                reason: Some("invalid Web Push p256dh encoding".to_owned()),
                retry_after_ms: None,
            })?;
        let auth_bytes = URL_SAFE_NO_PAD
            .decode(auth.expose_secret().as_bytes())
            .map_err(|_| ProviderError {
                class: "invalid_token".to_owned(),
                reason: Some("invalid Web Push auth encoding".to_owned()),
                retry_after_ms: None,
            })?;
        if auth_bytes.len() != 16 {
            return Err(ProviderError {
                class: "invalid_token".to_owned(),
                reason: Some("invalid Web Push auth length".to_owned()),
                retry_after_ms: None,
            });
        }

        let request = WebPushBuilder::new(
            endpoint.parse().map_err(|_| ProviderError {
                class: "invalid_token".to_owned(),
                reason: Some("invalid Web Push endpoint".to_owned()),
                retry_after_ms: None,
            })?,
            PublicKey::from_sec1_bytes(&p256dh_bytes).map_err(|_| ProviderError {
                class: "invalid_token".to_owned(),
                reason: Some("invalid Web Push p256dh key".to_owned()),
                retry_after_ms: None,
            })?,
            Auth::clone_from_slice(&auth_bytes),
        )
        .with_valid_duration(self.valid_for)
        .build(payload.to_vec())
        .map_err(|error| ProviderError {
            class: "invalid_payload".to_owned(),
            reason: Some(format!("web push encryption failed: {error}")),
            retry_after_ms: None,
        })?;

        let authorization = Some(self.vapid_authorization(endpoint)?);
        let headers = request
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                let name = name.as_str().to_ascii_lowercase();
                let value = value.to_str().ok()?;
                Some((name, value.to_owned()))
            })
            .collect();

        Ok(WebPushPreparedRequest {
            headers,
            authorization,
            body: request.into_body(),
        })
    }
}

impl WebPushDispatcher {
    pub fn new(
        _vapid_audience: impl Into<String>,
        token_provider: CachedTokenProvider,
        crypto: Arc<dyn WebPushCrypto + Send + Sync>,
        http: Arc<dyn ProviderHttpClient + Send + Sync>,
    ) -> Self {
        Self {
            token_provider,
            crypto,
            http,
        }
    }

    pub async fn build_request(
        &self,
        job: &DeliveryJob,
    ) -> Result<ProviderHttpRequest, ProviderError> {
        let PushRecipient::Web {
            endpoint,
            p256dh,
            auth,
        } = &job.recipient
        else {
            return Err(ProviderError {
                class: "invalid_token".to_owned(),
                reason: Some("web push recipient is missing subscription material".to_owned()),
                retry_after_ms: None,
            });
        };
        let endpoint = endpoint.expose_secret().to_owned();
        validate_webpush_target(&endpoint)?;
        let authorization = self
            .token_provider
            .bearer_token(now_ms())
            .await
            .map_err(auth_error)?;
        let rendered = render_payload_json(PushProviderKind::WebPush, job)?;
        let body = serde_json::to_vec(&rendered).map_err(|_| ProviderError {
            class: "invalid_payload".to_owned(),
            reason: Some("web push payload serialization failed".to_owned()),
            retry_after_ms: None,
        })?;
        let prepared = self
            .crypto
            .prepare_request(&endpoint, p256dh, auth, &body, authorization)
            .await?;
        Ok(ProviderHttpRequest {
            method: ProviderHttpMethod::Post,
            url: endpoint,
            headers: prepared.headers,
            authorization: prepared.authorization,
            body: prepared.body,
        })
    }
}

#[async_trait]
impl PushDispatcher for WebPushDispatcher {
    fn provider(&self) -> PushProviderKind {
        PushProviderKind::WebPush
    }

    async fn dispatch(&self, batch: DeliveryBatch) -> Vec<DeliveryResult> {
        let futures = batch.jobs.into_iter().map(|job| async move {
            let request = match self.build_request(&job).await {
                Ok(request) => request,
                Err(error) => return result_from_error(job, DeliveryOutcome::Rejected, error),
            };
            classify_http_result(
                job,
                self.http.send(request).await,
                classify_webpush_response,
            )
        });
        join_all(futures).await
    }

    async fn health_check(&self) -> HealthStatus {
        HealthStatus {
            provider: PushProviderKind::WebPush,
            healthy: true,
            details: "web push dispatcher configured with external RFC8291 crypto adapter"
                .to_owned(),
        }
    }
}

pub(super) fn classify_webpush_response(response: &ProviderHttpResponse) -> ProviderClassification {
    if matches!(response.status, 200..=202) {
        return (
            DeliveryOutcome::Accepted,
            None,
            response.headers.get("location").cloned(),
        );
    }
    match response.status {
        404 | 410 => rejected("invalid_token", response, None),
        413 => rejected("invalid_payload", response, Some("payload_too_large")),
        429 => retryable("quota", response),
        500..=599 => retryable("unavailable", response),
        401 | 403 => rejected("auth_failure", response, None),
        _ => rejected("provider_rejected", response, None),
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "push-webpush")]
    use super::*;

    #[cfg(feature = "push-webpush")]
    #[tokio::test]
    async fn native_web_push_crypto_encrypts_and_signs_request() {
        use web_push_native::p256::{
            SecretKey,
            elliptic_curve::{rand_core::OsRng, sec1::ToEncodedPoint},
        };

        let ua_secret = SecretKey::random(&mut OsRng);
        let ua_public = ua_secret.public_key();
        let auth_bytes = [7u8; 16];
        let p256dh =
            SecretString::new(URL_SAFE_NO_PAD.encode(ua_public.to_encoded_point(false).as_bytes()))
                .unwrap();
        let auth = SecretString::new(URL_SAFE_NO_PAD.encode(auth_bytes)).unwrap();
        let vapid_private = SecretString::new(URL_SAFE_NO_PAD.encode([1u8; 32])).unwrap();
        let crypto = NativeWebPushCrypto::new(
            vapid_private.expose_secret(),
            "mailto:push-admin@example.com",
        );
        let payload = br#"{"title":"Hello","body":"Body"}"#;

        let prepared = crypto
            .prepare_request(
                "https://push.example/subscription",
                &p256dh,
                &auth,
                payload,
                SecretString::new("unused-bearer").unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(prepared.headers["content-encoding"], "aes128gcm");
        assert_eq!(prepared.headers["content-type"], "application/octet-stream");
        let authorization = prepared.authorization.as_ref().unwrap();
        assert!(authorization.expose_secret().starts_with("vapid t="));
        assert!(!String::from_utf8_lossy(&prepared.body).contains("Hello"));

        let auth = web_push_native::Auth::clone_from_slice(&auth_bytes);
        let decrypted = web_push_native::decrypt(prepared.body, &ua_secret, &auth).unwrap();
        assert_eq!(decrypted, payload);
    }
}
