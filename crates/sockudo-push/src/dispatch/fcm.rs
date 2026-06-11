use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::future::join_all;
#[cfg(feature = "push-fcm")]
use jsonwebtoken::{Algorithm, EncodingKey, Header};
#[cfg(feature = "push-fcm")]
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::auth::{CachedTokenProvider, auth_error};
#[cfg(feature = "push-fcm")]
use super::auth::{ProviderAccessToken, ProviderAuthError, ProviderTokenSource};
#[cfg(feature = "push-fcm")]
use super::http::ProviderHttpMethod;
use super::http::{
    ProviderEndpointConfig, ProviderHttpClient, ProviderHttpRequest, ProviderHttpResponse,
};
use super::outcome::{
    ProviderClassification, classify_http_result, json_field, json_request, recipient_token,
    rejected, render_payload_json, result_from_error, retryable,
};
use super::{HealthStatus, PushDispatcher};
#[cfg(feature = "push-fcm")]
use crate::domain::SecretString;
use crate::domain::{
    DeliveryBatch, DeliveryJob, DeliveryOutcome, DeliveryResult, ProviderError, PushProviderKind,
};
use crate::pipeline::now_ms;

#[cfg(feature = "push-fcm")]
const FCM_MESSAGING_SCOPE: &str = "https://www.googleapis.com/auth/firebase.messaging";

#[cfg(feature = "push-fcm")]
const GOOGLE_OAUTH_JWT_BEARER_GRANT: &str = "urn:ietf:params:oauth:grant-type:jwt-bearer";

#[cfg(feature = "push-fcm")]
#[derive(Clone)]
pub struct FcmServiceAccountTokenSource {
    client_email: String,
    private_key_id: Option<String>,
    encoding_key: EncodingKey,
    token_uri: String,
    project_id: Option<String>,
    http: Arc<dyn ProviderHttpClient + Send + Sync>,
}

#[cfg(feature = "push-fcm")]
#[derive(Debug, Deserialize)]
struct FcmServiceAccountJson {
    client_email: String,
    private_key: String,
    #[serde(default)]
    private_key_id: Option<String>,
    #[serde(default)]
    token_uri: Option<String>,
    #[serde(default)]
    project_id: Option<String>,
}

#[cfg(feature = "push-fcm")]
#[derive(Debug, Deserialize)]
struct GoogleAccessTokenResponse {
    access_token: String,
    expires_in: u64,
}

#[cfg(feature = "push-fcm")]
impl FcmServiceAccountTokenSource {
    pub fn from_json(
        service_account_json: &str,
        http: Arc<dyn ProviderHttpClient + Send + Sync>,
    ) -> Result<Self, ProviderAuthError> {
        let credential: FcmServiceAccountJson = serde_json::from_str(service_account_json)
            .map_err(|error| ProviderAuthError {
                class: "auth_failure",
                reason: format!("invalid FCM service account JSON: {error}"),
            })?;
        Self::new(
            credential.client_email,
            credential.private_key,
            credential.private_key_id,
            credential
                .token_uri
                .unwrap_or_else(|| "https://oauth2.googleapis.com/token".to_owned()),
            credential.project_id,
            http,
        )
    }

    pub fn new(
        client_email: String,
        private_key: String,
        private_key_id: Option<String>,
        token_uri: String,
        project_id: Option<String>,
        http: Arc<dyn ProviderHttpClient + Send + Sync>,
    ) -> Result<Self, ProviderAuthError> {
        if client_email.trim().is_empty() {
            return Err(ProviderAuthError {
                class: "auth_failure",
                reason: "FCM service account client_email is empty".to_owned(),
            });
        }
        if token_uri.trim().is_empty() {
            return Err(ProviderAuthError {
                class: "auth_failure",
                reason: "FCM service account token_uri is empty".to_owned(),
            });
        }
        let private_key = private_key.replace("\\n", "\n");
        let encoding_key = EncodingKey::from_rsa_pem(private_key.as_bytes()).map_err(|error| {
            ProviderAuthError {
                class: "auth_failure",
                reason: format!("invalid FCM service account private key: {error}"),
            }
        })?;

        Ok(Self {
            client_email,
            private_key_id: private_key_id.filter(|value| !value.trim().is_empty()),
            encoding_key,
            token_uri,
            project_id: project_id.filter(|value| !value.trim().is_empty()),
            http,
        })
    }

    pub fn project_id(&self) -> Option<&str> {
        self.project_id.as_deref()
    }

    fn signed_assertion(&self, now_ms: u64) -> Result<String, ProviderAuthError> {
        #[derive(Serialize)]
        struct Claims<'a> {
            iss: &'a str,
            scope: &'a str,
            aud: &'a str,
            iat: u64,
            exp: u64,
        }

        let issued_at_secs = now_ms / 1_000;
        let mut header = Header::new(Algorithm::RS256);
        header.kid.clone_from(&self.private_key_id);
        let _ = jsonwebtoken::crypto::aws_lc::DEFAULT_PROVIDER.install_default();
        jsonwebtoken::encode(
            &header,
            &Claims {
                iss: &self.client_email,
                scope: FCM_MESSAGING_SCOPE,
                aud: &self.token_uri,
                iat: issued_at_secs,
                exp: issued_at_secs.saturating_add(3_600),
            },
            &self.encoding_key,
        )
        .map_err(|error| ProviderAuthError {
            class: "auth_failure",
            reason: format!("failed to sign FCM service account assertion: {error}"),
        })
    }
}

#[cfg(feature = "push-fcm")]
#[async_trait]
impl ProviderTokenSource for FcmServiceAccountTokenSource {
    async fn fetch_token(&self, now_ms: u64) -> Result<ProviderAccessToken, ProviderAuthError> {
        let assertion = self.signed_assertion(now_ms)?;
        let body = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("grant_type", GOOGLE_OAUTH_JWT_BEARER_GRANT)
            .append_pair("assertion", &assertion)
            .finish()
            .into_bytes();

        let response = self
            .http
            .send(ProviderHttpRequest {
                method: ProviderHttpMethod::Post,
                url: self.token_uri.clone(),
                headers: BTreeMap::from([(
                    "content-type".to_owned(),
                    "application/x-www-form-urlencoded".to_owned(),
                )]),
                authorization: None,
                body,
            })
            .await
            .map_err(|error| ProviderAuthError {
                class: "auth_failure",
                reason: format!("FCM OAuth token request failed: {error}"),
            })?;

        if !(200..300).contains(&response.status) {
            return Err(ProviderAuthError {
                class: "auth_failure",
                reason: format!(
                    "FCM OAuth token request returned status {}: {}",
                    response.status,
                    String::from_utf8_lossy(&response.body)
                ),
            });
        }

        let token: GoogleAccessTokenResponse =
            serde_json::from_slice(&response.body).map_err(|error| ProviderAuthError {
                class: "auth_failure",
                reason: format!("invalid FCM OAuth token response: {error}"),
            })?;
        let access_token =
            SecretString::new(token.access_token).map_err(|error| ProviderAuthError {
                class: "auth_failure",
                reason: error.to_string(),
            })?;

        Ok(ProviderAccessToken {
            token: access_token,
            expires_at_ms: now_ms.saturating_add(token.expires_in.saturating_mul(1_000)),
        })
    }
}

#[derive(Clone)]
pub struct FcmDispatcher {
    project_id: String,
    endpoint: ProviderEndpointConfig,
    token_provider: CachedTokenProvider,
    http: Arc<dyn ProviderHttpClient + Send + Sync>,
}

impl FcmDispatcher {
    pub fn new(
        project_id: impl Into<String>,
        token_provider: CachedTokenProvider,
        http: Arc<dyn ProviderHttpClient + Send + Sync>,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            endpoint: ProviderEndpointConfig {
                base_url: "https://fcm.googleapis.com".to_owned(),
                credential_id: "fcm".to_owned(),
            },
            token_provider,
            http,
        }
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.endpoint.base_url = base_url.into();
        self
    }

    pub async fn build_request(
        &self,
        job: &DeliveryJob,
    ) -> Result<ProviderHttpRequest, ProviderError> {
        let authorization = self
            .token_provider
            .bearer_token(now_ms())
            .await
            .map_err(auth_error)?;
        let mut payload = render_payload_json(PushProviderKind::Fcm, job)?;
        if let Some(token) = recipient_token(&job.recipient) {
            payload["message"]["token"] = Value::String(token.to_owned());
        }
        json_request(
            self.endpoint
                .joined_url(&format!("/v1/projects/{}/messages:send", self.project_id)),
            BTreeMap::new(),
            Some(authorization),
            payload,
        )
    }
}

#[async_trait]
impl PushDispatcher for FcmDispatcher {
    fn provider(&self) -> PushProviderKind {
        PushProviderKind::Fcm
    }

    async fn dispatch(&self, batch: DeliveryBatch) -> Vec<DeliveryResult> {
        let futures = batch.jobs.into_iter().map(|job| async move {
            let request = match self.build_request(&job).await {
                Ok(request) => request,
                Err(error) => return result_from_error(job, DeliveryOutcome::Rejected, error),
            };
            let mut response = self.http.send(request).await;
            if response.as_ref().is_ok_and(is_fcm_auth_failure_response) {
                self.token_provider.invalidate().await;
                response = match self.build_request(&job).await {
                    Ok(request) => self.http.send(request).await,
                    Err(error) => {
                        return result_from_error(job, DeliveryOutcome::Retryable, error);
                    }
                };
            }
            classify_http_result(job, response, classify_fcm_response)
        });
        join_all(futures).await
    }

    async fn health_check(&self) -> HealthStatus {
        HealthStatus {
            provider: PushProviderKind::Fcm,
            healthy: !self.project_id.trim().is_empty(),
            details: "fcm http-v1 dispatcher configured".to_owned(),
        }
    }
}

pub(super) fn classify_fcm_response(response: &ProviderHttpResponse) -> ProviderClassification {
    if (200..300).contains(&response.status) {
        return (
            DeliveryOutcome::Accepted,
            None,
            json_field(&response.body, &["name"]),
        );
    }
    let body = String::from_utf8_lossy(&response.body).to_ascii_uppercase();
    if matches!(response.status, 404 | 410)
        || body.contains("UNREGISTERED")
        || body.contains("NOT_FOUND")
    {
        rejected("invalid_token", response, None)
    } else if matches!(response.status, 400 | 413) {
        rejected("invalid_payload", response, None)
    } else if response.status == 403 && body.contains("SENDER_ID_MISMATCH") {
        rejected("invalid_token", response, Some("sender_id_mismatch"))
    } else if matches!(response.status, 401 | 403) {
        rejected("auth_failure", response, None)
    } else if response.status == 429 {
        retryable("quota", response)
    } else if matches!(response.status, 500 | 502 | 503 | 504) {
        retryable("unavailable", response)
    } else {
        rejected("provider_rejected", response, None)
    }
}

fn is_fcm_auth_failure_response(response: &ProviderHttpResponse) -> bool {
    matches!(response.status, 401 | 403)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use serde_json::json;

    use super::super::test_support::{CountingTokenSource, MockHttpClient, batch, response};
    use super::*;
    use crate::domain::SecretString;

    #[cfg(feature = "push-fcm")]
    #[tokio::test]
    async fn fcm_service_account_source_exchanges_signed_assertion() {
        const RSA_PRIVATE_KEY: &str = "-----BEGIN RSA PRIVATE KEY-----\nMIICXQIBAAKBgQDBg42A0sLSkY9ZCWOpxHKzTq1Rhnv4Y7TTzD/0QwVX1zGnI25r\nqC5S/7QB6G7umLXVQ7nAcYCZw4ey1cR014uzvjiZ6/zCjxEUo19F21VyJDcUuaQV\n+reHPEmCPXTvJiRIayERZ9yeejHedA8nAmQomu10RV3WFSrtovcaZqID7QIDAQAB\nAoGAaD2cMQPXbKR6aoTzSdUH0G0WUe82wpO7KchBvyuHKk1Ccv1UEuwSoCUb61cw\nwphCgvIrkm3Rz4KTi5M5c5NUrfscNHpUGksLi5QLL0ryethvVdU960LhMBdnPVkL\ntjNkoDvDCpmdpqPFjC7SG7iefsAPrM1ssSyzlNIKjPLknAECQQDr4oee/lrb8Xd7\nNTUbQBSHdxGbncotZ089VQF3rG0UmWeIHjysNIDmSssYJ4iDn1kPuJrDg9f8LSg8\nn3lRbIztAkEA0gQLoCDLYpbjsf7WNTu0tfZUzv/cQOYuHXW0d24lLuXZ8brI3WOO\n1CgqXYjD9V2axUBuaLMgnKigmeIiE6NzAQJAOgNOg4Xe5rTuQ7kERJ1v7xkTlYgr\nDxuXW2gqojof4a8QzWNSXK/U+85tQJWId4abKsimF2u3lOeuO6qk9aeSyQJBAM9J\nGORimfvkLDbp7Sk7lgCnckuhdBZGWqvPGmFqwJ6KVVPm5QzGkBnMzwvkym0qh3E7\nR/5lFsIpGeLotHOntwECQQDBPAAFfhieU8veUdyVyC/bu+uv6oyUW/35mFM+6L+Y\nz3SK+k2yxYhWqUN3c8EiwWk1Ja93qCxpAqirxf1X7h6C\n-----END RSA PRIVATE KEY-----\n";
        let http = MockHttpClient::with_responses(vec![response(
            200,
            json!({"access_token": "oauth-access-token", "expires_in": 3600}),
        )]);
        let service_account_json = json!({
            "client_email": "sockudo-fcm@example.iam.gserviceaccount.com",
            "private_key": RSA_PRIVATE_KEY,
            "private_key_id": "key-1",
            "project_id": "project-1",
            "token_uri": "https://oauth2.googleapis.com/token"
        })
        .to_string();
        let source =
            FcmServiceAccountTokenSource::from_json(&service_account_json, http.clone()).unwrap();

        let token = source.fetch_token(1_700_000_000_000).await.unwrap();

        assert_eq!(source.project_id(), Some("project-1"));
        assert_eq!(token.token.expose_secret(), "oauth-access-token");
        assert_eq!(token.expires_at_ms, 1_700_003_600_000);
        let requests = http.requests().await;
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].url, "https://oauth2.googleapis.com/token");
        assert_eq!(
            requests[0].headers["content-type"],
            "application/x-www-form-urlencoded"
        );
        let body = String::from_utf8_lossy(&requests[0].body);
        assert!(body.contains("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer"));
        assert!(body.contains("assertion="));
    }

    #[tokio::test]
    async fn fcm_dispatcher_invalidates_token_and_retries_auth_failures() {
        let http = MockHttpClient::with_responses(vec![
            response(401, json!({"error": {"status": "UNAUTHENTICATED"}})),
            response(200, json!({"name": "fcm-message"})),
        ]);
        let source = Arc::new(CountingTokenSource {
            count: AtomicUsize::new(0),
            first_expiry: now_ms() + 600_000,
        });
        let token = CachedTokenProvider::new(source.clone());
        let dispatcher =
            FcmDispatcher::new("project-1", token, http.clone()).with_base_url("https://fcm.test");

        let results = dispatcher.dispatch(batch(PushProviderKind::Fcm)).await;

        assert_eq!(results[0].outcome, DeliveryOutcome::Accepted);
        assert_eq!(source.count.load(Ordering::SeqCst), 2);
        let requests = http.requests().await;
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0]
                .authorization
                .as_ref()
                .map(SecretString::expose_secret),
            Some("Bearer token-0")
        );
        assert_eq!(
            requests[1]
                .authorization
                .as_ref()
                .map(SecretString::expose_secret),
            Some("Bearer token-1")
        );
    }
}
