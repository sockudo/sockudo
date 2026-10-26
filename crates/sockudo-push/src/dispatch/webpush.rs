use std::collections::BTreeMap;
use std::sync::Arc;
#[cfg(feature = "push-webpush")]
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
#[cfg(feature = "push-webpush")]
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::future::join_all;
use sonic_rs::Value;
#[cfg(feature = "push-webpush")]
use sonic_rs::json;
use sonic_rs::prelude::*;
#[cfg(feature = "push-webpush")]
use url::Url;
#[cfg(feature = "push-webpush")]
use {p256::ecdsa::SigningKey, p256::elliptic_curve::sec1::ToSec1Point};

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
    DeliveryBatch, DeliveryJob, DeliveryOutcome, DeliveryResult, ProviderError,
    ProviderFailureClass, PushProviderKind, PushRecipient, SecretString,
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
    signing_key: p256::ecdsa::SigningKey,
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
                use p256::elliptic_curve::sec1::ToSec1Point;

                let secret_key =
                    p256::SecretKey::from_slice(&bytes).map_err(|_| NativeWebPushKeyError::Key)?;
                let public_key =
                    URL_SAFE_NO_PAD.encode(secret_key.public_key().to_sec1_point(false).as_bytes());
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
        use p256::ecdsa::{Signature, signature::Signer};

        let vapid_key = self.vapid_key.as_ref().map_err(|error| match error {
            NativeWebPushKeyError::Encoding => ProviderError {
                class: "auth_failure".to_owned(),
                failure_class: ProviderFailureClass::CredentialAuth,
                reason: Some("invalid VAPID private key encoding".to_owned()),
                retry_after_ms: None,
            },
            NativeWebPushKeyError::Key => ProviderError {
                class: "auth_failure".to_owned(),
                failure_class: ProviderFailureClass::CredentialAuth,
                reason: Some("invalid VAPID private key".to_owned()),
                retry_after_ms: None,
            },
        })?;
        let endpoint_uri = Url::parse(endpoint).map_err(|_| ProviderError {
            class: "invalid_token".to_owned(),
            failure_class: ProviderFailureClass::DeviceTerminal,
            reason: Some("invalid Web Push endpoint".to_owned()),
            retry_after_ms: None,
        })?;
        let scheme = endpoint_uri.scheme();
        if scheme.is_empty() {
            return Err(ProviderError {
                class: "invalid_token".to_owned(),
                failure_class: ProviderFailureClass::DeviceTerminal,
                reason: Some("invalid Web Push endpoint scheme".to_owned()),
                retry_after_ms: None,
            });
        }
        let host = endpoint_uri.host_str().ok_or_else(|| ProviderError {
            class: "invalid_token".to_owned(),
            failure_class: ProviderFailureClass::DeviceTerminal,
            reason: Some("invalid Web Push endpoint host".to_owned()),
            retry_after_ms: None,
        })?;
        let exp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ProviderError {
                class: "auth_failure".to_owned(),
                failure_class: ProviderFailureClass::CredentialAuth,
                reason: Some("system clock before Unix epoch".to_owned()),
                retry_after_ms: None,
            })?
            .as_secs()
            .saturating_add(self.valid_for.as_secs());
        let header = URL_SAFE_NO_PAD.encode(br#"{"typ":"JWT","alg":"ES256"}"#);
        let claims = URL_SAFE_NO_PAD.encode(
            sonic_rs::to_vec(&json!({
                "aud": format!("{scheme}://{host}"),
                "exp": exp,
                "sub": self.contact,
            }))
            .map_err(|_| ProviderError {
                class: "auth_failure".to_owned(),
                failure_class: ProviderFailureClass::CredentialAuth,
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
                failure_class: ProviderFailureClass::CredentialAuth,
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
        let p256dh_bytes = URL_SAFE_NO_PAD
            .decode(p256dh.expose_secret().as_bytes())
            .map_err(|_| ProviderError {
                class: "invalid_token".to_owned(),
                failure_class: ProviderFailureClass::DeviceTerminal,
                reason: Some("invalid Web Push p256dh encoding".to_owned()),
                retry_after_ms: None,
            })?;
        let auth_bytes = URL_SAFE_NO_PAD
            .decode(auth.expose_secret().as_bytes())
            .map_err(|_| ProviderError {
                class: "invalid_token".to_owned(),
                failure_class: ProviderFailureClass::DeviceTerminal,
                reason: Some("invalid Web Push auth encoding".to_owned()),
                retry_after_ms: None,
            })?;
        if auth_bytes.len() != 16 {
            return Err(ProviderError {
                class: "invalid_token".to_owned(),
                failure_class: ProviderFailureClass::DeviceTerminal,
                reason: Some("invalid Web Push auth length".to_owned()),
                retry_after_ms: None,
            });
        }

        let ua_public =
            p256::PublicKey::from_sec1_bytes(&p256dh_bytes).map_err(|_| ProviderError {
                class: "invalid_token".to_owned(),
                failure_class: ProviderFailureClass::DeviceTerminal,
                reason: Some("invalid Web Push p256dh key".to_owned()),
                retry_after_ms: None,
            })?;
        let auth_array: [u8; 16] = auth_bytes.try_into().map_err(|_| ProviderError {
            class: "invalid_token".to_owned(),
            failure_class: ProviderFailureClass::DeviceTerminal,
            reason: Some("invalid Web Push auth length".to_owned()),
            retry_after_ms: None,
        })?;
        let body = encrypt_webpush_payload(payload.to_vec(), &ua_public, &auth_array).map_err(
            |error| ProviderError {
                class: "invalid_payload".to_owned(),
                failure_class: ProviderFailureClass::CallerPayload,
                reason: Some(format!("web push encryption failed: {error}")),
                retry_after_ms: None,
            },
        )?;

        let authorization = Some(self.vapid_authorization(endpoint)?);
        let headers = BTreeMap::from([
            ("ttl".to_owned(), self.valid_for.as_secs().to_string()),
            ("content-encoding".to_owned(), "aes128gcm".to_owned()),
            (
                "content-type".to_owned(),
                "application/octet-stream".to_owned(),
            ),
            ("content-length".to_owned(), body.len().to_string()),
        ])
        .into_iter()
        .collect();

        Ok(WebPushPreparedRequest {
            headers,
            authorization,
            body,
        })
    }
}

/// Derives the RFC 8291 Section 3.3 input keying material for content
/// encryption: HKDF(auth_secret, ECDH shared secret, "WebPush: info" ||
/// 0x00 || ua_public || as_public).
#[cfg(feature = "push-webpush")]
fn webpush_ikm(
    shared: &p256::ecdh::SharedSecret,
    ua_public: &p256::PublicKey,
    as_public: &p256::PublicKey,
    ua_auth: &[u8; 16],
) -> [u8; 32] {
    let mut info = Vec::with_capacity(13 + 65 + 65);
    info.extend_from_slice(b"WebPush: info");
    info.push(0u8);
    info.extend_from_slice(ua_public.to_sec1_point(false).as_bytes());
    info.extend_from_slice(as_public.to_sec1_point(false).as_bytes());

    let mut ikm = [0u8; 32];
    let hk = hkdf::Hkdf::<sha2::Sha256>::new(Some(ua_auth), shared.raw_secret_bytes().as_ref());
    hk.expand(&info, &mut ikm)
        .expect("ikm length is always 32 bytes, cannot be too large");
    ikm
}

/// Encrypts a Web Push message according to RFC 8291 (Message Encryption for
/// Web Push) with the RFC 8188 "aes128gcm" content encoding, matching the
/// behavior of `web-push-native::WebPushBuilder::build`.
#[cfg(feature = "push-webpush")]
fn encrypt_webpush_payload(
    message: Vec<u8>,
    ua_public: &p256::PublicKey,
    ua_auth: &[u8; 16],
) -> Result<Vec<u8>, ece_native::Error> {
    use p256::ecdh::diffie_hellman;
    use p256::elliptic_curve::Generate;

    let salt = <[u8; 16] as Generate>::generate();
    let as_secret = p256::SecretKey::generate();
    let as_public = as_secret.public_key();
    let shared = diffie_hellman(as_secret.to_nonzero_scalar(), ua_public.as_affine());
    let ikm = webpush_ikm(&shared, ua_public, &as_public, ua_auth);

    let keyid = as_public.to_sec1_point(false);
    let encrypted_record_length = (message.len() + 17)
        .try_into()
        .map_err(|_| ece_native::Error::RecordLengthInvalid)?;

    ece_native::encrypt(
        ikm,
        salt,
        keyid.as_bytes(),
        Some(message).into_iter(),
        encrypted_record_length,
    )
}

/// Decrypts a payload produced by [`encrypt_webpush_payload`]; used by tests to
/// verify the full RFC 8291 round trip.
#[cfg(all(feature = "push-webpush", test))]
pub(crate) fn decrypt_webpush_payload(
    encrypted: Vec<u8>,
    as_secret: &p256::SecretKey,
    ua_auth: &[u8; 16],
) -> Result<Vec<u8>, ece_native::Error> {
    use p256::ecdh::diffie_hellman;

    if encrypted.len() < 21 {
        return Err(ece_native::Error::HeaderLengthInvalid);
    }

    let idlen = usize::from(encrypted[20]);
    if encrypted[21..].len() < idlen {
        return Err(ece_native::Error::KeyIdLengthInvalid);
    }

    let sender_public = p256::PublicKey::from_sec1_bytes(&encrypted[21..21 + idlen])
        .map_err(|_| ece_native::Error::Aes128Gcm)?;
    let shared = diffie_hellman(as_secret.to_nonzero_scalar(), sender_public.as_affine());
    let ikm = webpush_ikm(&shared, &as_secret.public_key(), &sender_public, ua_auth);

    ece_native::decrypt(ikm, encrypted)
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
                failure_class: ProviderFailureClass::DeviceTerminal,
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
        let body = sonic_rs::to_vec(&rendered).map_err(|_| ProviderError {
            class: "invalid_payload".to_owned(),
            failure_class: ProviderFailureClass::CallerPayload,
            reason: Some("web push payload serialization failed".to_owned()),
            retry_after_ms: None,
        })?;
        let prepared = self
            .crypto
            .prepare_request(&endpoint, p256dh, auth, &body, authorization)
            .await?;
        let mut headers = prepared.headers;
        headers.extend(web_push_headers(&rendered)?);
        Ok(ProviderHttpRequest {
            method: ProviderHttpMethod::Post,
            url: endpoint,
            headers,
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

fn web_push_headers(rendered: &Value) -> Result<BTreeMap<String, String>, ProviderError> {
    let mut headers = BTreeMap::new();
    let Some(rendered_headers) = rendered.get("headers").and_then(Value::as_object) else {
        return Ok(headers);
    };
    if let Some(ttl) = rendered_headers.get(&"ttl") {
        let ttl = ttl
            .as_u64()
            .map(|ttl| ttl.to_string())
            .or_else(|| ttl.as_str().map(str::to_owned))
            .ok_or_else(|| ProviderError {
                class: "invalid_payload".to_owned(),
                failure_class: ProviderFailureClass::CallerPayload,
                reason: Some("web push ttl must be a positive integer".to_owned()),
                retry_after_ms: None,
            })?;
        headers.insert("ttl".to_owned(), ttl);
    }
    for name in ["urgency", "topic"] {
        if let Some(value) = rendered_headers.get(&name) {
            let value = value.as_str().ok_or_else(|| ProviderError {
                class: "invalid_payload".to_owned(),
                failure_class: ProviderFailureClass::CallerPayload,
                reason: Some(format!("web push {name} must be a string")),
                retry_after_ms: None,
            })?;
            headers.insert(name.to_owned(), value.to_owned());
        }
    }
    Ok(headers)
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
        404 | 410 => rejected(
            "invalid_token",
            ProviderFailureClass::DeviceTerminal,
            response,
            None,
        ),
        413 => rejected(
            "invalid_payload",
            ProviderFailureClass::CallerPayload,
            response,
            Some("payload_too_large"),
        ),
        429 => retryable("quota", ProviderFailureClass::ProviderQuota, response),
        500..=599 => retryable(
            "unavailable",
            ProviderFailureClass::ProviderTransient,
            response,
        ),
        401 | 403 => rejected(
            "auth_failure",
            ProviderFailureClass::CredentialAuth,
            response,
            None,
        ),
        _ => rejected(
            "provider_rejected",
            ProviderFailureClass::Unknown,
            response,
            None,
        ),
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "push-webpush")]
    use super::*;

    #[cfg(feature = "push-webpush")]
    #[tokio::test]
    async fn native_web_push_crypto_encrypts_and_signs_request() {
        use base64::Engine as _;
        use p256::elliptic_curve::{Generate, sec1::ToSec1Point};

        let ua_secret = p256::SecretKey::generate();
        let ua_public = ua_secret.public_key();
        let auth_bytes = [7u8; 16];
        let p256dh =
            SecretString::new(URL_SAFE_NO_PAD.encode(ua_public.to_sec1_point(false).as_bytes()))
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

        let decrypted = decrypt_webpush_payload(prepared.body, &ua_secret, &auth_bytes).unwrap();
        assert_eq!(decrypted, payload);
    }

    /// Pins the production key derivation against the RFC 8291 Appendix A /
    /// Section 5 vectors, so a shared mistake in [`webpush_ikm`] cannot pass
    /// the round-trip tests while real user agents fail to decrypt.
    ///
    /// The RFC example encrypts with record size 4096 (rs field 0x00001000)
    /// and a fixed salt/sender key; [`encrypt_webpush_payload`] chooses
    /// `len + 17` instead, so the header and ciphertext are asserted in
    /// segments rather than byte-comparing one full body.
    #[cfg(feature = "push-webpush")]
    #[test]
    fn encryption_matches_rfc_8291_appendix_a_vectors() {
        use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
        use p256::ecdh::diffie_hellman;
        use p256::elliptic_curve::sec1::ToSec1Point;

        fn b64d(value: &str) -> Vec<u8> {
            URL_SAFE_NO_PAD.decode(value).unwrap()
        }

        let ua_public = p256::PublicKey::from_sec1_bytes(&b64d(
            "BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcxaOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4",
        ))
        .unwrap();
        let as_public = p256::PublicKey::from_sec1_bytes(&b64d(
            "BP4z9KsN6nGRTbVYI_c7VJSPQTBtkgcy27mlmlMoZIIgDll6e3vCYLocInmYWAmS6TlzAC8wEqKK6PBru3jl7A8",
        ))
        .unwrap();
        let as_secret =
            p256::SecretKey::from_slice(&b64d("yfWPiYE-n46HLnH0KqZOF1fJJU3MYrct3AELtAQ-oRw"))
                .unwrap();
        let auth: [u8; 16] = b64d("BTBZMqHH6r4Tts7J_aSIgg").try_into().unwrap();
        let salt: [u8; 16] = b64d("DGv6ra1nlYgDCS1FRnbzlw").try_into().unwrap();
        let plaintext = b"When I grow up, I want to be a watermelon";

        let shared = diffie_hellman(as_secret.to_nonzero_scalar(), ua_public.as_affine());
        assert_eq!(
            shared.raw_secret_bytes().as_slice(),
            b64d("kyrL1jIIOHEzg3sM2ZWRHDRB62YACZhhSlknJ672kSs").as_slice(),
            "ECDH shared secret must match RFC 8291 Appendix A"
        );

        let ikm = webpush_ikm(&shared, &ua_public, &as_public, &auth);
        assert_eq!(
            ikm,
            b64d("S4lYMb_L0FxCeq0WhDx813KgSYqU26kOyzWUdsXYyrg").as_slice(),
            "\"WebPush: info\" HKDF derivation must match RFC 8291 Appendix A"
        );

        let keyid = as_public.to_sec1_point(false);
        let encrypted = ece_native::encrypt(
            ikm,
            salt,
            keyid.as_bytes(),
            Some(plaintext.to_vec()).into_iter(),
            4096,
        )
        .unwrap();

        // Header: salt || rs=4096 (0x00001000) || idlen=65 || as_public.
        assert_eq!(&encrypted[..16], &salt[..]);
        assert_eq!(&encrypted[16..20], &[0x00, 0x00, 0x10, 0x00]);
        assert_eq!(encrypted[20], 65);
        assert_eq!(&encrypted[21..86], keyid.as_bytes());

        // RFC 8188/8291 ciphertext for the 0x02-padded plaintext under the
        // CEK and nonce derived from the pinned IKM.
        assert_eq!(
            &encrypted[86..],
            b64d("8pfeW0KbunFT06SuDKoJH9Ql87S1QUrdirN6GcG7sFz1y1sqLgVi1VhjVkHsUoEsbI_0LpXMuGvnzQ")
                .as_slice(),
            "aes128gcm record must match RFC 8291 Section 5 ciphertext"
        );

        // The receiver-side derivation must decrypt the RFC message back; the
        // UA private key from RFC 8291 Section 5 is the receiver key here.
        let ua_secret =
            p256::SecretKey::from_slice(&b64d("q1dXpw3UpT5VOmu_cf_v6ih07Aems3njxI-JWgLcM94"))
                .unwrap();
        let decrypted = decrypt_webpush_payload(encrypted, &ua_secret, &auth).unwrap();
        assert_eq!(decrypted, plaintext);
    }
}
