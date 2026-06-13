use std::collections::BTreeMap;
#[cfg(any(
    feature = "push-fcm",
    feature = "push-apns",
    feature = "push-webpush",
    feature = "push-hms",
    feature = "push-wns"
))]
use std::error::Error as StdError;
use std::fmt;
use std::net::IpAddr;
#[cfg(any(
    feature = "push-fcm",
    feature = "push-apns",
    feature = "push-webpush",
    feature = "push-hms",
    feature = "push-wns"
))]
use std::time::Duration;

use async_trait::async_trait;
use url::{Host, Url};

use crate::domain::{ProviderError, SecretString};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProviderHttpMethod {
    Get,
    Post,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ProviderHttpRequest {
    pub method: ProviderHttpMethod,
    pub url: String,
    pub headers: BTreeMap<String, String>,
    pub authorization: Option<SecretString>,
    pub body: Vec<u8>,
}

impl fmt::Debug for ProviderHttpRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProviderHttpRequest")
            .field("method", &self.method)
            .field("url", &redact_url(&self.url))
            .field("headers", &redacted_headers(&self.headers))
            .field(
                "authorization",
                &self.authorization.as_ref().map(|_| "[REDACTED]"),
            )
            .field("body", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderHttpResponse {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}

#[async_trait]
pub trait ProviderHttpClient: Send + Sync {
    async fn send(&self, request: ProviderHttpRequest) -> Result<ProviderHttpResponse, String>;
}

#[cfg(any(
    feature = "push-fcm",
    feature = "push-apns",
    feature = "push-webpush",
    feature = "push-hms",
    feature = "push-wns"
))]
#[derive(Clone)]
pub struct ReqwestProviderHttpClient {
    client: reqwest::Client,
}

#[cfg(any(
    feature = "push-fcm",
    feature = "push-apns",
    feature = "push-webpush",
    feature = "push-hms",
    feature = "push-wns"
))]
impl ReqwestProviderHttpClient {
    pub fn new() -> Result<Self, String> {
        let client = reqwest::Client::builder()
            .use_rustls_tls()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|error| error.to_string())?;
        Ok(Self { client })
    }

    #[cfg(feature = "push-apns")]
    pub fn new_with_pem_identity(pem: &str) -> Result<Self, String> {
        let identity = reqwest::Identity::from_pem(pem.as_bytes())
            .map_err(|error| format!("invalid APNs PEM identity: {error}"))?;
        let client = reqwest::Client::builder()
            .use_rustls_tls()
            .identity(identity)
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|error| error.to_string())?;
        Ok(Self { client })
    }

    #[cfg(feature = "push-apns")]
    pub fn new_with_pkcs12_identity(der: &[u8], password: &str) -> Result<Self, String> {
        let identity = reqwest::Identity::from_pkcs12_der(der, password)
            .map_err(|error| format!("invalid APNs PKCS#12 identity: {error}"))?;
        let client = reqwest::Client::builder()
            .use_native_tls()
            .identity(identity)
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|error| error.to_string())?;
        Ok(Self { client })
    }
}

#[cfg(any(
    feature = "push-fcm",
    feature = "push-apns",
    feature = "push-webpush",
    feature = "push-hms",
    feature = "push-wns"
))]
#[async_trait]
impl ProviderHttpClient for ReqwestProviderHttpClient {
    async fn send(&self, request: ProviderHttpRequest) -> Result<ProviderHttpResponse, String> {
        validate_delivery_destination(&request.url).await?;
        let method = match request.method {
            ProviderHttpMethod::Get => reqwest::Method::GET,
            ProviderHttpMethod::Post => reqwest::Method::POST,
        };
        let mut builder = self.client.request(method, &request.url);
        for (name, value) in request.headers {
            builder = builder.header(name, value);
        }
        if let Some(authorization) = request.authorization {
            builder = builder.header("authorization", authorization.expose_secret());
        }
        let response = builder
            .body(request.body)
            .send()
            .await
            .map_err(reqwest_error_chain)?;
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_ascii_lowercase(), value.to_owned()))
            })
            .collect();
        let body = response.bytes().await.map_err(reqwest_error_chain)?;
        Ok(ProviderHttpResponse {
            status,
            headers,
            body: body.to_vec(),
        })
    }
}

#[cfg(any(
    feature = "push-fcm",
    feature = "push-apns",
    feature = "push-webpush",
    feature = "push-hms",
    feature = "push-wns"
))]
fn reqwest_error_chain(error: reqwest::Error) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(error) = source {
        message.push_str(": ");
        message.push_str(&error.to_string());
        source = error.source();
    }
    message
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderEndpointConfig {
    pub base_url: String,
    pub credential_id: String,
}

impl ProviderEndpointConfig {
    pub(super) fn joined_url(&self, path: &str) -> String {
        format!(
            "{}/{}",
            self.base_url.trim_end_matches('/'),
            path.trim_start_matches('/')
        )
    }
}

pub(super) fn validate_webpush_target(endpoint: &str) -> Result<(), ProviderError> {
    let parsed = Url::parse(endpoint).map_err(|_| ProviderError {
        class: "invalid_token".to_owned(),
        reason: Some("web push endpoint must be a URL".to_owned()),
        retry_after_ms: None,
    })?;
    validate_parsed_https_url(&parsed, "invalid_token")?;
    let host = parsed.host_str().unwrap_or_default().to_ascii_lowercase();
    if parsed.host().is_some_and(host_variant_is_private_or_local)
        || host_is_private_or_local(&host)
    {
        return Err(ProviderError {
            class: "invalid_token".to_owned(),
            reason: Some("web push endpoint host is not allowed".to_owned()),
            retry_after_ms: None,
        });
    }
    Ok(())
}

#[cfg(any(
    feature = "push-fcm",
    feature = "push-apns",
    feature = "push-webpush",
    feature = "push-hms",
    feature = "push-wns"
))]
async fn validate_delivery_destination(url: &str) -> Result<(), String> {
    let parsed = Url::parse(url).map_err(|_| "provider URL is invalid".to_owned())?;
    validate_parsed_https_url(&parsed, "invalid_token").map_err(|error| {
        error
            .reason
            .unwrap_or_else(|| "provider URL is not allowed".to_owned())
    })?;
    let host = parsed
        .host_str()
        .ok_or_else(|| "provider URL must include a host".to_owned())?
        .to_ascii_lowercase();
    if parsed.host().is_some_and(host_variant_is_private_or_local)
        || host_is_private_or_local(&host)
    {
        return Err("provider URL host is not allowed".to_owned());
    }
    let port = parsed.port_or_known_default().unwrap_or(443);
    let addresses = tokio::net::lookup_host((host.as_str(), port))
        .await
        .map_err(|error| format!("provider URL DNS lookup failed: {error}"))?;
    for address in addresses {
        if ip_is_private_or_local(address.ip()) {
            return Err("provider URL resolved to a disallowed address".to_owned());
        }
    }
    Ok(())
}

fn host_is_private_or_local(host: &str) -> bool {
    host == "localhost"
        || host.ends_with(".local")
        || host.parse::<IpAddr>().is_ok_and(ip_is_private_or_local)
}

fn ip_is_private_or_local(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_multicast()
                || ip.is_unspecified()
                || ip.is_broadcast()
                || ip.is_documentation()
                || ip.octets()[0] == 0
        }
        IpAddr::V6(ip) => {
            if let Some(mapped) = ip.to_ipv4_mapped() {
                return ip_is_private_or_local(IpAddr::V4(mapped));
            }
            ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_unique_local()
                || ip.is_unicast_link_local()
                || ip.is_multicast()
        }
    }
}

fn host_variant_is_private_or_local(host: Host<&str>) -> bool {
    match host {
        Host::Domain(_) => false,
        Host::Ipv4(ip) => ip_is_private_or_local(IpAddr::V4(ip)),
        Host::Ipv6(ip) => ip_is_private_or_local(IpAddr::V6(ip)),
    }
}

fn validate_parsed_https_url(parsed: &Url, class: &str) -> Result<(), ProviderError> {
    if parsed.scheme() != "https" {
        return Err(ProviderError {
            class: class.to_owned(),
            reason: Some("provider URL must use https".to_owned()),
            retry_after_ms: None,
        });
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(ProviderError {
            class: class.to_owned(),
            reason: Some("provider URL must not include userinfo".to_owned()),
            retry_after_ms: None,
        });
    }
    Ok(())
}

fn redact_url(url: &str) -> String {
    Url::parse(url)
        .ok()
        .map(|mut parsed| {
            parsed.set_query(None);
            if parsed.path().starts_with("/3/device/") {
                parsed.set_path("/3/device/[REDACTED]");
            } else {
                let redacted_path = parsed
                    .path_segments()
                    .map(|segments| {
                        segments
                            .map(redact_path_segment)
                            .collect::<Vec<_>>()
                            .join("/")
                    })
                    .unwrap_or_default();
                parsed.set_path(&format!("/{redacted_path}"));
            }
            parsed.to_string()
        })
        .unwrap_or_else(|| "[REDACTED_URL]".to_owned())
}

fn redact_path_segment(segment: &str) -> String {
    let long_token_shape = segment.len() >= 24
        && segment
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'='));
    if long_token_shape {
        "[REDACTED]".to_owned()
    } else {
        segment.to_owned()
    }
}

fn redacted_headers(headers: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    headers
        .iter()
        .map(|(name, value)| {
            let lower = name.to_ascii_lowercase();
            if matches!(
                lower.as_str(),
                "authorization" | "proxy-authorization" | "cookie" | "set-cookie"
            ) {
                (name.clone(), "[REDACTED]".to_owned())
            } else {
                (name.clone(), value.clone())
            }
        })
        .collect()
}
