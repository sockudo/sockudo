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

#[cfg(all(
    feature = "opentelemetry",
    any(
        feature = "push-fcm",
        feature = "push-apns",
        feature = "push-webpush",
        feature = "push-hms",
        feature = "push-wns"
    )
))]
use opentelemetry::{global, propagation::Injector};
#[cfg(all(
    feature = "opentelemetry",
    any(
        feature = "push-fcm",
        feature = "push-apns",
        feature = "push-webpush",
        feature = "push-hms",
        feature = "push-wns"
    )
))]
use tracing::{Instrument, field, info_span};
#[cfg(all(
    feature = "opentelemetry",
    any(
        feature = "push-fcm",
        feature = "push-apns",
        feature = "push-webpush",
        feature = "push-hms",
        feature = "push-wns"
    )
))]
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::domain::{ProviderError, ProviderFailureClass, SecretString};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderHttpMethod {
    Get,
    Post,
    Delete,
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
    validate_each_destination: bool,
}

#[cfg(any(
    feature = "push-fcm",
    feature = "push-apns",
    feature = "push-webpush",
    feature = "push-hms",
    feature = "push-wns"
))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderHttpClientOptions {
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub pool_idle_timeout_secs: u64,
    pub max_idle_connections_per_host: usize,
    pub tcp_keepalive_secs: u64,
    pub http2_keepalive_interval_secs: u64,
    pub http2_keepalive_timeout_secs: u64,
}

#[cfg(any(
    feature = "push-fcm",
    feature = "push-apns",
    feature = "push-webpush",
    feature = "push-hms",
    feature = "push-wns"
))]
impl Default for ProviderHttpClientOptions {
    fn default() -> Self {
        Self {
            connect_timeout_ms: 5_000,
            request_timeout_ms: 10_000,
            pool_idle_timeout_secs: 90,
            max_idle_connections_per_host: 128,
            tcp_keepalive_secs: 60,
            http2_keepalive_interval_secs: 30,
            http2_keepalive_timeout_secs: 10,
        }
    }
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
        Self::build_rustls(ProviderHttpClientOptions::default(), true, None)
    }

    /// Builds a pooled client for provider URLs derived exclusively from trusted server config.
    /// This avoids a DNS lookup on every delivery while retaining request-level validation for
    /// user-controlled Web Push and WNS destinations.
    pub fn new_for_trusted_provider(options: ProviderHttpClientOptions) -> Result<Self, String> {
        Self::build_rustls(options, false, None)
    }

    #[cfg(feature = "push-apns")]
    pub fn new_with_pem_identity(pem: &str) -> Result<Self, String> {
        Self::new_with_pem_identity_and_options(pem, ProviderHttpClientOptions::default())
    }

    #[cfg(feature = "push-apns")]
    pub fn new_with_pem_identity_and_options(
        pem: &str,
        options: ProviderHttpClientOptions,
    ) -> Result<Self, String> {
        let identity = reqwest::Identity::from_pem(pem.as_bytes())
            .map_err(|error| format!("invalid APNs PEM identity: {error}"))?;
        Self::build_rustls(options, false, Some(identity))
    }

    #[cfg(feature = "push-apns")]
    pub fn new_with_pkcs12_identity(der: &[u8], password: &str) -> Result<Self, String> {
        Self::new_with_pkcs12_identity_and_options(
            der,
            password,
            ProviderHttpClientOptions::default(),
        )
    }

    #[cfg(feature = "push-apns")]
    pub fn new_with_pkcs12_identity_and_options(
        der: &[u8],
        password: &str,
        options: ProviderHttpClientOptions,
    ) -> Result<Self, String> {
        let identity = reqwest::Identity::from_pkcs12_der(der, password)
            .map_err(|error| format!("invalid APNs PKCS#12 identity: {error}"))?;
        let client = configured_builder(&options)
            .use_native_tls()
            .identity(identity)
            .build()
            .map_err(|error| error.to_string())?;
        Ok(Self {
            client,
            validate_each_destination: false,
        })
    }

    fn build_rustls(
        options: ProviderHttpClientOptions,
        validate_each_destination: bool,
        identity: Option<reqwest::Identity>,
    ) -> Result<Self, String> {
        let mut builder = configured_builder(&options).use_rustls_tls();
        if let Some(identity) = identity {
            builder = builder.identity(identity);
        }
        let client = builder.build().map_err(|error| error.to_string())?;
        Ok(Self {
            client,
            validate_each_destination,
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
fn configured_builder(options: &ProviderHttpClientOptions) -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_millis(options.connect_timeout_ms.max(1)))
        .timeout(Duration::from_millis(options.request_timeout_ms.max(1)))
        .pool_idle_timeout(Duration::from_secs(options.pool_idle_timeout_secs.max(1)))
        .pool_max_idle_per_host(options.max_idle_connections_per_host.max(1))
        .tcp_keepalive(Duration::from_secs(options.tcp_keepalive_secs.max(1)))
        .http2_adaptive_window(true)
        .http2_keep_alive_interval(Duration::from_secs(
            options.http2_keepalive_interval_secs.max(1),
        ))
        .http2_keep_alive_timeout(Duration::from_secs(
            options.http2_keepalive_timeout_secs.max(1),
        ))
        .http2_keep_alive_while_idle(true)
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
        if self.validate_each_destination {
            validate_delivery_destination(&request.url).await?;
        }
        #[cfg(feature = "opentelemetry")]
        let server_address = Url::parse(&request.url)
            .ok()
            .and_then(|url| url.host_str().map(ToOwned::to_owned))
            .unwrap_or_else(|| "unknown".to_string());
        #[cfg(feature = "opentelemetry")]
        let method_name = match request.method {
            ProviderHttpMethod::Get => "GET",
            ProviderHttpMethod::Post => "POST",
            ProviderHttpMethod::Delete => "DELETE",
        };
        #[cfg(feature = "opentelemetry")]
        let span = info_span!(
            target: "sockudo_telemetry",
            "http.client.request",
            otel.kind = "client",
            otel.name = %format!("{method_name} push provider"),
            http.request.method = method_name,
            server.address = %server_address,
            http.response.status_code = field::Empty,
            otel.status_code = field::Empty,
        );
        let method = match request.method {
            ProviderHttpMethod::Get => reqwest::Method::GET,
            ProviderHttpMethod::Post => reqwest::Method::POST,
            ProviderHttpMethod::Delete => reqwest::Method::DELETE,
        };
        let mut builder = self.client.request(method, &request.url);
        let headers = request.headers;
        #[cfg(feature = "opentelemetry")]
        let headers = {
            let mut headers = headers;
            global::get_text_map_propagator(|propagator| {
                propagator.inject_context(&span.context(), &mut PushHeaderInjector(&mut headers));
            });
            headers
        };
        for (name, value) in headers {
            builder = builder.header(name, value);
        }
        if let Some(authorization) = request.authorization {
            builder = builder.header("authorization", authorization.expose_secret());
        }
        let response = builder.body(request.body).send();
        #[cfg(feature = "opentelemetry")]
        let response = response.instrument(span.clone()).await;
        #[cfg(not(feature = "opentelemetry"))]
        let response = response.await;
        let response = response.map_err(|error| {
            #[cfg(feature = "opentelemetry")]
            span.record("otel.status_code", "ERROR");
            reqwest_error_chain(error)
        })?;
        let status = response.status().as_u16();
        #[cfg(feature = "opentelemetry")]
        {
            span.record("http.response.status_code", status);
            if status >= 400 {
                span.record("otel.status_code", "ERROR");
            }
        }
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

#[cfg(all(
    feature = "opentelemetry",
    any(
        feature = "push-fcm",
        feature = "push-apns",
        feature = "push-webpush",
        feature = "push-hms",
        feature = "push-wns"
    )
))]
struct PushHeaderInjector<'a>(&'a mut BTreeMap<String, String>);

#[cfg(all(
    feature = "opentelemetry",
    any(
        feature = "push-fcm",
        feature = "push-apns",
        feature = "push-webpush",
        feature = "push-hms",
        feature = "push-wns"
    )
))]
impl Injector for PushHeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_owned(), value);
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
        failure_class: ProviderFailureClass::DeviceTerminal,
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
            failure_class: ProviderFailureClass::DeviceTerminal,
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
            failure_class: ProviderFailureClass::from_legacy_url_class(class),
            reason: Some("provider URL must use https".to_owned()),
            retry_after_ms: None,
        });
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(ProviderError {
            class: class.to_owned(),
            failure_class: ProviderFailureClass::from_legacy_url_class(class),
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
                "authorization"
                    | "proxy-authorization"
                    | "cookie"
                    | "set-cookie"
                    | "apns-channel-id"
            ) {
                (name.clone(), "[REDACTED]".to_owned())
            } else {
                (name.clone(), value.clone())
            }
        })
        .collect()
}
