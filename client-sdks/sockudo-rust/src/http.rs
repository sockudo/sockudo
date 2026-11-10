//! HTTP plumbing for auth endpoints, token URLs, proxies and push (feature `http`).

use std::time::Duration;

use bytes::Bytes;
use reqwest::header::{CONTENT_TYPE, HeaderName, HeaderValue};
use reqwest::{Client, Method, RequestBuilder};
use sonic_rs::Value;

use crate::auth::{ChannelAuthData, ChannelAuthRequest, Endpoint, UserAuthData, UserAuthRequest};
use crate::error::{Error, Result};
use crate::token::{TokenAuthData, TokenRequest};

const AUTH_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_ERROR_BODY: usize = 512;

fn apply_headers(
    mut request: RequestBuilder,
    headers: &[(String, String)],
) -> Result<RequestBuilder> {
    for (name, value) in headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| Error::invalid(format!("invalid header name '{name}'")))?;
        let value = HeaderValue::from_str(value)
            .map_err(|_| Error::invalid(format!("invalid value for header '{name}'")))?;
        request = request.header(name, value);
    }
    Ok(request)
}

fn truncate(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    if text.len() <= MAX_ERROR_BODY {
        return text.into_owned();
    }
    let mut end = MAX_ERROR_BODY;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end])
}

async fn send(request: RequestBuilder) -> Result<(u16, Bytes)> {
    let response = request.send().await?;
    let status = response.status().as_u16();
    let body = response.bytes().await?;
    Ok((status, body))
}

async fn post_form(
    client: &Client,
    endpoint: &Endpoint,
    form: Vec<(String, String)>,
) -> Result<(u16, Bytes)> {
    let body = form_urlencoded::Serializer::new(String::new())
        .extend_pairs(form)
        .finish();
    let request = client
        .post(&endpoint.url)
        .timeout(AUTH_TIMEOUT)
        .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(body);
    send(apply_headers(request, &endpoint.resolved_headers())?).await
}

fn parse_auth<T: serde::de::DeserializeOwned>(status: u16, body: &[u8]) -> Result<T> {
    if status >= 400 {
        return Err(Error::auth(
            Some(status),
            format!("could not get auth info from endpoint, status: {status}"),
        ));
    }
    sonic_rs::from_slice(body)
        .map_err(|_| Error::auth(Some(status), "JSON returned from auth endpoint was invalid"))
}

/// Form POST `socket_id`, `channel_name` (+ params) → `{auth, channel_data?, shared_secret?}`.
pub(crate) async fn authorize_channel(
    client: &Client,
    endpoint: &Endpoint,
    request: &ChannelAuthRequest,
) -> Result<ChannelAuthData> {
    let mut form = endpoint.resolved_params();
    form.push(("socket_id".into(), request.socket_id.clone()));
    form.push(("channel_name".into(), request.channel_name.clone()));
    let (status, body) = post_form(client, endpoint, form).await.map_err(as_auth)?;
    parse_auth(status, &body)
}

/// Form POST `socket_id` (+ params) → `{auth, user_data}`.
pub(crate) async fn authenticate_user(
    client: &Client,
    endpoint: &Endpoint,
    request: &UserAuthRequest,
) -> Result<UserAuthData> {
    let mut form = endpoint.resolved_params();
    form.push(("socket_id".into(), request.socket_id.clone()));
    let (status, body) = post_form(client, endpoint, form).await.map_err(as_auth)?;
    parse_auth(status, &body)
}

fn as_auth(error: Error) -> Error {
    match error {
        Error::Auth { .. } | Error::InvalidOptions(_) => error,
        other => Error::auth(None, other.to_string()),
    }
}

/// JSON POST `{socket_id, reason}` → `{token, expires_in?, …}` (or a bare
/// JSON string token).
pub(crate) async fn fetch_token(
    client: &Client,
    endpoint: &Endpoint,
    request: &TokenRequest,
) -> Result<TokenAuthData> {
    let body = sonic_rs::to_vec(&sonic_rs::json!({
        "socket_id": request.socket_id.as_deref(),
        "reason": request.reason.as_str(),
    }))?;
    let builder = client
        .post(&endpoint.url)
        .timeout(AUTH_TIMEOUT)
        .header(CONTENT_TYPE, "application/json")
        .body(body);
    let (status, body) = send(apply_headers(builder, &endpoint.resolved_headers())?)
        .await
        .map_err(|error| Error::TokenProvider(error.to_string()))?;
    if status >= 400 {
        return Err(Error::TokenProvider(format!(
            "capability-token request failed with status {status}"
        )));
    }
    if let Ok(token) = sonic_rs::from_slice::<String>(&body) {
        return Ok(TokenAuthData::new(token));
    }
    sonic_rs::from_slice(&body)
        .map_err(|_| Error::TokenProvider("token endpoint returned invalid JSON".into()))
}

/// JSON POST to a proxy endpoint; returns the decoded JSON body.
pub(crate) async fn post_proxy(
    client: &Client,
    endpoint: &Endpoint,
    body: &Value,
    timeout: Duration,
) -> Result<Value> {
    let builder = client
        .post(&endpoint.url)
        .timeout(timeout)
        .header(CONTENT_TYPE, "application/json")
        .body(sonic_rs::to_vec(body)?);
    let (status, body) = send(apply_headers(builder, &endpoint.resolved_headers())?).await?;
    if status >= 400 {
        return Err(Error::Http {
            status,
            body: truncate(&body),
        });
    }
    sonic_rs::from_slice(&body)
        .map_err(|error| Error::Decode(format!("proxy returned invalid JSON: {error}")))
}

/// A JSON request for the push registration API. `204` yields `None`.
pub(crate) async fn json_request(
    client: &Client,
    method: Method,
    url: &str,
    headers: &[(String, String)],
    query: &[(String, String)],
    body: Option<Vec<u8>>,
) -> Result<Option<Value>> {
    let url = if query.is_empty() {
        url.to_owned()
    } else {
        let encoded = form_urlencoded::Serializer::new(String::new())
            .extend_pairs(query)
            .finish();
        let separator = if url.contains('?') { '&' } else { '?' };
        format!("{url}{separator}{encoded}")
    };
    let mut builder = client
        .request(method, url)
        .header(CONTENT_TYPE, "application/json");
    if let Some(body) = body {
        builder = builder.body(body);
    }
    let (status, body) = send(apply_headers(builder, headers)?).await?;
    if status >= 400 {
        return Err(Error::Http {
            status,
            body: truncate(&body),
        });
    }
    if status == 204 || body.is_empty() {
        return Ok(None);
    }
    sonic_rs::from_slice(&body)
        .map(Some)
        .map_err(|error| Error::Decode(format!("push API returned invalid JSON: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncates_long_bodies_on_char_boundaries() {
        assert_eq!(truncate(b"short"), "short");
        let long = "é".repeat(600);
        let cut = truncate(long.as_bytes());
        assert!(cut.ends_with('…'));
        assert!(cut.len() <= MAX_ERROR_BODY + '…'.len_utf8());
    }

    #[test]
    fn auth_parsing_maps_status_and_shape() {
        let ok: ChannelAuthData =
            parse_auth(200, br#"{"auth":"k:s","channel_data":"{}"}"#).unwrap();
        assert_eq!(ok.auth, "k:s");
        assert!(matches!(
            parse_auth::<ChannelAuthData>(403, b"{}"),
            Err(Error::Auth {
                status: Some(403),
                ..
            })
        ));
        assert!(matches!(
            parse_auth::<ChannelAuthData>(200, b"[]"),
            Err(Error::Auth {
                status: Some(200),
                ..
            })
        ));
    }
}
