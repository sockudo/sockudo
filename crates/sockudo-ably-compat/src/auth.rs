//! Ably key and bearer credential parsing.

use axum::http::{HeaderMap, header};
use base64::{Engine as _, engine::general_purpose};

pub(crate) fn parse_ably_key(raw: &str) -> (&str, Option<&str>) {
    raw.split_once(':')
        .map(|(key, secret)| (key, Some(secret)))
        .unwrap_or((raw, None))
}

pub(crate) fn basic_credential(headers: &HeaderMap) -> Option<String> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let encoded = value.strip_prefix("Basic ")?;
    let decoded = general_purpose::STANDARD.decode(encoded).ok()?;
    String::from_utf8(decoded).ok()
}

pub(crate) fn bearer_token(headers: &HeaderMap) -> Option<String> {
    let raw = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))?;
    general_purpose::STANDARD
        .decode(raw)
        .ok()
        .and_then(|decoded| String::from_utf8(decoded).ok())
        .or_else(|| Some(raw.to_string()))
}
