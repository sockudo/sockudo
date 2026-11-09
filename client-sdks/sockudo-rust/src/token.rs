//! Capability-token authentication (Protocol V2).

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use serde::Deserialize;
use sonic_rs::JsonValueTrait;

use crate::error::{Error, Result};

/// A boxed, `Send` future.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Why a capability token is being requested.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenRequestReason {
    /// First connection.
    Initial,
    /// Reconnecting after a drop.
    Reconnect,
    /// Proactive refresh at ~80% of the token lifetime.
    Refresh,
    /// The server reported `40142` (token expired).
    Expired,
}

impl TokenRequestReason {
    /// Wire name used by `auth_url` requests.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Initial => "initial",
            Self::Reconnect => "reconnect",
            Self::Refresh => "refresh",
            Self::Expired => "expired",
        }
    }
}

/// Context handed to a [`TokenProvider`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenRequest {
    /// Current socket id, when connected.
    pub socket_id: Option<String>,
    /// Why the token is needed.
    pub reason: TokenRequestReason,
}

/// A capability token plus optional lifetime metadata.
///
/// When no lifetime is given, `exp`/`iat` claims are read from JWT tokens.
#[derive(Clone, Default, PartialEq, Deserialize)]
#[non_exhaustive]
pub struct TokenAuthData {
    /// The token.
    pub token: String,
    /// Expiry as Unix seconds.
    #[serde(default, alias = "expiresAt")]
    pub expires_at: Option<f64>,
    /// Expiry as Unix milliseconds.
    #[serde(default, alias = "expiresAtMs")]
    pub expires_at_ms: Option<u64>,
    /// Lifetime in seconds from `issued_at` (or now).
    #[serde(default, alias = "expiresIn")]
    pub expires_in: Option<f64>,
    /// Issue time as Unix seconds.
    #[serde(default, alias = "issuedAt")]
    pub issued_at: Option<f64>,
    /// Issue time as Unix milliseconds.
    #[serde(default, alias = "issuedAtMs")]
    pub issued_at_ms: Option<u64>,
    /// JWT-style expiry (Unix seconds).
    #[serde(default)]
    pub exp: Option<f64>,
    /// JWT-style issue time (Unix seconds).
    #[serde(default)]
    pub iat: Option<f64>,
}

impl fmt::Debug for TokenAuthData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never print the token itself.
        f.debug_struct("TokenAuthData")
            .field("token", &"<redacted>")
            .field("expires_at", &self.expires_at)
            .field("expires_at_ms", &self.expires_at_ms)
            .field("expires_in", &self.expires_in)
            .finish_non_exhaustive()
    }
}

impl TokenAuthData {
    /// A token without explicit lifetime metadata.
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
            ..Self::default()
        }
    }

    /// Set the lifetime from now (builder style).
    pub fn with_expires_in(mut self, lifetime: Duration) -> Self {
        self.expires_in = Some(lifetime.as_secs_f64());
        self
    }

    /// Set the absolute expiry (builder style).
    pub fn with_expires_at(mut self, at: SystemTime) -> Self {
        self.expires_at = Some(unix_seconds(at));
        self
    }

    /// Set the issue time (builder style).
    pub fn with_issued_at(mut self, at: SystemTime) -> Self {
        self.issued_at = Some(unix_seconds(at));
        self
    }
}

impl From<String> for TokenAuthData {
    fn from(token: String) -> Self {
        Self::new(token)
    }
}

impl From<&str> for TokenAuthData {
    fn from(token: &str) -> Self {
        Self::new(token)
    }
}

/// Supplies capability tokens on demand.
///
/// Implemented for any `Fn(TokenRequest) -> impl Future<Output = Result<TokenAuthData>>`.
pub trait TokenProvider: Send + Sync {
    /// Fetch a token.
    fn token(&self, request: TokenRequest) -> BoxFuture<'static, Result<TokenAuthData>>;
}

impl<F, Fut> TokenProvider for F
where
    F: Fn(TokenRequest) -> Fut + Send + Sync,
    Fut: Future<Output = Result<TokenAuthData>> + Send + 'static,
{
    fn token(&self, request: TokenRequest) -> BoxFuture<'static, Result<TokenAuthData>> {
        Box::pin(self(request))
    }
}

/// A token with resolved lifetime (Unix seconds).
#[derive(Clone, PartialEq)]
pub(crate) struct NormalizedToken {
    pub token: String,
    pub issued_at: Option<f64>,
    pub expires_at: Option<f64>,
}

impl fmt::Debug for NormalizedToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NormalizedToken")
            .field("issued_at", &self.issued_at)
            .field("expires_at", &self.expires_at)
            .finish_non_exhaustive()
    }
}

pub(crate) fn now_seconds() -> f64 {
    unix_seconds(SystemTime::now())
}

fn unix_seconds(at: SystemTime) -> f64 {
    at.duration_since(UNIX_EPOCH)
        .map_or(0.0, |elapsed| elapsed.as_secs_f64())
}

fn finite(value: Option<f64>) -> Option<f64> {
    value.filter(|value| value.is_finite())
}

/// Resolve lifetime metadata. Explicit milliseconds win over seconds, an
/// explicit expiry wins over `expires_in`, and JWT claims fill the gaps.
pub(crate) fn normalize(data: TokenAuthData, now: f64) -> Result<NormalizedToken> {
    if data.token.is_empty() {
        return Err(Error::TokenProvider(
            "capability-token provider returned an empty token".to_owned(),
        ));
    }
    let (claim_exp, claim_iat) = jwt_times(&data.token);
    let mut issued_at = data
        .issued_at_ms
        .map(|ms| ms as f64 / 1000.0)
        .or(finite(data.issued_at))
        .or(finite(data.iat));
    let mut expires_at = data
        .expires_at_ms
        .map(|ms| ms as f64 / 1000.0)
        .or(finite(data.expires_at))
        .or(finite(data.exp));
    if expires_at.is_none()
        && let Some(lifetime) = finite(data.expires_in)
    {
        let start = *issued_at.get_or_insert(now);
        expires_at = Some(start + lifetime);
    }
    if expires_at.is_none() {
        expires_at = claim_exp;
    }
    if issued_at.is_none() {
        issued_at = claim_iat;
    }
    Ok(NormalizedToken {
        token: data.token,
        issued_at,
        expires_at,
    })
}

/// Read `exp` / `iat` from a JWT payload without verifying it.
pub(crate) fn jwt_times(token: &str) -> (Option<f64>, Option<f64>) {
    let mut parts = token.split('.');
    let (Some(_), Some(payload), Some(_)) = (parts.next(), parts.next(), parts.next()) else {
        return (None, None);
    };
    let trimmed = payload.trim_end_matches('=');
    let Ok(decoded) = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(trimmed) else {
        return (None, None);
    };
    let Ok(claims) = sonic_rs::from_slice::<sonic_rs::Value>(&decoded) else {
        return (None, None);
    };
    let number = |key: &str| {
        claims
            .get(key)
            .and_then(|value| value.as_f64())
            .filter(|v| v.is_finite())
    };
    (number("exp"), number("iat"))
}

/// When to refresh: 80% through the lifetime, but no later than `leeway`
/// before expiry (when that is still in the future), and never sooner than 1s.
pub(crate) fn refresh_delay(
    token: &NormalizedToken,
    now: f64,
    leeway: Duration,
) -> Option<Duration> {
    let expires_at = token.expires_at?;
    let issued_at = token.issued_at.unwrap_or(now);
    let lifetime = expires_at - issued_at;
    if lifetime <= 0.0 {
        return Some(Duration::from_secs(1));
    }
    let mut refresh_at = issued_at + lifetime * 0.8;
    let latest = expires_at - leeway.as_secs_f64();
    if latest > now {
        refresh_at = refresh_at.min(latest);
    }
    let seconds = (refresh_at - now).max(1.0);
    Some(Duration::from_secs_f64(seconds))
}

/// Apply an `auth_success` payload to the current token state.
pub(crate) fn apply_auth_success(
    current: &mut Option<NormalizedToken>,
    payload: &sonic_rs::Value,
    now: f64,
) {
    let number = |key: &str| {
        payload
            .get(key)
            .and_then(|value| value.as_f64())
            .filter(|v| v.is_finite())
    };
    let token = payload
        .get("token")
        .and_then(|value| value.as_str())
        .map(str::to_owned);
    let mut issued_at = number("issued_at_ms")
        .map(|ms| ms / 1000.0)
        .or_else(|| number("issued_at"))
        .or_else(|| number("iat"));
    let mut expires_at = number("expires_at_ms")
        .map(|ms| ms / 1000.0)
        .or_else(|| number("expires_at"))
        .or_else(|| number("exp"));
    if expires_at.is_none()
        && let Some(lifetime) = number("expires_in")
    {
        let start = *issued_at.get_or_insert(now);
        expires_at = Some(start + lifetime);
    }
    if expires_at.is_none()
        && let Some(token) = token.as_deref()
    {
        let (claim_exp, claim_iat) = jwt_times(token);
        expires_at = claim_exp;
        issued_at = issued_at.or(claim_iat);
    }
    let state = match current {
        Some(state) => state,
        None => match token.clone() {
            Some(token) => current.insert(NormalizedToken {
                token,
                issued_at: None,
                expires_at: None,
            }),
            None => return,
        },
    };
    if let Some(token) = token {
        state.token = token;
    }
    if expires_at.is_some() {
        state.expires_at = expires_at;
    }
    if issued_at.is_some() {
        state.issued_at = issued_at;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn jwt(claims: &str) -> String {
        let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        format!(
            "{}.{}.sig",
            engine.encode(br#"{"alg":"none"}"#),
            engine.encode(claims.as_bytes())
        )
    }

    #[test]
    fn rejects_empty_tokens() {
        assert!(normalize(TokenAuthData::new(""), 0.0).is_err());
    }

    #[test]
    fn explicit_lifetime_wins_over_claims() {
        let token = jwt(r#"{"exp":5000,"iat":4000}"#);
        let data = TokenAuthData {
            expires_at_ms: Some(9_000_000),
            ..TokenAuthData::new(token.clone())
        };
        let normalized = normalize(data, 100.0).unwrap();
        assert_eq!(normalized.expires_at, Some(9000.0));
        assert_eq!(normalized.issued_at, Some(4000.0));
        let from_claims = normalize(TokenAuthData::new(token), 100.0).unwrap();
        assert_eq!(from_claims.expires_at, Some(5000.0));
    }

    #[test]
    fn expires_in_is_relative_to_issue_time_or_now() {
        let normalized = normalize(
            TokenAuthData::new("opaque").with_expires_in(Duration::from_secs(300)),
            1000.0,
        )
        .unwrap();
        assert_eq!(normalized.issued_at, Some(1000.0));
        assert_eq!(normalized.expires_at, Some(1300.0));
    }

    #[test]
    fn deserializes_snake_and_camel_case() {
        let snake: TokenAuthData =
            sonic_rs::from_str(r#"{"token":"t","expires_in":60,"issued_at_ms":1000}"#).unwrap();
        assert_eq!(snake.expires_in, Some(60.0));
        assert_eq!(snake.issued_at_ms, Some(1000));
        let camel: TokenAuthData =
            sonic_rs::from_str(r#"{"token":"t","expiresAtMs":2000,"issuedAt":1}"#).unwrap();
        assert_eq!(camel.expires_at_ms, Some(2000));
        assert_eq!(camel.issued_at, Some(1.0));
    }

    #[test]
    fn refresh_delay_follows_eighty_percent_and_leeway() {
        let leeway = Duration::from_secs(30);
        let token = NormalizedToken {
            token: "t".into(),
            issued_at: Some(0.0),
            expires_at: Some(1000.0),
        };
        // 80% of 1000s = 800s; leeway bound (970s) is later.
        assert_eq!(
            refresh_delay(&token, 0.0, leeway),
            Some(Duration::from_secs(800))
        );
        // Short token: leeway bound (30s) is earlier than 80% (48s).
        let short = NormalizedToken {
            expires_at: Some(60.0),
            ..token.clone()
        };
        assert_eq!(
            refresh_delay(&short, 0.0, leeway),
            Some(Duration::from_secs(30))
        );
        // Past refresh point: clamp to 1s.
        assert_eq!(
            refresh_delay(&token, 999.0, leeway),
            Some(Duration::from_secs(1))
        );
        // No expiry: no refresh.
        let opaque = NormalizedToken {
            expires_at: None,
            ..token
        };
        assert_eq!(refresh_delay(&opaque, 0.0, leeway), None);
    }

    #[test]
    fn auth_success_updates_lifetime() {
        let mut state = Some(NormalizedToken {
            token: "old".into(),
            issued_at: None,
            expires_at: None,
        });
        apply_auth_success(
            &mut state,
            &sonic_rs::json!({"token": "new", "expires_in": 120}),
            10.0,
        );
        let state = state.unwrap();
        assert_eq!(state.token, "new");
        assert_eq!(state.issued_at, Some(10.0));
        assert_eq!(state.expires_at, Some(130.0));
    }

    #[test]
    fn debug_never_prints_tokens() {
        let rendered = format!("{:?}", TokenAuthData::new("secret-token"));
        assert!(!rendered.contains("secret-token"));
    }
}
