use crate::app::App;
use crate::error::{Error, Result};
use crate::websocket::ConnectionCapabilities;
use ahash::AHashMap;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::Deserialize;
use std::time::{SystemTime, UNIX_EPOCH};

pub const MAX_CAPABILITY_TOKEN_BYTES: usize = 8 * 1024;
pub const MAX_CAPABILITY_PATTERNS: usize = 100;
pub const MAX_CLIENT_ID_BYTES: usize = 128;
pub const MAX_TOKEN_LIFETIME_SECONDS: i64 = 24 * 60 * 60;
pub const TOKEN_CLOCK_SKEW_SECONDS: i64 = 30;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenAuthContext {
    pub client_id: String,
    pub capabilities: ConnectionCapabilities,
    pub jti: String,
    pub exp: i64,
}

#[derive(Debug, Deserialize)]
struct CapabilityTokenClaims {
    #[serde(rename = "x-sockudo-capability")]
    capability: String,
    #[serde(rename = "x-sockudo-client-id")]
    client_id: String,
    iat: i64,
    exp: i64,
    #[serde(default)]
    nbf: Option<i64>,
    jti: String,
}

pub fn validate_capability_token(token: &str, app: &App) -> Result<TokenAuthContext> {
    if token.len() > MAX_CAPABILITY_TOKEN_BYTES {
        return Err(Error::Auth("capability token exceeds 8 KiB".to_string()));
    }

    let header = decode_header(token)
        .map_err(|_| Error::Auth("capability token has an invalid header".to_string()))?;
    if header.alg != Algorithm::HS256 {
        return Err(Error::Auth("capability token must use HS256".to_string()));
    }
    if header.kid.as_deref() != Some(app.key.as_str()) {
        return Err(Error::Auth(
            "capability token kid does not match app key".to_string(),
        ));
    }

    let mut validation = Validation::new(Algorithm::HS256);
    validation.leeway = TOKEN_CLOCK_SKEW_SECONDS as u64;
    validation.validate_exp = true;
    validation.validate_nbf = true;
    validation.set_required_spec_claims(&["exp"]);

    let decoded = decode::<CapabilityTokenClaims>(
        token,
        &DecodingKey::from_secret(app.secret.as_bytes()),
        &validation,
    )
    .map_err(|_| Error::Auth("capability token signature or claims are invalid".to_string()))?;

    let now = now_seconds()?;
    validate_claims(decoded.claims, now)
}

fn validate_claims(claims: CapabilityTokenClaims, now: i64) -> Result<TokenAuthContext> {
    if claims.client_id.is_empty() || claims.client_id.len() > MAX_CLIENT_ID_BYTES {
        return Err(Error::Auth(
            "x-sockudo-client-id must be 1..=128 bytes".to_string(),
        ));
    }
    if claims.jti.is_empty() {
        return Err(Error::Auth("jti is required".to_string()));
    }
    if claims.iat > now + TOKEN_CLOCK_SKEW_SECONDS {
        return Err(Error::Auth(
            "capability token iat is too far in the future".to_string(),
        ));
    }
    if claims.exp <= now - TOKEN_CLOCK_SKEW_SECONDS {
        return Err(Error::Auth("capability token is expired".to_string()));
    }
    if claims.exp - claims.iat > MAX_TOKEN_LIFETIME_SECONDS {
        return Err(Error::Auth(
            "capability token lifetime exceeds 24 hours".to_string(),
        ));
    }
    if let Some(nbf) = claims.nbf
        && nbf > now + TOKEN_CLOCK_SKEW_SECONDS
    {
        return Err(Error::Auth(
            "capability token is not active yet".to_string(),
        ));
    }

    let capabilities = parse_capability_map(&claims.capability)?;
    Ok(TokenAuthContext {
        client_id: claims.client_id,
        capabilities,
        jti: claims.jti,
        exp: claims.exp,
    })
}

fn parse_capability_map(input: &str) -> Result<ConnectionCapabilities> {
    let grants: AHashMap<String, Vec<String>> = sonic_rs::from_str(input).map_err(|_| {
        Error::Auth("x-sockudo-capability must be a JSON object string".to_string())
    })?;
    if grants.len() > MAX_CAPABILITY_PATTERNS {
        return Err(Error::Auth(
            "x-sockudo-capability exceeds 100 patterns".to_string(),
        ));
    }

    let mut capabilities = ConnectionCapabilities {
        subscribe: Some(Vec::new()),
        publish: Some(Vec::new()),
        history: Some(Vec::new()),
        presence: Some(Vec::new()),
        ..Default::default()
    };

    for (pattern, operations) in grants {
        if pattern.is_empty() {
            return Err(Error::Auth(
                "capability patterns must not be empty".to_string(),
            ));
        }
        for operation in operations {
            match operation.as_str() {
                "subscribe" => capabilities
                    .subscribe
                    .as_mut()
                    .unwrap()
                    .push(pattern.clone()),
                "publish" => capabilities.publish.as_mut().unwrap().push(pattern.clone()),
                "history" => capabilities.history.as_mut().unwrap().push(pattern.clone()),
                "presence" => capabilities
                    .presence
                    .as_mut()
                    .unwrap()
                    .push(pattern.clone()),
                _ => {
                    return Err(Error::Auth(format!(
                        "unsupported capability operation '{operation}'"
                    )));
                }
            }
        }
    }

    Ok(capabilities)
}

fn now_seconds() -> Result<i64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .map_err(|_| Error::Internal("system clock is before unix epoch".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header, encode};
    use serde::Serialize;

    #[derive(Serialize)]
    struct TestClaims<'a> {
        #[serde(rename = "x-sockudo-capability")]
        capability: &'a str,
        #[serde(rename = "x-sockudo-client-id")]
        client_id: &'a str,
        iat: i64,
        exp: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        nbf: Option<i64>,
        jti: &'a str,
    }

    fn app() -> App {
        App::from_policy(
            "app".to_string(),
            "app-key".to_string(),
            "app-secret".to_string(),
            true,
            Default::default(),
        )
    }

    fn token_with(claims: TestClaims<'_>, alg: Algorithm, kid: &str, secret: &str) -> String {
        let mut header = Header::new(alg);
        header.kid = Some(kid.to_string());
        encode(
            &header,
            &claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap()
    }

    fn claims(now: i64, capability: &str) -> TestClaims<'_> {
        TestClaims {
            capability,
            client_id: "client-1",
            iat: now,
            exp: now + 3600,
            nbf: None,
            jti: "jti-1",
        }
    }

    #[test]
    fn validates_hs256_token_and_builds_capabilities() {
        let now = now_seconds().unwrap();
        let token = token_with(
            claims(
                now,
                r#"{"room:*":["subscribe","history"],"private-room":["publish"],"presence-room":["presence"]}"#,
            ),
            Algorithm::HS256,
            "app-key",
            "app-secret",
        );

        let context = validate_capability_token(&token, &app()).unwrap();
        assert_eq!(context.client_id, "client-1");
        assert!(context.capabilities.allows_subscribe("room:1"));
        assert!(context.capabilities.allows_history("room:1"));
        assert!(context.capabilities.allows_publish("private-room"));
        assert!(context.capabilities.allows_subscribe("presence-room"));
        assert!(!context.capabilities.allows_publish("room:1"));
    }

    #[test]
    fn rejects_wrong_kid() {
        let now = now_seconds().unwrap();
        let token = token_with(
            claims(now, r#"{"*":["subscribe"]}"#),
            Algorithm::HS256,
            "other-key",
            "app-secret",
        );

        assert!(validate_capability_token(&token, &app()).is_err());
    }

    #[test]
    fn rejects_non_hs256_algorithm() {
        let now = now_seconds().unwrap();
        let token = token_with(
            claims(now, r#"{"*":["subscribe"]}"#),
            Algorithm::HS384,
            "app-key",
            "app-secret",
        );

        assert!(validate_capability_token(&token, &app()).is_err());
    }

    #[test]
    fn rejects_unsupported_operation() {
        let now = now_seconds().unwrap();
        let token = token_with(
            claims(now, r#"{"*":["subscribe","admin"]}"#),
            Algorithm::HS256,
            "app-key",
            "app-secret",
        );

        assert!(validate_capability_token(&token, &app()).is_err());
    }

    #[test]
    fn rejects_missing_jti() {
        let now = now_seconds().unwrap();
        let token = token_with(
            TestClaims {
                jti: "",
                ..claims(now, r#"{"*":["subscribe"]}"#)
            },
            Algorithm::HS256,
            "app-key",
            "app-secret",
        );

        assert!(validate_capability_token(&token, &app()).is_err());
    }

    #[test]
    fn rejects_lifetime_over_24_hours() {
        let now = now_seconds().unwrap();
        let mut test_claims = claims(now, r#"{"*":["subscribe"]}"#);
        test_claims.exp = now + MAX_TOKEN_LIFETIME_SECONDS + 1;
        let token = token_with(test_claims, Algorithm::HS256, "app-key", "app-secret");

        assert!(validate_capability_token(&token, &app()).is_err());
    }

    #[test]
    fn wildcard_prefix_does_not_match_bare_namespace() {
        let capabilities = parse_capability_map(r#"{"ns:*":["subscribe"]}"#).unwrap();

        assert!(!capabilities.allows_subscribe("ns"));
        assert!(capabilities.allows_subscribe("ns:one"));
        assert!(!capabilities.allows_subscribe("NS:one"));
    }
}
