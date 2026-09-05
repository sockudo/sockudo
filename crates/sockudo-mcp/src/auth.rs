//! Principals, scopes, and bearer-token authentication for the MCP surface.
//!
//! Every tool, resource, and prompt is gated by a [`Scope`]. A [`Principal`]
//! carries a [`ScopeSet`] plus an app allow-list so an operator can hand an
//! agent a token that, for example, may only read history for one app.

use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sockudo_core::token::secure_compare;

/// Permission level required by a tool or resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Scope {
    /// Inspect channels, history, presence, health, and metrics.
    Read,
    /// Publish events, mutate messages, publish annotations, send pushes.
    Write,
    /// Destructive or connection-affecting operations: terminate users,
    /// reset or purge history, revoke tokens, replay dead letters.
    Admin,
}

impl Scope {
    const fn bit(self) -> u8 {
        match self {
            Scope::Read => 0b001,
            Scope::Write => 0b010,
            Scope::Admin => 0b100,
        }
    }

    /// Stable name used in configuration, logs, and metrics labels.
    pub const fn as_str(self) -> &'static str {
        match self {
            Scope::Read => "read",
            Scope::Write => "write",
            Scope::Admin => "admin",
        }
    }

    /// Parse a configuration string (case-insensitive).
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "read" | "ro" | "readonly" | "read-only" => Some(Scope::Read),
            "write" | "rw" => Some(Scope::Write),
            "admin" => Some(Scope::Admin),
            _ => None,
        }
    }
}

/// Compact set of scopes. `admin` implies `write`, and `write` implies `read`,
/// mirroring the way operators reason about the levels.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct ScopeSet(u8);

impl ScopeSet {
    /// No permissions at all.
    pub const EMPTY: Self = Self(0);
    /// Read-only.
    pub const READ: Self = Self(Scope::Read.bit());
    /// Read and write.
    pub const READ_WRITE: Self = Self(Scope::Read.bit() | Scope::Write.bit());
    /// Every scope.
    pub const ALL: Self = Self(Scope::Read.bit() | Scope::Write.bit() | Scope::Admin.bit());
    /// Number of distinct bitmask values; used to size per-scope caches.
    pub const CARDINALITY: usize = 8;

    /// Build a set from explicit scopes, expanding implied levels.
    pub fn from_scopes<I: IntoIterator<Item = Scope>>(scopes: I) -> Self {
        scopes
            .into_iter()
            .fold(Self::EMPTY, |set, scope| set.with(scope))
    }

    /// Add a scope and everything it implies.
    #[must_use]
    pub const fn with(self, scope: Scope) -> Self {
        let bits = match scope {
            Scope::Read => Scope::Read.bit(),
            Scope::Write => Scope::Read.bit() | Scope::Write.bit(),
            Scope::Admin => Scope::Read.bit() | Scope::Write.bit() | Scope::Admin.bit(),
        };
        Self(self.0 | bits)
    }

    /// Whether the set grants `scope`.
    pub const fn allows(self, scope: Scope) -> bool {
        self.0 & scope.bit() != 0
    }

    /// Bitmask index in `0..CARDINALITY`, stable for cache addressing.
    pub const fn index(self) -> usize {
        self.0 as usize
    }

    /// Highest granted scope, if any.
    pub const fn highest(self) -> Option<Scope> {
        if self.allows(Scope::Admin) {
            Some(Scope::Admin)
        } else if self.allows(Scope::Write) {
            Some(Scope::Write)
        } else if self.allows(Scope::Read) {
            Some(Scope::Read)
        } else {
            None
        }
    }

    /// Scopes granted, lowest first.
    pub fn iter(self) -> impl Iterator<Item = Scope> {
        [Scope::Read, Scope::Write, Scope::Admin]
            .into_iter()
            .filter(move |scope| self.allows(*scope))
    }
}

impl fmt::Debug for ScopeSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set()
            .entries(self.iter().map(Scope::as_str))
            .finish()
    }
}

impl fmt::Display for ScopeSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for scope in self.iter() {
            if !first {
                f.write_str(",")?;
            }
            first = false;
            f.write_str(scope.as_str())?;
        }
        if first {
            f.write_str("none")?;
        }
        Ok(())
    }
}

/// Which Sockudo apps a principal may touch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppAccess {
    /// Every app known to the credential source.
    All,
    /// Only the listed app ids.
    Only(Arc<[String]>),
}

impl AppAccess {
    /// Build from a configuration list where `*` (or an empty list) means all.
    pub fn from_list<I, S>(apps: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let apps: Vec<String> = apps
            .into_iter()
            .map(Into::into)
            .map(|app| app.trim().to_string())
            .filter(|app| !app.is_empty())
            .collect();
        if apps.is_empty() || apps.iter().any(|app| app == "*") {
            AppAccess::All
        } else {
            AppAccess::Only(apps.into())
        }
    }

    /// Whether `app_id` may be accessed.
    pub fn allows(&self, app_id: &str) -> bool {
        match self {
            AppAccess::All => true,
            AppAccess::Only(apps) => apps.iter().any(|app| app == app_id),
        }
    }
}

/// An authenticated caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    /// Operator-facing name used in audit logs; never a secret.
    pub name: Arc<str>,
    /// Granted scopes.
    pub scopes: ScopeSet,
    /// App allow-list.
    pub apps: AppAccess,
}

impl Principal {
    /// Construct a principal.
    pub fn new(name: impl Into<Arc<str>>, scopes: ScopeSet, apps: AppAccess) -> Self {
        Self {
            name: name.into(),
            scopes,
            apps,
        }
    }

    /// Principal used for local, unauthenticated transports such as stdio.
    pub fn local(scopes: ScopeSet) -> Self {
        Self::new("local", scopes, AppAccess::All)
    }

    /// Check a scope requirement.
    pub fn require(&self, scope: Scope) -> Result<(), AuthError> {
        if self.scopes.allows(scope) {
            Ok(())
        } else {
            Err(AuthError::MissingScope(scope))
        }
    }

    /// Check an app requirement.
    pub fn require_app(&self, app_id: &str) -> Result<(), AuthError> {
        if self.apps.allows(app_id) {
            Ok(())
        } else {
            Err(AuthError::AppNotAllowed(app_id.to_string()))
        }
    }
}

/// Authentication and authorization failures.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AuthError {
    /// No credential was presented.
    #[error("missing bearer token")]
    Missing,
    /// The credential did not match any configured token.
    #[error("invalid bearer token")]
    Invalid,
    /// The principal lacks the scope a tool requires.
    #[error("this operation requires the '{}' scope", .0.as_str())]
    MissingScope(Scope),
    /// The principal may not touch the requested app.
    #[error("app '{0}' is outside this token's allow-list")]
    AppNotAllowed(String),
}

/// Resolves a presented credential to a [`Principal`].
pub trait Authenticator: Send + Sync + 'static {
    /// Authenticate the raw bearer credential, if any.
    fn authenticate(&self, credential: Option<&str>) -> Result<Principal, AuthError>;
}

/// Static bearer-token table. Comparison is constant-time and always walks the
/// whole table so response timing does not reveal which token matched.
pub struct TokenAuthenticator {
    entries: Vec<(String, Principal)>,
}

impl TokenAuthenticator {
    /// Build from `(token, principal)` pairs. Empty tokens are ignored.
    pub fn new<I>(entries: I) -> Self
    where
        I: IntoIterator<Item = (String, Principal)>,
    {
        Self {
            entries: entries
                .into_iter()
                .filter(|(token, _)| !token.is_empty())
                .collect(),
        }
    }

    /// Number of configured tokens.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no tokens are configured.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl fmt::Debug for TokenAuthenticator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenAuthenticator")
            .field("tokens", &self.entries.len())
            .finish_non_exhaustive()
    }
}

impl Authenticator for TokenAuthenticator {
    fn authenticate(&self, credential: Option<&str>) -> Result<Principal, AuthError> {
        let presented = credential.map(str::trim).filter(|value| !value.is_empty());
        let Some(presented) = presented else {
            return Err(AuthError::Missing);
        };
        let mut matched: Option<&Principal> = None;
        for (token, principal) in &self.entries {
            if secure_compare(token, presented) && matched.is_none() {
                matched = Some(principal);
            }
        }
        matched.cloned().ok_or(AuthError::Invalid)
    }
}

/// Accepts any (or no) credential and always yields the same principal. Meant
/// for stdio and explicitly opted-in anonymous development setups.
#[derive(Debug, Clone)]
pub struct StaticAuthenticator(pub Principal);

impl Authenticator for StaticAuthenticator {
    fn authenticate(&self, _credential: Option<&str>) -> Result<Principal, AuthError> {
        Ok(self.0.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_sets_expand_implied_levels() {
        assert!(ScopeSet::from_scopes([Scope::Admin]).allows(Scope::Read));
        assert!(ScopeSet::from_scopes([Scope::Write]).allows(Scope::Read));
        assert!(!ScopeSet::from_scopes([Scope::Write]).allows(Scope::Admin));
        assert!(!ScopeSet::READ.allows(Scope::Write));
        assert_eq!(ScopeSet::ALL.highest(), Some(Scope::Admin));
        assert_eq!(ScopeSet::EMPTY.highest(), None);
        assert_eq!(ScopeSet::READ_WRITE.to_string(), "read,write");
    }

    #[test]
    fn app_access_wildcard_and_lists() {
        assert_eq!(AppAccess::from_list(["*"]), AppAccess::All);
        assert_eq!(AppAccess::from_list(Vec::<String>::new()), AppAccess::All);
        let only = AppAccess::from_list(["app-1", " app-2 "]);
        assert!(only.allows("app-1"));
        assert!(only.allows("app-2"));
        assert!(!only.allows("app-3"));
    }

    #[test]
    fn token_authenticator_matches_exact_tokens_only() {
        let auth = TokenAuthenticator::new([
            (
                "secret-one".to_string(),
                Principal::new("one", ScopeSet::READ, AppAccess::All),
            ),
            (
                "secret-two".to_string(),
                Principal::new("two", ScopeSet::ALL, AppAccess::from_list(["app-1"])),
            ),
        ]);
        assert_eq!(auth.authenticate(None), Err(AuthError::Missing));
        assert_eq!(auth.authenticate(Some("")), Err(AuthError::Missing));
        assert_eq!(auth.authenticate(Some("secret")), Err(AuthError::Invalid));
        assert_eq!(
            auth.authenticate(Some("secret-one "))
                .unwrap()
                .name
                .as_ref(),
            "one"
        );
        let two = auth.authenticate(Some("secret-two")).unwrap();
        assert!(two.scopes.allows(Scope::Admin));
        assert!(two.require_app("app-2").is_err());
    }
}
