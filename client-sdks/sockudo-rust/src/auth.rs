//! Channel authorization and user authentication.
//!
//! Protected channels (`private-*`, `presence-*`, `private-encrypted-*`) and
//! user sign-in need a signature from **your** backend, which holds the app
//! secret. Configure either an HTTP endpoint (feature `http`) or a custom
//! handler that calls your backend however you like.

use std::fmt;
use std::future::Future;
use std::sync::Arc;

use serde::Deserialize;

use crate::error::Result;
use crate::token::BoxFuture;

/// Context for a channel authorization request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelAuthRequest {
    /// The connection's socket id.
    pub socket_id: String,
    /// The channel being subscribed.
    pub channel_name: String,
}

/// A signed channel authorization.
#[derive(Clone, PartialEq, Eq, Deserialize)]
pub struct ChannelAuthData {
    /// `app_key:signature`.
    pub auth: String,
    /// Presence member data (JSON text with `user_id` and `user_info`).
    #[serde(default)]
    pub channel_data: Option<String>,
    /// Base64 32-byte key for `private-encrypted-*` channels.
    #[serde(default)]
    pub shared_secret: Option<String>,
}

impl fmt::Debug for ChannelAuthData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChannelAuthData")
            .field("auth", &"<redacted>")
            .field("channel_data", &self.channel_data)
            .field(
                "shared_secret",
                &self.shared_secret.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl ChannelAuthData {
    /// Authorization for a private channel.
    pub fn new(auth: impl Into<String>) -> Self {
        Self {
            auth: auth.into(),
            channel_data: None,
            shared_secret: None,
        }
    }

    /// Attach presence channel data (builder style).
    pub fn with_channel_data(mut self, channel_data: impl Into<String>) -> Self {
        self.channel_data = Some(channel_data.into());
        self
    }

    /// Attach an encrypted-channel shared secret (builder style).
    pub fn with_shared_secret(mut self, shared_secret: impl Into<String>) -> Self {
        self.shared_secret = Some(shared_secret.into());
        self
    }
}

/// Context for a user authentication request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserAuthRequest {
    /// The connection's socket id.
    pub socket_id: String,
}

/// A signed user authentication.
#[derive(Clone, PartialEq, Eq, Deserialize)]
pub struct UserAuthData {
    /// `app_key:signature`.
    pub auth: String,
    /// User data JSON text; must contain a string `id`.
    pub user_data: String,
}

impl fmt::Debug for UserAuthData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UserAuthData")
            .field("auth", &"<redacted>")
            .field("user_data", &self.user_data)
            .finish()
    }
}

impl UserAuthData {
    /// Build user authentication data.
    pub fn new(auth: impl Into<String>, user_data: impl Into<String>) -> Self {
        Self {
            auth: auth.into(),
            user_data: user_data.into(),
        }
    }
}

/// Produces channel authorizations.
///
/// Implemented for `Fn(ChannelAuthRequest) -> impl Future<Output = Result<ChannelAuthData>>`.
pub trait ChannelAuthorizer: Send + Sync {
    /// Authorize a subscription.
    fn authorize(&self, request: ChannelAuthRequest)
    -> BoxFuture<'static, Result<ChannelAuthData>>;
}

impl<F, Fut> ChannelAuthorizer for F
where
    F: Fn(ChannelAuthRequest) -> Fut + Send + Sync,
    Fut: Future<Output = Result<ChannelAuthData>> + Send + 'static,
{
    fn authorize(
        &self,
        request: ChannelAuthRequest,
    ) -> BoxFuture<'static, Result<ChannelAuthData>> {
        Box::pin(self(request))
    }
}

/// Produces user authentications.
///
/// Implemented for `Fn(UserAuthRequest) -> impl Future<Output = Result<UserAuthData>>`.
pub trait UserAuthenticator: Send + Sync {
    /// Authenticate the connection's user.
    fn authenticate(&self, request: UserAuthRequest) -> BoxFuture<'static, Result<UserAuthData>>;
}

impl<F, Fut> UserAuthenticator for F
where
    F: Fn(UserAuthRequest) -> Fut + Send + Sync,
    Fut: Future<Output = Result<UserAuthData>> + Send + 'static,
{
    fn authenticate(&self, request: UserAuthRequest) -> BoxFuture<'static, Result<UserAuthData>> {
        Box::pin(self(request))
    }
}

/// Computes header pairs at request time (e.g. a fresh session token).
pub type HeadersProvider = Arc<dyn Fn() -> Vec<(String, String)> + Send + Sync>;

/// Computes extra form parameters at request time.
pub type ParamsProvider = Arc<dyn Fn() -> Vec<(String, String)> + Send + Sync>;

/// An HTTP endpoint plus static and dynamic headers/params.
#[derive(Clone, Default)]
pub struct Endpoint {
    /// Absolute URL.
    pub url: String,
    /// Static headers.
    pub headers: Vec<(String, String)>,
    /// Static form parameters (auth endpoints only).
    pub params: Vec<(String, String)>,
    /// Headers computed per request.
    pub headers_provider: Option<HeadersProvider>,
    /// Params computed per request (auth endpoints only).
    pub params_provider: Option<ParamsProvider>,
}

impl fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Header values often carry credentials; print names only.
        f.debug_struct("Endpoint")
            .field("url", &self.url)
            .field(
                "headers",
                &self
                    .headers
                    .iter()
                    .map(|(name, _)| name)
                    .collect::<Vec<_>>(),
            )
            .field("params", &self.params.len())
            .field("headers_provider", &self.headers_provider.is_some())
            .field("params_provider", &self.params_provider.is_some())
            .finish()
    }
}

impl Endpoint {
    /// An endpoint at `url`.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            ..Self::default()
        }
    }

    /// Add a static header (builder style).
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// Add a static form parameter (builder style).
    pub fn param(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.params.push((name.into(), value.into()));
        self
    }

    /// Compute headers per request (builder style).
    pub fn headers_provider(
        mut self,
        provider: impl Fn() -> Vec<(String, String)> + Send + Sync + 'static,
    ) -> Self {
        self.headers_provider = Some(Arc::new(provider));
        self
    }

    /// Compute params per request (builder style).
    pub fn params_provider(
        mut self,
        provider: impl Fn() -> Vec<(String, String)> + Send + Sync + 'static,
    ) -> Self {
        self.params_provider = Some(Arc::new(provider));
        self
    }

    #[cfg_attr(not(feature = "http"), allow(dead_code))]
    pub(crate) fn resolved_headers(&self) -> Vec<(String, String)> {
        let mut headers = self.headers.clone();
        if let Some(provider) = self.headers_provider.as_ref() {
            headers.extend(provider());
        }
        headers
    }

    #[cfg_attr(not(feature = "http"), allow(dead_code))]
    pub(crate) fn resolved_params(&self) -> Vec<(String, String)> {
        let mut params = self.params.clone();
        if let Some(provider) = self.params_provider.as_ref() {
            params.extend(provider());
        }
        params
    }
}

impl From<&str> for Endpoint {
    fn from(url: &str) -> Self {
        Self::new(url)
    }
}

impl From<String> for Endpoint {
    fn from(url: String) -> Self {
        Self::new(url)
    }
}
