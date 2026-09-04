//! `reqwest`-backed transport for a Sockudo deployment reachable over HTTP.

use async_trait::async_trait;
use bytes::Bytes;
use url::Url;

use super::{ApiError, ApiTransport};

/// Sends signed requests to a remote Sockudo server.
#[derive(Debug, Clone)]
pub struct RemoteTransport {
    client: reqwest::Client,
    base_url: Url,
}

impl RemoteTransport {
    /// `base_url` may carry a path prefix (for example a reverse-proxy mount).
    pub fn new(base_url: Url, client: reqwest::Client) -> Self {
        Self { client, base_url }
    }

    /// Base URL requests are resolved against.
    pub fn base_url(&self) -> &Url {
        &self.base_url
    }

    fn resolve(&self, path_and_query: &str) -> Result<Url, ApiError> {
        let (path, query) = match path_and_query.split_once('?') {
            Some((path, query)) => (path, Some(query)),
            None => (path_and_query, None),
        };
        let mut url = self.base_url.clone();
        let base_path = url.path().trim_end_matches('/').to_string();
        url.set_path(&format!("{base_path}{path}"));
        url.set_query(query);
        Ok(url)
    }
}

#[async_trait]
impl ApiTransport for RemoteTransport {
    async fn send(&self, request: http::Request<Bytes>) -> Result<http::Response<Bytes>, ApiError> {
        let (parts, body) = request.into_parts();
        let target = parts
            .uri
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("/");
        let url = self.resolve(target)?;
        let mut builder = self.client.request(parts.method, url);
        for (name, value) in &parts.headers {
            if name == http::header::HOST {
                continue;
            }
            builder = builder.header(name, value);
        }
        if !body.is_empty() {
            builder = builder.body(body);
        }
        let response = builder
            .send()
            .await
            .map_err(|error| ApiError::Transport(error.to_string()))?;
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = response
            .bytes()
            .await
            .map_err(|error| ApiError::Transport(error.to_string()))?;
        let mut out = http::Response::new(bytes);
        *out.status_mut() = status;
        *out.headers_mut() = headers;
        Ok(out)
    }

    fn kind(&self) -> &'static str {
        "remote"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_against_prefixed_base_url() {
        let transport = RemoteTransport::new(
            Url::parse("https://rt.example.com/sockudo/").unwrap(),
            reqwest::Client::new(),
        );
        let url = transport
            .resolve("/apps/a/channels?auth_key=k&info=x")
            .unwrap();
        assert_eq!(
            url.as_str(),
            "https://rt.example.com/sockudo/apps/a/channels?auth_key=k&info=x"
        );
        let plain = RemoteTransport::new(
            Url::parse("http://127.0.0.1:6001").unwrap(),
            reqwest::Client::new(),
        );
        assert_eq!(
            plain.resolve("/up").unwrap().as_str(),
            "http://127.0.0.1:6001/up"
        );
    }
}
