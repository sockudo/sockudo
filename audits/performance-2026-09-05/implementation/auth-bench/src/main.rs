#![allow(dead_code)]
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
mod domain {
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct SecretString(String);
    impl SecretString {
        pub fn new(s: String) -> Result<Self, String> {
            Ok(Self(s))
        }
        pub fn expose_secret(&self) -> &str {
            &self.0
        }
    }
    #[derive(Clone, Debug)]
    pub enum ProviderFailureClass {
        CredentialAuth,
    }
    pub struct ProviderError {
        pub class: String,
        pub failure_class: ProviderFailureClass,
        pub reason: Option<String>,
        pub retry_after_ms: Option<u64>,
    }
}
#[path = "/home/radud/Desktop/Code/Rust/sockudo/crates/sockudo-push/src/dispatch/auth.rs"]
mod auth;
struct Source {
    count: AtomicUsize,
    fail: bool,
}
#[async_trait::async_trait]
impl auth::ProviderTokenSource for Source {
    async fn fetch_token(
        &self,
        _: u64,
    ) -> Result<auth::ProviderAccessToken, auth::ProviderAuthError> {
        self.count.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(5)).await;
        if self.fail {
            Err(auth::ProviderAuthError {
                class: "auth_failure",
                reason: "simulated outage".to_owned(),
            })
        } else {
            Ok(auth::ProviderAccessToken {
                token: domain::SecretString::new("test-token".into()).unwrap(),
                expires_at_ms: u64::MAX,
            })
        }
    }
}
fn main() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    rt.block_on(async {
        for fail in [false, true] {
            for n in [1, 16, 64] {
                for rep in 0..5 {
                    let source = Arc::new(Source {
                        count: AtomicUsize::new(0),
                        fail,
                    });
                    let cache = auth::CachedTokenProvider::new(source.clone());
                    let start = Instant::now();
                    let mut tasks = tokio::task::JoinSet::new();
                    for _ in 0..n {
                        let cache = cache.clone();
                        tasks.spawn(async move { cache.bearer_token(1_000).await.is_ok() });
                    }
                    while tasks.join_next().await.is_some() {}
                    println!(
                        "fail={fail},jobs={n},rep={rep},fetches={},elapsed_ms={:.3}",
                        source.count.load(Ordering::Relaxed),
                        start.elapsed().as_secs_f64() * 1000.0
                    );
                }
            }
        }
    });
}
