use crate::app::App;
use crate::utils::{parse_bool_env, parse_env, parse_env_optional};
use std::str::FromStr;
use tracing::{info, warn};

use super::*;

mod adapters;
mod apps;
mod core;
mod databases;
mod drivers;
mod features;
mod history;
mod maintenance;
mod queues;
mod runtime;

// Helper function to parse driver enums with fallback behavior (matches main.rs)
fn parse_driver_enum<T: FromStr + Clone + std::fmt::Debug>(
    driver_str: String,
    default_driver: T,
    driver_name: &str,
) -> T
where
    <T as FromStr>::Err: std::fmt::Debug,
{
    match T::from_str(&driver_str.to_lowercase()) {
        Ok(driver_enum) => driver_enum,
        Err(_) => {
            warn!(
                driver_name = driver_name,
                reason = "parse_failed",
                "driver config parse failed, using default"
            );
            default_driver
        }
    }
}

fn override_db_pool_settings(db_conn: &mut DatabaseConnection, prefix: &str) {
    if let Some(min) = parse_env_optional::<u32>(&format!("{}_POOL_MIN", prefix)) {
        db_conn.pool_min = Some(min);
    }
    if let Some(max) = parse_env_optional::<u32>(&format!("{}_POOL_MAX", prefix)) {
        db_conn.pool_max = Some(max);
    }
}

impl ServerOptions {
    pub async fn override_from_env(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        core::apply(self)?;
        drivers::apply(self)?;
        databases::apply(self)?;
        runtime::apply(self)?;
        queues::apply(self)?;
        adapters::apply(self)?;
        apps::apply(self)?;
        maintenance::apply(self)?;
        history::apply(self)?;
        features::apply(self)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::ServerOptions;
    use crate::app::{App, AppPolicy};

    const APP_BOOTSTRAP_ENV_KEYS: &[&str] = &[
        "SOCKUDO_DEFAULT_APP_ID",
        "SOCKUDO_DEFAULT_APP_KEY",
        "SOCKUDO_DEFAULT_APP_SECRET",
        "SOCKUDO_DEFAULT_APP_ENABLED",
        "SOCKUDO_SKIP_INLINE_APPS",
        "APP_MANAGER_REGISTER_INLINE_APPS",
    ];

    struct EnvGuard {
        previous: Vec<(&'static str, Option<String>)>,
    }

    impl EnvGuard {
        fn app_bootstrap(overrides: &[(&'static str, &'static str)]) -> Self {
            let previous = APP_BOOTSTRAP_ENV_KEYS
                .iter()
                .map(|key| (*key, std::env::var(key).ok()))
                .collect();

            // SAFETY: These tests isolate the app-bootstrap environment keys
            // before applying per-test overrides and restore them in Drop.
            unsafe {
                for key in APP_BOOTSTRAP_ENV_KEYS {
                    std::env::remove_var(key);
                }
                for (key, value) in overrides {
                    std::env::set_var(key, value);
                }
            }

            Self { previous }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            // SAFETY: Restores each key to its pre-test value or removes it if
            // it did not exist before the test.
            unsafe {
                for (key, value) in &self.previous {
                    if let Some(value) = value {
                        std::env::set_var(key, value);
                    } else {
                        std::env::remove_var(key);
                    }
                }
            }
        }
    }

    fn inline_test_app() -> App {
        App::from_policy(
            "app-id".to_string(),
            "app-key".to_string(),
            "app-secret".to_string(),
            true,
            AppPolicy::default(),
        )
    }

    #[tokio::test]
    async fn app_bootstrap_env_overrides_inline_apps() {
        {
            let _env = EnvGuard::app_bootstrap(&[("SOCKUDO_DEFAULT_APP_ENABLED", "false")]);
            let mut options = ServerOptions::default();
            options.app_manager.array.apps.push(inline_test_app());

            options.override_from_env().await.unwrap();

            assert!(options.app_manager.array.apps.is_empty());
        }

        {
            let _env = EnvGuard::app_bootstrap(&[("SOCKUDO_DEFAULT_APP_ENABLED", "true")]);
            let mut options = ServerOptions::default();
            options.app_manager.array.apps.push(inline_test_app());

            options.override_from_env().await.unwrap();

            assert_eq!(options.app_manager.array.apps.len(), 1);
            assert_eq!(options.app_manager.array.apps[0].id, "app-id");
        }

        {
            let _env = EnvGuard::app_bootstrap(&[
                ("SOCKUDO_DEFAULT_APP_ID", "prod-app"),
                ("SOCKUDO_DEFAULT_APP_KEY", "prod-key"),
                ("SOCKUDO_DEFAULT_APP_SECRET", "prod-secret"),
                ("SOCKUDO_DEFAULT_APP_ENABLED", "true"),
            ]);
            let mut options = ServerOptions::default();
            options.app_manager.array.apps.push(inline_test_app());

            options.override_from_env().await.unwrap();

            assert_eq!(options.app_manager.array.apps.len(), 1);
            let app = &options.app_manager.array.apps[0];
            assert_eq!(app.id, "prod-app");
            assert_eq!(app.key, "prod-key");
            assert_eq!(app.secret, "prod-secret");
            assert!(app.enabled);
        }

        {
            let _env = EnvGuard::app_bootstrap(&[("APP_MANAGER_REGISTER_INLINE_APPS", "false")]);
            let mut options = ServerOptions::default();
            options.app_manager.array.apps.push(inline_test_app());

            options.override_from_env().await.unwrap();

            assert!(options.app_manager.array.apps.is_empty());
        }
    }

    #[tokio::test]
    async fn websocket_rate_limit_trust_hops_overrides_from_env() {
        let previous = std::env::var("RATE_LIMITER_WS_TRUST_HOPS").ok();
        // SAFETY: This test controls the environment variable lifecycle for a
        // single key and restores the prior value before it returns.
        unsafe { std::env::set_var("RATE_LIMITER_WS_TRUST_HOPS", "2") };

        let mut options = ServerOptions::default();
        options.override_from_env().await.unwrap();

        if let Some(previous) = previous {
            // SAFETY: Restoring the pre-test value for the same key.
            unsafe { std::env::set_var("RATE_LIMITER_WS_TRUST_HOPS", previous) };
        } else {
            // SAFETY: Removing the test-only environment variable before exit.
            unsafe { std::env::remove_var("RATE_LIMITER_WS_TRUST_HOPS") };
        }

        assert_eq!(
            options.rate_limiter.websocket_rate_limit.trust_hops,
            Some(2)
        );
    }
}
