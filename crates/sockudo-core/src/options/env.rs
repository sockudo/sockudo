use crate::app::App;
use crate::utils::{parse_bool_env, parse_env, parse_env_optional};
use std::str::FromStr;
use tracing::{info, warn};

use super::*;

mod ably_compat;
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
        ably_compat::apply(self)?;
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
    const OPENTELEMETRY_ENV_KEYS: &[&str] = &[
        "SOCKUDO_OTEL_ENABLED",
        "SOCKUDO_OTEL_TRACES_ENABLED",
        "SOCKUDO_OTEL_METRICS_ENABLED",
        "SOCKUDO_OTEL_LOGS_ENABLED",
        "SOCKUDO_OTEL_SERVICE_NAME",
        "SOCKUDO_OTEL_SERVICE_NAMESPACE",
        "SOCKUDO_OTEL_DEPLOYMENT_ENVIRONMENT",
        "SOCKUDO_OTEL_RESOURCE_ATTRIBUTES",
        "SOCKUDO_OTEL_ENDPOINT",
        "SOCKUDO_OTEL_EXPORT_TIMEOUT_MS",
        "SOCKUDO_OTEL_BATCH_SCHEDULED_DELAY_MS",
        "SOCKUDO_OTEL_BATCH_MAX_QUEUE_SIZE",
        "SOCKUDO_OTEL_BATCH_MAX_EXPORT_BATCH_SIZE",
        "SOCKUDO_OTEL_METRIC_EXPORT_INTERVAL_MS",
        "SOCKUDO_OTEL_PROPAGATION_TRACE_CONTEXT",
        "SOCKUDO_OTEL_PROPAGATION_BAGGAGE",
        "OTEL_SERVICE_NAME",
    ];

    struct EnvGuard {
        previous: Vec<(&'static str, Option<String>)>,
    }

    impl EnvGuard {
        fn isolated(
            keys: &'static [&'static str],
            overrides: &[(&'static str, &'static str)],
        ) -> Self {
            let previous = keys
                .iter()
                .map(|key| (*key, std::env::var(key).ok()))
                .collect();

            // SAFETY: These tests isolate the selected environment keys
            // before applying per-test overrides and restore them in Drop.
            unsafe {
                for key in keys {
                    std::env::remove_var(key);
                }
                for (key, value) in overrides {
                    std::env::set_var(key, value);
                }
            }

            Self { previous }
        }

        fn app_bootstrap(overrides: &[(&'static str, &'static str)]) -> Self {
            Self::isolated(APP_BOOTSTRAP_ENV_KEYS, overrides)
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

    #[tokio::test]
    async fn opentelemetry_env_overrides_are_sockudo_scoped() {
        {
            let _env = EnvGuard::isolated(
                OPENTELEMETRY_ENV_KEYS,
                &[("OTEL_SERVICE_NAME", "sdk-owned-service")],
            );
            let mut options = ServerOptions::default();

            options.override_from_env().await.unwrap();

            assert_eq!(options.opentelemetry.service_name, "sockudo");
        }

        {
            let _env = EnvGuard::isolated(
                OPENTELEMETRY_ENV_KEYS,
                &[
                    ("SOCKUDO_OTEL_ENABLED", "true"),
                    ("SOCKUDO_OTEL_TRACES_ENABLED", "false"),
                    ("SOCKUDO_OTEL_METRICS_ENABLED", "false"),
                    ("SOCKUDO_OTEL_LOGS_ENABLED", "false"),
                    ("SOCKUDO_OTEL_SERVICE_NAME", "realtime-server"),
                    ("SOCKUDO_OTEL_SERVICE_NAMESPACE", "sockudo-cloud"),
                    ("SOCKUDO_OTEL_DEPLOYMENT_ENVIRONMENT", "staging"),
                    (
                        "SOCKUDO_OTEL_RESOURCE_ATTRIBUTES",
                        "region=eu-central-1,instance.type=api",
                    ),
                    ("SOCKUDO_OTEL_ENDPOINT", "http://collector:4317"),
                    ("SOCKUDO_OTEL_EXPORT_TIMEOUT_MS", "11000"),
                    ("SOCKUDO_OTEL_BATCH_SCHEDULED_DELAY_MS", "6000"),
                    ("SOCKUDO_OTEL_BATCH_MAX_QUEUE_SIZE", "4096"),
                    ("SOCKUDO_OTEL_BATCH_MAX_EXPORT_BATCH_SIZE", "1024"),
                    ("SOCKUDO_OTEL_METRIC_EXPORT_INTERVAL_MS", "61000"),
                    ("SOCKUDO_OTEL_PROPAGATION_TRACE_CONTEXT", "false"),
                    ("SOCKUDO_OTEL_PROPAGATION_BAGGAGE", "false"),
                ],
            );
            let mut options = ServerOptions::default();

            options.override_from_env().await.unwrap();

            let config = options.opentelemetry;
            assert!(config.enabled);
            assert!(!config.traces_enabled);
            assert!(!config.metrics_enabled);
            assert!(!config.logs_enabled);
            assert_eq!(config.service_name, "realtime-server");
            assert_eq!(config.service_namespace.as_deref(), Some("sockudo-cloud"));
            assert_eq!(config.deployment_environment.as_deref(), Some("staging"));
            assert_eq!(
                config.resource_attributes.get("region").map(String::as_str),
                Some("eu-central-1")
            );
            assert_eq!(
                config
                    .resource_attributes
                    .get("instance.type")
                    .map(String::as_str),
                Some("api")
            );
            assert_eq!(config.endpoint.as_deref(), Some("http://collector:4317"));
            assert_eq!(config.export_timeout_ms, 11_000);
            assert_eq!(config.batch_scheduled_delay_ms, 6_000);
            assert_eq!(config.batch_max_queue_size, 4_096);
            assert_eq!(config.batch_max_export_batch_size, 1_024);
            assert_eq!(config.metric_export_interval_ms, 61_000);
            assert!(!config.propagation_trace_context);
            assert!(!config.propagation_baggage);
        }
    }
}
