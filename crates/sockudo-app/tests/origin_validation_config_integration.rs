#[cfg(test)]
mod origin_validation_config_integration_tests {
    use sockudo_app::MemoryAppManager;
    use sockudo_core::app::AppManager;
    use sockudo_core::app::{
        App, AppChannelsPolicy, AppFeaturesPolicy, AppLimitsPolicy, AppPolicy,
    };
    use sockudo_core::origin_validation::OriginValidator;

    fn test_app(id: &str, key: &str, secret: &str, allowed_origins: Option<Vec<String>>) -> App {
        App::from_policy(
            id.to_string(),
            key.to_string(),
            secret.to_string(),
            true,
            AppPolicy {
                limits: AppLimitsPolicy {
                    max_connections: 1000,
                    max_backend_events_per_second: Some(1000),
                    max_client_events_per_second: 100,
                    max_read_requests_per_second: Some(1000),
                    max_presence_members_per_channel: Some(100),
                    max_presence_member_size_in_kb: Some(2),
                    max_channel_name_length: Some(200),
                    max_event_channels_at_once: Some(10),
                    max_event_name_length: Some(200),
                    max_event_payload_in_kb: Some(100),
                    max_event_batch_size: Some(10),
                },
                features: AppFeaturesPolicy {
                    enable_client_messages: true,
                    enable_user_authentication: Some(false),
                    enable_watchlist_events: Some(false),
                },
                channels: AppChannelsPolicy {
                    allowed_origins,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
    }

    #[tokio::test]
    async fn test_memory_app_manager_with_allowed_origins() {
        // Create test apps with different origin configurations
        let apps = vec![
            // App with no origin restrictions
            test_app("app-no-restrictions", "key1", "secret1", None),
            // App with specific allowed origins
            test_app(
                "app-with-origins",
                "key2",
                "secret2",
                Some(vec![
                    "https://app.example.com".to_string(),
                    "*.staging.example.com".to_string(),
                    "http://localhost:3000".to_string(),
                ]),
            ),
            // App with wildcard allowing all origins
            test_app(
                "app-allow-all",
                "key3",
                "secret3",
                Some(vec!["*".to_string()]),
            ),
        ];

        // Create memory app manager
        let app_manager = MemoryAppManager::new();

        // Add apps to the manager
        for app in apps {
            app_manager.create_app(app).await.unwrap();
        }

        // Test app with no restrictions
        let app1 = app_manager
            .find_by_id("app-no-restrictions")
            .await
            .unwrap()
            .unwrap();
        assert!(app1.allowed_origins_ref().is_none());
        // Validator should allow any origin when no restrictions are configured
        assert!(OriginValidator::validate_origin(
            "https://any-site.com",
            app1.allowed_origins_ref().unwrap_or_default()
        ));

        // Test app with specific allowed origins
        let app2 = app_manager
            .find_by_id("app-with-origins")
            .await
            .unwrap()
            .unwrap();
        assert!(app2.allowed_origins_ref().is_some());
        let allowed_origins = app2.allowed_origins_ref().unwrap();

        // Should allow configured origins
        assert!(OriginValidator::validate_origin(
            "https://app.example.com",
            allowed_origins
        ));
        assert!(OriginValidator::validate_origin(
            "https://test.staging.example.com",
            allowed_origins
        ));
        assert!(OriginValidator::validate_origin(
            "http://localhost:3000",
            allowed_origins
        ));

        // Should reject non-allowed origins
        assert!(!OriginValidator::validate_origin(
            "https://evil.com",
            allowed_origins
        ));
        assert!(!OriginValidator::validate_origin(
            "http://app.example.com",
            allowed_origins
        )); // Different protocol

        // Test app with wildcard allow all
        let app3 = app_manager
            .find_by_id("app-allow-all")
            .await
            .unwrap()
            .unwrap();
        assert!(app3.allowed_origins_ref().is_some());
        let allowed_origins = app3.allowed_origins_ref().unwrap();

        // Should allow any origin due to wildcard
        assert!(OriginValidator::validate_origin(
            "https://any-site.com",
            allowed_origins
        ));
        assert!(OriginValidator::validate_origin(
            "http://malicious.com",
            allowed_origins
        ));
    }

    #[test]
    fn test_environment_variable_parsing() {
        // Test the environment variable parsing logic from options.rs

        // Test empty string
        let empty_str = "";
        let empty_result = if empty_str.is_empty() {
            None
        } else {
            Some(
                empty_str
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect::<Vec<String>>(),
            )
        };
        assert_eq!(empty_result, None);

        // Test single origin
        let single_result = "https://app.example.com"
            .split(',')
            .map(|s| s.trim().to_string())
            .collect::<Vec<String>>();
        assert_eq!(single_result, vec!["https://app.example.com"]);

        // Test multiple origins
        let multiple_result =
            "https://app.example.com,*.staging.example.com, http://localhost:3000"
                .split(',')
                .map(|s| s.trim().to_string())
                .collect::<Vec<String>>();
        assert_eq!(
            multiple_result,
            vec![
                "https://app.example.com",
                "*.staging.example.com",
                "http://localhost:3000"
            ]
        );
    }
}
