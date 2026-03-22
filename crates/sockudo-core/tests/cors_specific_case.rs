#[cfg(test)]
mod cors_specific_case_tests {
    use sockudo_core::origin_validation::OriginValidator;

    #[test]
    fn test_protocol_agnostic_with_custom_port() {
        // Test protocol-agnostic pattern with custom domain and port
        let allowed_origins = vec!["node1.ghslocal.com:444".to_string()];

        // Should work with HTTPS
        assert!(OriginValidator::validate_origin(
            "https://node1.ghslocal.com:444",
            &allowed_origins
        ));

        // Should work with HTTP
        assert!(OriginValidator::validate_origin(
            "http://node1.ghslocal.com:444",
            &allowed_origins
        ));

        // Wrong port should not work
        assert!(!OriginValidator::validate_origin(
            "https://node1.ghslocal.com:443",
            &allowed_origins
        ));

        // Wrong domain should not work
        assert!(!OriginValidator::validate_origin(
            "https://node2.ghslocal.com:444",
            &allowed_origins
        ));
    }

    #[test]
    fn test_common_cors_patterns() {
        let allowed_origins = vec![
            "localhost:3000".to_string(),
            "api.example.com".to_string(),
            "*.staging.example.com".to_string(),
            "https://secure.example.com".to_string(), // Protocol-specific
        ];

        // Localhost should work with any protocol
        assert!(OriginValidator::validate_origin(
            "http://localhost:3000",
            &allowed_origins
        ));
        assert!(OriginValidator::validate_origin(
            "https://localhost:3000",
            &allowed_origins
        ));

        // API subdomain should work with any protocol
        assert!(OriginValidator::validate_origin(
            "https://api.example.com",
            &allowed_origins
        ));
        assert!(OriginValidator::validate_origin(
            "http://api.example.com",
            &allowed_origins
        ));

        // Wildcard staging should work with any protocol
        assert!(OriginValidator::validate_origin(
            "https://test.staging.example.com",
            &allowed_origins
        ));
        assert!(OriginValidator::validate_origin(
            "http://app.staging.example.com",
            &allowed_origins
        ));

        // Protocol-specific should only work with HTTPS
        assert!(OriginValidator::validate_origin(
            "https://secure.example.com",
            &allowed_origins
        ));
        assert!(!OriginValidator::validate_origin(
            "http://secure.example.com", // Wrong protocol
            &allowed_origins
        ));
    }

    #[test]
    fn test_cors_config_wildcard_origins() {
        // Simulates patterns that would appear in cors.origin config
        let allowed = vec![
            "*.sockudo.io".to_string(),
            "https://exact.example.com".to_string(),
        ];

        // Wildcard subdomain matching
        assert!(OriginValidator::validate_origin(
            "https://beta.sockudo.io",
            &allowed
        ));
        assert!(OriginValidator::validate_origin(
            "https://app.sockudo.io",
            &allowed
        ));
        assert!(OriginValidator::validate_origin(
            "http://staging.sockudo.io",
            &allowed
        ));

        // Exact match
        assert!(OriginValidator::validate_origin(
            "https://exact.example.com",
            &allowed
        ));

        // Non-matching origins rejected
        assert!(!OriginValidator::validate_origin(
            "https://evil.com",
            &allowed
        ));
        assert!(!OriginValidator::validate_origin(
            "https://notsockudo.io",
            &allowed
        ));
    }

    #[test]
    fn test_cors_and_app_origins_behave_identically() {
        // The same origin list should produce the same results
        // whether used in cors.origin or app.allowed_origins
        let origins = vec![
            "*.example.com".to_string(),
            "https://specific.other.com".to_string(),
            "localhost:3000".to_string(),
        ];

        let test_cases = vec![
            ("https://app.example.com", true),
            ("http://sub.example.com", true),
            ("https://specific.other.com", true),
            ("http://specific.other.com", false),
            ("http://localhost:3000", true),
            ("https://localhost:3000", true),
            ("https://evil.com", false),
        ];

        for (origin, expected) in test_cases {
            assert_eq!(
                OriginValidator::validate_origin(origin, &origins),
                expected,
                "Origin '{}' should be {}",
                origin,
                if expected { "allowed" } else { "rejected" }
            );
        }
    }
}
