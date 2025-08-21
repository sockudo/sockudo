#[cfg(test)]
mod origin_validation_integration_tests {
    use sockudo::adapter::handler::origin_validation::OriginValidator;

    #[test]
    fn test_complex_wildcard_patterns() {
        // Test protocol-specific patterns only
        let secure_patterns = vec![
            "https://*.secure.example.com".to_string(),
            "http://localhost:*".to_string(),
        ];

        // Test protocol-specific wildcard
        assert!(OriginValidator::validate_origin(
            "https://app.secure.example.com",
            &secure_patterns
        ));

        assert!(!OriginValidator::validate_origin(
            "http://app.secure.example.com",
            &secure_patterns
        ));

        // Test localhost with any port
        let localhost_patterns = vec!["http://localhost:*".to_string()];

        assert!(OriginValidator::validate_origin(
            "http://localhost:3000",
            &localhost_patterns
        ));

        assert!(OriginValidator::validate_origin(
            "http://localhost:8080",
            &localhost_patterns
        ));
    }

    #[test]
    fn test_edge_cases() {
        let origins = vec!["https://example.com".to_string()];

        // Empty origin should not match
        assert!(!OriginValidator::validate_origin("", &origins));

        // Different protocol should not match
        assert!(!OriginValidator::validate_origin(
            "http://example.com",
            &origins
        ));

        // Different port should not match exact
        assert!(!OriginValidator::validate_origin(
            "https://example.com:8080",
            &origins
        ));

        // Test case insensitive matching (RFC 6454 compliance)
        assert!(OriginValidator::validate_origin(
            "https://EXAMPLE.COM",
            &origins
        ));
    }
}
