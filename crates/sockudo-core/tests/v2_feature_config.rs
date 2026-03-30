#[cfg(test)]
mod v2_feature_config_tests {
    use sockudo_core::options::ServerOptions;
    use std::env;

    #[test]
    fn test_v2_feature_config_defaults() {
        let options = ServerOptions::default();

        assert!(options.ephemeral.enabled);
        assert!(options.echo_control.enabled);
        assert!(options.echo_control.default_echo_messages);
        assert!(options.event_name_filtering.enabled);
        assert_eq!(options.event_name_filtering.max_events_per_filter, 50);
        assert_eq!(options.event_name_filtering.max_event_name_length, 200);
    }

    #[tokio::test]
    async fn test_v2_feature_config_env_overrides() {
        unsafe {
            env::set_var("EPHEMERAL_ENABLED", "false");
            env::set_var("ECHO_CONTROL_ENABLED", "false");
            env::set_var("ECHO_CONTROL_DEFAULT_ECHO_MESSAGES", "false");
            env::set_var("EVENT_NAME_FILTERING_ENABLED", "false");
            env::set_var("EVENT_NAME_FILTERING_MAX_EVENTS_PER_FILTER", "10");
            env::set_var("EVENT_NAME_FILTERING_MAX_EVENT_NAME_LENGTH", "64");
        }

        let mut options = ServerOptions::default();
        options.override_from_env().await.unwrap();

        assert!(!options.ephemeral.enabled);
        assert!(!options.echo_control.enabled);
        assert!(!options.echo_control.default_echo_messages);
        assert!(!options.event_name_filtering.enabled);
        assert_eq!(options.event_name_filtering.max_events_per_filter, 10);
        assert_eq!(options.event_name_filtering.max_event_name_length, 64);

        unsafe {
            env::remove_var("EPHEMERAL_ENABLED");
            env::remove_var("ECHO_CONTROL_ENABLED");
            env::remove_var("ECHO_CONTROL_DEFAULT_ECHO_MESSAGES");
            env::remove_var("EVENT_NAME_FILTERING_ENABLED");
            env::remove_var("EVENT_NAME_FILTERING_MAX_EVENTS_PER_FILTER");
            env::remove_var("EVENT_NAME_FILTERING_MAX_EVENT_NAME_LENGTH");
        }
    }
}
