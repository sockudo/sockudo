use super::*;

pub(super) fn apply(options: &mut ServerOptions) -> Result<(), Box<dyn std::error::Error>> {
    // --- NATS Adapter ---
    if let Ok(servers) = std::env::var("NATS_SERVERS") {
        options.adapter.nats.servers = servers.split(',').map(|s| s.trim().to_string()).collect();
    }
    if let Ok(user) = std::env::var("NATS_USERNAME") {
        options.adapter.nats.username = Some(user);
    }
    if let Ok(pass) = std::env::var("NATS_PASSWORD") {
        options.adapter.nats.password = Some(pass);
    }
    if let Ok(token) = std::env::var("NATS_TOKEN") {
        options.adapter.nats.token = Some(token);
    }
    if let Ok(prefix) = std::env::var("NATS_PREFIX") {
        options.adapter.nats.prefix = prefix;
    }
    options.adapter.nats.connection_timeout_ms = parse_env::<u64>(
        "NATS_CONNECTION_TIMEOUT_MS",
        options.adapter.nats.connection_timeout_ms,
    );
    options.adapter.nats.request_timeout_ms = parse_env::<u64>(
        "NATS_REQUEST_TIMEOUT_MS",
        options.adapter.nats.request_timeout_ms,
    );
    options.adapter.nats.discovery_max_wait_ms = parse_env::<u64>(
        "NATS_DISCOVERY_MAX_WAIT_MS",
        options.adapter.nats.discovery_max_wait_ms,
    );
    options.adapter.nats.discovery_idle_wait_ms = parse_env::<u64>(
        "NATS_DISCOVERY_IDLE_WAIT_MS",
        options.adapter.nats.discovery_idle_wait_ms,
    );
    if let Some(nodes) = parse_env_optional::<u32>("NATS_NODES_NUMBER") {
        options.adapter.nats.nodes_number = Some(nodes);
    }
    if let Some(v) = parse_env_optional::<usize>("NATS_SUBSCRIPTION_CAPACITY") {
        options.adapter.nats.subscription_capacity = Some(v);
    }
    if let Some(v) = parse_env_optional::<usize>("NATS_CLIENT_CAPACITY") {
        options.adapter.nats.client_capacity = Some(v);
    }
    if let Some(v) = parse_env_optional::<usize>("NATS_MAX_RECONNECTS") {
        options.adapter.nats.max_reconnects = Some(v);
    }
    if let Some(v) = parse_env_optional::<usize>("NATS_PRESENCE_SYNC_CHUNK_SIZE") {
        options.adapter.nats.presence_sync_chunk_size = Some(v);
    }

    // --- Pulsar Adapter ---
    if let Ok(url) = std::env::var("PULSAR_URL") {
        options.adapter.pulsar.url = url;
    }
    if let Ok(prefix) = std::env::var("PULSAR_PREFIX") {
        options.adapter.pulsar.prefix = prefix;
    }
    if let Ok(token) = std::env::var("PULSAR_TOKEN") {
        options.adapter.pulsar.token = Some(token);
    }
    options.adapter.pulsar.request_timeout_ms = parse_env::<u64>(
        "PULSAR_REQUEST_TIMEOUT_MS",
        options.adapter.pulsar.request_timeout_ms,
    );
    if let Some(nodes) = parse_env_optional::<u32>("PULSAR_NODES_NUMBER") {
        options.adapter.pulsar.nodes_number = Some(nodes);
    }

    // --- RabbitMQ Adapter ---
    if let Ok(url) = std::env::var("RABBITMQ_URL") {
        options.adapter.rabbitmq.url = url;
    }
    if let Ok(prefix) = std::env::var("RABBITMQ_PREFIX") {
        options.adapter.rabbitmq.prefix = prefix;
    }
    options.adapter.rabbitmq.connection_timeout_ms = parse_env::<u64>(
        "RABBITMQ_CONNECTION_TIMEOUT_MS",
        options.adapter.rabbitmq.connection_timeout_ms,
    );
    options.adapter.rabbitmq.request_timeout_ms = parse_env::<u64>(
        "RABBITMQ_REQUEST_TIMEOUT_MS",
        options.adapter.rabbitmq.request_timeout_ms,
    );
    if let Some(nodes) = parse_env_optional::<u32>("RABBITMQ_NODES_NUMBER") {
        options.adapter.rabbitmq.nodes_number = Some(nodes);
    }

    // --- Google Pub/Sub Adapter ---
    if let Ok(project_id) = std::env::var("GOOGLE_PUBSUB_PROJECT_ID") {
        options.adapter.google_pubsub.project_id = project_id;
    }
    if let Ok(prefix) = std::env::var("GOOGLE_PUBSUB_PREFIX") {
        options.adapter.google_pubsub.prefix = prefix;
    }
    if let Ok(emulator_host) = std::env::var("PUBSUB_EMULATOR_HOST") {
        options.adapter.google_pubsub.emulator_host = Some(emulator_host);
    }
    options.adapter.google_pubsub.request_timeout_ms = parse_env::<u64>(
        "GOOGLE_PUBSUB_REQUEST_TIMEOUT_MS",
        options.adapter.google_pubsub.request_timeout_ms,
    );
    if let Some(nodes) = parse_env_optional::<u32>("GOOGLE_PUBSUB_NODES_NUMBER") {
        options.adapter.google_pubsub.nodes_number = Some(nodes);
    }

    // --- Kafka Adapter ---
    if let Ok(brokers) = std::env::var("KAFKA_BROKERS") {
        options.adapter.kafka.brokers = brokers.split(',').map(|s| s.trim().to_string()).collect();
    }
    if let Ok(prefix) = std::env::var("KAFKA_PREFIX") {
        options.adapter.kafka.prefix = prefix;
    }
    if let Ok(protocol) = std::env::var("KAFKA_SECURITY_PROTOCOL") {
        options.adapter.kafka.security_protocol = Some(protocol);
    }
    if let Ok(mechanism) = std::env::var("KAFKA_SASL_MECHANISM") {
        options.adapter.kafka.sasl_mechanism = Some(mechanism);
    }
    if let Ok(username) = std::env::var("KAFKA_SASL_USERNAME") {
        options.adapter.kafka.sasl_username = Some(username);
    }
    if let Ok(password) = std::env::var("KAFKA_SASL_PASSWORD") {
        options.adapter.kafka.sasl_password = Some(password);
    }
    options.adapter.kafka.request_timeout_ms = parse_env::<u64>(
        "KAFKA_REQUEST_TIMEOUT_MS",
        options.adapter.kafka.request_timeout_ms,
    );
    if let Some(nodes) = parse_env_optional::<u32>("KAFKA_NODES_NUMBER") {
        options.adapter.kafka.nodes_number = Some(nodes);
    }

    // --- Apache Iggy Adapter / Queue ---
    if let Ok(connection_string) = std::env::var("IGGY_CONNECTION_STRING") {
        options.adapter.iggy.connection_string = connection_string.clone();
        options.queue.iggy.connection_string = connection_string;
    }
    if let Ok(username) = std::env::var("IGGY_USERNAME") {
        let username = (!username.is_empty()).then_some(username);
        options.adapter.iggy.username = username.clone();
        options.queue.iggy.username = username;
    }
    if let Ok(password) = std::env::var("IGGY_PASSWORD") {
        let password = (!password.is_empty()).then_some(password);
        options.adapter.iggy.password = password.clone();
        options.queue.iggy.password = password;
    }
    if let Ok(consumer_name) = std::env::var("IGGY_CONSUMER_NAME") {
        let consumer_name = (!consumer_name.is_empty()).then_some(consumer_name);
        options.adapter.iggy.consumer_name = consumer_name.clone();
        options.queue.iggy.consumer_name = consumer_name;
    } else if let Ok(process_id) = std::env::var("INSTANCE_PROCESS_ID") {
        let process_id = (!process_id.is_empty()).then_some(process_id);
        options.adapter.iggy.consumer_name = process_id.clone();
        options.queue.iggy.consumer_name = process_id;
    }
    if let Ok(stream) = std::env::var("IGGY_STREAM") {
        options.adapter.iggy.stream = stream.clone();
        options.queue.iggy.stream = stream;
    }
    if let Ok(prefix) = std::env::var("IGGY_TOPIC_PREFIX") {
        options.adapter.iggy.topic_prefix = prefix;
    }
    if let Ok(prefix) = std::env::var("IGGY_QUEUE_TOPIC_PREFIX") {
        options.queue.iggy.queue_topic_prefix = prefix;
    }
    if let Ok(prefix) = std::env::var("IGGY_CONSUMER_GROUP_PREFIX") {
        options.queue.iggy.consumer_group_prefix = prefix;
    }
    options.adapter.iggy.request_timeout_ms = parse_env::<u64>(
        "IGGY_REQUEST_TIMEOUT_MS",
        options.adapter.iggy.request_timeout_ms,
    );
    options.queue.iggy.request_timeout_ms = parse_env::<u64>(
        "IGGY_REQUEST_TIMEOUT_MS",
        options.queue.iggy.request_timeout_ms,
    );
    options.adapter.iggy.poll_interval_ms = parse_env::<u64>(
        "IGGY_POLL_INTERVAL_MS",
        options.adapter.iggy.poll_interval_ms,
    );
    options.queue.iggy.poll_interval_ms =
        parse_env::<u64>("IGGY_POLL_INTERVAL_MS", options.queue.iggy.poll_interval_ms);
    options.adapter.iggy.poll_batch_size =
        parse_env::<u32>("IGGY_POLL_BATCH_SIZE", options.adapter.iggy.poll_batch_size);
    options.queue.iggy.poll_batch_size =
        parse_env::<u32>("IGGY_POLL_BATCH_SIZE", options.queue.iggy.poll_batch_size);
    options.adapter.iggy.partitions_count = parse_env::<u32>(
        "ADAPTER_IGGY_PARTITIONS_COUNT",
        parse_env::<u32>(
            "IGGY_PARTITIONS_COUNT",
            options.adapter.iggy.partitions_count,
        ),
    );
    options.queue.iggy.partitions_count = parse_env::<u32>(
        "QUEUE_IGGY_PARTITIONS_COUNT",
        parse_env::<u32>("IGGY_PARTITIONS_COUNT", options.queue.iggy.partitions_count),
    );
    options.adapter.iggy.partition_id = parse_env::<u32>(
        "ADAPTER_IGGY_PARTITION_ID",
        parse_env::<u32>("IGGY_PARTITION_ID", options.adapter.iggy.partition_id),
    );
    options.queue.iggy.partition_id = parse_env::<u32>(
        "QUEUE_IGGY_PARTITION_ID",
        parse_env::<u32>("IGGY_PARTITION_ID", options.queue.iggy.partition_id),
    );
    options.adapter.iggy.auto_create =
        parse_env::<bool>("IGGY_AUTO_CREATE", options.adapter.iggy.auto_create);
    options.queue.iggy.auto_create =
        parse_env::<bool>("IGGY_AUTO_CREATE", options.queue.iggy.auto_create);
    options.adapter.iggy.start_from_latest = parse_env::<bool>(
        "IGGY_START_FROM_LATEST",
        options.adapter.iggy.start_from_latest,
    );
    if let Some(nodes) = parse_env_optional::<u32>("IGGY_NODES_NUMBER") {
        options.adapter.iggy.nodes_number = Some(nodes);
        options.queue.iggy.nodes_number = Some(nodes);
    }

    Ok(())
}
