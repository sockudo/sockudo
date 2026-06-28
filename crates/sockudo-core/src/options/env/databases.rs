use super::*;

pub(super) fn apply(options: &mut ServerOptions) -> Result<(), Box<dyn std::error::Error>> {
    // --- Database: Redis ---
    if let Ok(host) = std::env::var("DATABASE_REDIS_HOST") {
        options.database.redis.host = host;
    }
    options.database.redis.port =
        parse_env::<u16>("DATABASE_REDIS_PORT", options.database.redis.port);
    if let Ok(username) = std::env::var("DATABASE_REDIS_USERNAME") {
        options.database.redis.username = if username.is_empty() {
            None
        } else {
            Some(username)
        };
    }
    if let Ok(password) = std::env::var("DATABASE_REDIS_PASSWORD") {
        options.database.redis.password = Some(password);
    }
    options.database.redis.db = parse_env::<u32>("DATABASE_REDIS_DB", options.database.redis.db);
    if let Ok(prefix) = std::env::var("DATABASE_REDIS_KEY_PREFIX") {
        options.database.redis.key_prefix = prefix;
    }

    // --- Database: Redis Sentinel ---
    if let Ok(sentinel_password) = std::env::var("DATABASE_REDIS_SENTINEL_PASSWORD") {
        options.database.redis.sentinel_password = if sentinel_password.is_empty() {
            None
        } else {
            Some(sentinel_password)
        };
    }
    if let Ok(sentinel_username) = std::env::var("DATABASE_REDIS_SENTINEL_USERNAME") {
        options.database.redis.sentinel_username = if sentinel_username.is_empty() {
            None
        } else {
            Some(sentinel_username)
        };
    }
    apply_redis_tls_env(
        &mut options.database.redis.sentinel_tls,
        "DATABASE_REDIS_SENTINEL_TLS",
    );
    apply_redis_tls_env(
        &mut options.database.redis.master_tls,
        "DATABASE_REDIS_MASTER_TLS",
    );
    if let Ok(cluster_username) = std::env::var("DATABASE_REDIS_CLUSTER_USERNAME") {
        options.database.redis.cluster.username = if cluster_username.is_empty() {
            None
        } else {
            Some(cluster_username)
        };
    }
    if let Ok(cluster_password) = std::env::var("DATABASE_REDIS_CLUSTER_PASSWORD") {
        options.database.redis.cluster.password = Some(cluster_password);
    }
    options.database.redis.cluster.use_tls = parse_bool_env(
        "DATABASE_REDIS_CLUSTER_USE_TLS",
        options.database.redis.cluster.use_tls,
    );

    // --- Database: MySQL ---
    if let Ok(host) = std::env::var("DATABASE_MYSQL_HOST") {
        options.database.mysql.host = host;
    }
    options.database.mysql.port =
        parse_env::<u16>("DATABASE_MYSQL_PORT", options.database.mysql.port);
    if let Ok(user) = std::env::var("DATABASE_MYSQL_USERNAME") {
        options.database.mysql.username = user;
    }
    if let Ok(pass) = std::env::var("DATABASE_MYSQL_PASSWORD") {
        options.database.mysql.password = pass;
    }
    if let Ok(db) = std::env::var("DATABASE_MYSQL_DATABASE") {
        options.database.mysql.database = db;
    }
    if let Ok(table) = std::env::var("DATABASE_MYSQL_TABLE_NAME") {
        options.database.mysql.table_name = table;
    }
    override_db_pool_settings(&mut options.database.mysql, "DATABASE_MYSQL");

    // --- Database: PostgreSQL ---
    if let Ok(host) = std::env::var("DATABASE_POSTGRES_HOST") {
        options.database.postgres.host = host;
    }
    options.database.postgres.port =
        parse_env::<u16>("DATABASE_POSTGRES_PORT", options.database.postgres.port);
    if let Ok(user) = std::env::var("DATABASE_POSTGRES_USERNAME") {
        options.database.postgres.username = user;
    }
    if let Ok(pass) = std::env::var("DATABASE_POSTGRES_PASSWORD") {
        options.database.postgres.password = pass;
    }
    if let Ok(db) = std::env::var("DATABASE_POSTGRES_DATABASE") {
        options.database.postgres.database = db;
    }
    override_db_pool_settings(&mut options.database.postgres, "DATABASE_POSTGRES");

    // --- Database: DynamoDB ---
    if let Ok(region) = std::env::var("DATABASE_DYNAMODB_REGION") {
        options.database.dynamodb.region = region;
    }
    if let Ok(table) = std::env::var("DATABASE_DYNAMODB_TABLE_NAME") {
        options.database.dynamodb.table_name = table;
    }
    if let Ok(endpoint) = std::env::var("DATABASE_DYNAMODB_ENDPOINT_URL") {
        options.database.dynamodb.endpoint_url = Some(endpoint);
    }
    if let Ok(key_id) = std::env::var("AWS_ACCESS_KEY_ID") {
        options.database.dynamodb.aws_access_key_id = Some(key_id);
    }
    if let Ok(secret) = std::env::var("AWS_SECRET_ACCESS_KEY") {
        options.database.dynamodb.aws_secret_access_key = Some(secret);
    }

    // --- Database: SurrealDB ---
    if let Ok(url) = std::env::var("DATABASE_SURREALDB_URL") {
        options.database.surrealdb.url = url;
    }
    if let Ok(namespace) = std::env::var("DATABASE_SURREALDB_NAMESPACE") {
        options.database.surrealdb.namespace = namespace;
    }
    if let Ok(database) = std::env::var("DATABASE_SURREALDB_DATABASE") {
        options.database.surrealdb.database = database;
    }
    if let Ok(username) = std::env::var("DATABASE_SURREALDB_USERNAME") {
        options.database.surrealdb.username = username;
    }
    if let Ok(password) = std::env::var("DATABASE_SURREALDB_PASSWORD") {
        options.database.surrealdb.password = password;
    }
    if let Ok(table) = std::env::var("DATABASE_SURREALDB_TABLE_NAME") {
        options.database.surrealdb.table_name = table;
    }

    // --- Redis Cluster ---
    let apply_redis_cluster_nodes = |options: &mut ServerOptions, nodes: &str| {
        let node_list: Vec<String> = nodes
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(ToString::to_string)
            .collect();

        options.adapter.cluster.nodes = node_list.clone();
        options.queue.redis_cluster.nodes = node_list.clone();

        let parsed_nodes: Vec<ClusterNode> = node_list
            .iter()
            .filter_map(|seed| ClusterNode::from_seed(seed))
            .collect();
        options.database.redis.cluster.nodes = parsed_nodes.clone();
        options.database.redis.cluster_nodes = parsed_nodes;
    };

    if let Ok(nodes) = std::env::var("REDIS_CLUSTER_NODES") {
        apply_redis_cluster_nodes(options, &nodes);
    }
    if let Ok(nodes) = std::env::var("DATABASE_REDIS_CLUSTER_NODES") {
        apply_redis_cluster_nodes(options, &nodes);
    }
    options.queue.redis_cluster.concurrency = parse_env::<u32>(
        "REDIS_CLUSTER_QUEUE_CONCURRENCY",
        options.queue.redis_cluster.concurrency,
    );
    options.queue.redis_cluster.request_timeout_ms = parse_env::<u64>(
        "REDIS_CLUSTER_QUEUE_REQUEST_TIMEOUT_MS",
        options.queue.redis_cluster.request_timeout_ms,
    );
    if let Ok(prefix) = std::env::var("REDIS_CLUSTER_QUEUE_PREFIX") {
        options.queue.redis_cluster.prefix = Some(prefix);
    }

    Ok(())
}

/// Applies the `<PREFIX>_ENABLED`, `<PREFIX>_ACCEPT_INVALID_CERTS`, `<PREFIX>_CA_PATH`,
/// `<PREFIX>_CLIENT_CERT_PATH`, and `<PREFIX>_CLIENT_KEY_PATH` environment variables
/// onto a [`RedisTlsOptions`] block, leaving unset fields untouched.
fn apply_redis_tls_env(tls: &mut RedisTlsOptions, prefix: &str) {
    tls.enabled = parse_bool_env(&format!("{prefix}_ENABLED"), tls.enabled);
    tls.accept_invalid_certs = parse_bool_env(
        &format!("{prefix}_ACCEPT_INVALID_CERTS"),
        tls.accept_invalid_certs,
    );
    if let Ok(ca_path) = std::env::var(format!("{prefix}_CA_PATH")) {
        tls.ca_path = if ca_path.is_empty() {
            None
        } else {
            Some(ca_path)
        };
    }
    if let Ok(cert_path) = std::env::var(format!("{prefix}_CLIENT_CERT_PATH")) {
        tls.client_cert_path = if cert_path.is_empty() {
            None
        } else {
            Some(cert_path)
        };
    }
    if let Ok(key_path) = std::env::var(format!("{prefix}_CLIENT_KEY_PATH")) {
        tls.client_key_path = if key_path.is_empty() {
            None
        } else {
            Some(key_path)
        };
    }
}
