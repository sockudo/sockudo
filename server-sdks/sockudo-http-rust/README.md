# Sockudo HTTP Rust Client

A fast, safe, and idiomatic Rust client for interacting with the Sockudo HTTP API, allowing you to publish events, authorize channels, authenticate users, and handle webhooks from your Rust applications.

## Features

- Trigger events on public, private, and presence channels
- Trigger events to specific users (User Authentication)
- Trigger batch events for efficiency
- Support for end-to-end encrypted channels
- Authorize client subscriptions to private, presence, and encrypted channels
- Authenticate users for user-specific Pusher features
- Terminate user connections
- Validate and process incoming Pusher webhooks
- Configurable host, port, scheme (HTTP/HTTPS), and timeout
- Asynchronous API using `async/await`
- Typed responses and errors
- **Fast JSON** with SIMD-accelerated `sonic-rs` library

## Installation

Add the following to your `Cargo.toml`:

```toml
[dependencies]
sockudo-http = { path = "../sockudo/server-sdks/sockudo-http-rust" }
sonic-rs = "0.5"
tokio = { version = "1", features = ["full"] }
```

If your application lives inside this monorepo, use:

```toml
[dependencies]
sockudo-http = { path = "server-sdks/sockudo-http-rust" }
sonic-rs = "0.5"
tokio = { version = "1", features = ["full"] }
```

Then run:

```bash
cargo build
```

## Usage

### 1. Initialization

Configure and create a client:

```rust
use sockudo_http::{Config, Pusher, PusherError};

#[tokio::main]
async fn main() -> Result<(), PusherError> {
    let config = Config::builder()
        .app_id("YOUR_APP_ID")
        .key("YOUR_APP_KEY")
        .secret("YOUR_APP_SECRET")
        .host("127.0.0.1")
        .port(6001)
        .use_tls(false)
        .timeout(std::time::Duration::from_secs(5)) // Optional
        .build()?;

    let pusher = Pusher::new(config)?;

    // Your application logic here...
    Ok(())
}
```

For encrypted channels, add the encryption master key:

```rust
let config = Config::builder()
    .app_id("YOUR_APP_ID")
    .key("YOUR_APP_KEY")
    .secret("YOUR_APP_SECRET")
    .host("127.0.0.1")
    .port(6001)
    .use_tls(false)
    .encryption_master_key_base64("YOUR_BASE64_ENCRYPTION_MASTER_KEY")?
    .build()?;
```

You can also initialize from a URL:

```rust
use pushers::{Pusher, PusherError};

let pusher = Pusher::from_url(
    "http://YOUR_APP_KEY:YOUR_APP_SECRET@127.0.0.1:6001/apps/YOUR_APP_ID",
    None,
)?;
```

### 2. Triggering Events

```rust
use pushers::{Pusher, Channel, PusherError};
use sonic_rs::json;

async fn trigger_event(pusher: &Pusher) -> Result<(), PusherError> {
    let channels = vec![Channel::from_string("my-channel")?];
    let event_name = "new-message";
    let data = json!({ "text": "Hello from Rust!" });

    match pusher.trigger(&channels, event_name, data, None).await {
        Ok(response) => {
            println!("Event triggered! Status: {}", response.status());
        }
        Err(e) => eprintln!("Error triggering event: {:?}", e),
    }
    Ok(())
}
```

**Encrypted channels:**
If `channels` contains a single encrypted channel (e.g., `"private-encrypted-mychannel"`) and you've set the `encryption_master_key` in the `Config`, the library will encrypt `data` automatically.

**Excluding a recipient:**

```rust
use pushers::{Pusher, Channel, PusherError, events::TriggerParams};
use sonic_rs::json;

async fn trigger_event_exclude(pusher: &Pusher) -> Result<(), PusherError> {
    let channels = vec![Channel::from_string("my-channel")?];
    let event_name = "new-message";
    let data = json!({ "text": "Hello from Rust!" });
    
    let params = TriggerParams::builder()
        .socket_id("socket_id_to_exclude")
        .build();

    pusher.trigger(&channels, event_name, data, Some(params)).await?;
    Ok(())
}
```

**Idempotency key:**

Attach an idempotency key so the server deduplicates the event on retries, ensuring at-most-once delivery:

```rust
let params = TriggerParams::builder()
    .idempotency_key("unique-key-for-this-event")
    .build();

pusher.trigger(&channels, event_name, data, Some(params)).await?;
```

### 3. Triggering Batch Events

```rust
use pushers::{Pusher, PusherError, events::BatchEvent};
use sonic_rs::json;

async fn trigger_batch(pusher: &Pusher) -> Result<(), PusherError> {
    let batch = vec![
        BatchEvent::new("event1", "channel-a", json!({ "value": 1 })),
        BatchEvent::new("event2", "channel-b", json!({ "value": 2 })),
    ];

    match pusher.trigger_batch(batch).await {
        Ok(response) => println!("Batch triggered! Status: {}", response.status()),
        Err(e) => eprintln!("Error triggering batch: {:?}", e),
    }
    Ok(())
}
```

### 4. Tag Filtering

Tag filtering allows you to add metadata tags to events, enabling clients to filter which events they receive based on tag values. This can significantly reduce bandwidth usage (60-90%) in high-volume scenarios.

**Triggering a single event with tags:**

```rust
use pushers::{Pusher, Channel, PusherError, events::TriggerParams};
use sonic_rs::json;
use std::collections::HashMap;

async fn trigger_with_tags(pusher: &Pusher) -> Result<(), PusherError> {
    let channels = vec![Channel::from_string("sports-updates")?];
    let event_name = "match-event";
    let data = json!({
        "match_id": "123",
        "team": "Home",
        "player": "John Doe",
        "minute": 45
    });

    // Create tags for filtering
    let mut tags = HashMap::new();
    tags.insert("event_type".to_string(), "goal".to_string());
    tags.insert("priority".to_string(), "high".to_string());

    let params = TriggerParams::builder()
        .tags(tags)
        .build();

    pusher.trigger(&channels, event_name, data, Some(params)).await?;
    Ok(())
}
```

**Triggering batch events with tags:**

```rust
use pushers::{Pusher, PusherError, events::BatchEvent};
use sonic_rs::json;
use std::collections::HashMap;

async fn trigger_batch_with_tags(pusher: &Pusher) -> Result<(), PusherError> {
    let mut goal_tags = HashMap::new();
    goal_tags.insert("event_type".to_string(), "goal".to_string());
    goal_tags.insert("priority".to_string(), "high".to_string());

    let mut shot_tags = HashMap::new();
    shot_tags.insert("event_type".to_string(), "shot".to_string());
    shot_tags.insert("xG".to_string(), "0.85".to_string());

    let batch = vec![
        BatchEvent::new("match-event", "match:123", json!({ "type": "goal", "player": "Smith" }))
            .with_tags(goal_tags),
        BatchEvent::new("match-event", "match:123", json!({ "type": "shot", "player": "Jones" }))
            .with_tags(shot_tags),
    ];

    pusher.trigger_batch(batch).await?;
    Ok(())
}
```

**Note:** Tag filtering must be enabled on the Sockudo server (`TAG_FILTERING_ENABLED=true`) for clients to filter events. Tags are key-value pairs where both keys and values are strings. Clients can subscribe with filter expressions to receive only events matching their criteria.

### 5. Authorizing Channels

Typically done in your HTTP handler when a client attempts to subscribe:

```rust
use pushers::{Pusher, Channel, PusherError};
use sonic_rs::json;

fn authorize_channel(pusher: &Pusher) -> Result<(), PusherError> {
    let socket_id = "123.456";
    let channel_name = "private-mychannel";
    let channel = Channel::from_string(channel_name)?;

    // For presence channels, include user data:
    let presence_data = Some(json!({
        "user_id": "unique_user_id",
        "user_info": { "name": "Alice" }
    }));

    match pusher.authorize_channel(socket_id, &channel, presence_data.as_ref()) {
        Ok(auth_signature) => {
            println!("Auth success: {:?}", auth_signature);
            // Return `auth_signature` as JSON to client
        }
        Err(e) => eprintln!("Auth error: {:?}", e),
    }
    Ok(())
}
```

### 6. Authenticating Users

For server-to-user events:

```rust
use pushers::{Pusher, PusherError};
use sonic_rs::json;

fn authenticate_user(pusher: &Pusher) -> Result<(), PusherError> {
    let socket_id = "789.012";
    let user_data = json!({
        "id": "user-bob",      // required
        "name": "Bob The Builder",
        "email": "bob@example.com"
    });

    match pusher.authenticate_user(socket_id, &user_data) {
        Ok(user_auth) => {
            println!("User auth success: {:?}", user_auth);
            // Return `user_auth` as JSON to client
        }
        Err(e) => eprintln!("User auth error: {:?}", e),
    }
    Ok(())
}
```

### 7. Sending an Event to a User

```rust
use pushers::{Pusher, PusherError};
use sonic_rs::json;

async fn send_to_user(pusher: &Pusher) -> Result<(), PusherError> {
    let user_id = "user-bob";
    let event_name = "personal-notification";
    let data = json!({ "alert": "Your report is ready!" });

    match pusher.send_to_user(user_id, event_name, data).await {
        Ok(response) => println!("Sent to user! Status: {}", response.status()),
        Err(e) => eprintln!("Error sending to user: {:?}", e),
    }
    Ok(())
}
```

### 8. Terminating User Connections

```rust
use pushers::{Pusher, PusherError};

async fn terminate_user(pusher: &Pusher) -> Result<(), PusherError> {
    let user_id = "user-charlie";

    match pusher.terminate_user_connections(user_id).await {
        Ok(response) => println!("Terminate successful! Status: {}", response.status()),
        Err(e) => eprintln!("Error terminating user: {:?}", e),
    }
    Ok(())
}
```

### 9. Handling Webhooks

```rust
use pushers::{Pusher, PusherError, webhook::WebhookEvent};
use std::collections::BTreeMap;

fn handle_webhook(pusher: &Pusher) -> Result<(), PusherError> {
    let mut headers = BTreeMap::new();
    headers.insert("X-Pusher-Key".to_string(), "YOUR_APP_KEY".to_string());
    headers.insert("X-Pusher-Signature".to_string(), "RECEIVED_SIGNATURE".to_string());
    headers.insert("Content-Type".to_string(), "application/json".to_string());

    let body = r#"{
        "time_ms": 1600000000000,
        "events":[{"name":"channel_occupied","channel":"my-channel"}]
    }"#;

    let webhook = pusher.webhook(&headers, body);

    if webhook.is_valid(None) {
        println!("Webhook is valid!");
        match webhook.get_events() {
            Ok(events) => {
                for event in events {
                    match event {
                        WebhookEvent::ChannelOccupied { channel } => {
                            println!("Channel occupied: {}", channel);
                        }
                        WebhookEvent::ChannelVacated { channel } => {
                            println!("Channel vacated: {}", channel);
                        }
                        WebhookEvent::MemberAdded { channel, user_id } => {
                            println!("Member {} added to {}", user_id, channel);
                        }
                        WebhookEvent::MemberRemoved { channel, user_id } => {
                            println!("Member {} removed from {}", user_id, channel);
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => eprintln!("Error getting events: {:?}", e),
        }
    } else {
        eprintln!("Invalid webhook!");
    }
    Ok(())
}
```

### 10. Example: Integration with Axum

```rust
use axum::{
    extract::{Json, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
    Router,
};
use pushers::{Config, Pusher, Channel};
use serde::Deserialize;
use sonic_rs::{json, Value};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone)]
struct AppState {
    pusher: Arc<Pusher>,
}

#[tokio::main]
async fn main() {
    let config = Config::builder()
        .app_id("YOUR_APP_ID")
        .key("YOUR_APP_KEY")
        .secret("YOUR_APP_SECRET")
        .host("127.0.0.1")
        .port(6001)
        .use_tls(false)
        .build()
        .expect("Failed to build config");

    let pusher = Arc::new(Pusher::new(config).expect("Failed to create client"));
    let app_state = AppState { pusher };

    let app = Router::new()
        .route("/pusher/auth", post(pusher_auth_handler))
        .route("/pusher/webhook", post(pusher_webhook_handler))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}

#[derive(Deserialize)]
struct AuthRequest {
    socket_id: String,
    channel_name: String,
    #[serde(alias = "channel_data")]
    presence_data: Option<Value>,
}

async fn pusher_auth_handler(
    State(state): State<AppState>,
    Json(payload): Json<AuthRequest>,
) -> impl IntoResponse {
    let channel = match Channel::from_string(&payload.channel_name) {
        Ok(ch) => ch,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "Invalid channel_name"})),
            ).into_response();
        }
    };

    match state.pusher.authorize_channel(
        &payload.socket_id,
        &channel,
        payload.presence_data.as_ref(),
    ) {
        Ok(auth_response) => (StatusCode::OK, Json(auth_response)).into_response(),
        Err(e) => {
            eprintln!("Auth error: {:?}", e);
            (StatusCode::FORBIDDEN, Json(json!({ "error": "Forbidden" }))).into_response()
        }
    }
}

async fn pusher_webhook_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: String,
) -> impl IntoResponse {
    let mut hdrs_btreemap = BTreeMap::new();
    for (k, v) in headers.iter() {
        if let Ok(s) = v.to_str() {
            hdrs_btreemap.insert(k.as_str().to_string(), s.to_string());
        }
    }

    let webhook = state.pusher.webhook(&hdrs_btreemap, &body);

    if webhook.is_valid(None) {
        (StatusCode::OK, Json(json!({ "status": "ok" }))).into_response()
    } else {
        (StatusCode::UNAUTHORIZED, Json(json!({ "error": "Unauthorized" }))).into_response()
    }
}
```

## Configuration Options

The `Config` struct is used to configure the Pusher client. Create it using `Config::builder()`:

| Method | Description |
|--------|-------------|
| `app_id(id)` | Sets the Pusher app ID (required) |
| `key(key)` | Sets the Pusher app key (required) |
| `secret(secret)` | Sets the Pusher app secret (required) |
| `cluster(name)` | Sets a named cluster (e.g., `"eu"`, `"ap1"`). For self-hosted Sockudo, use `host` and `port` directly instead. |
| `host(host)` | Sets a custom host if not using a standard cluster |
| `use_tls(bool)` | Enable HTTPS (default: `true`) |
| `port(number)` | Custom port |
| `timeout(duration)` | HTTP request timeout |
| `encryption_master_key(key)` | Sets the 32-byte encryption master key from raw bytes |
| `encryption_master_key_base64(key)` | Sets the encryption master key from a base64 encoded string |
| `pool_max_idle_per_host(max)` | Maximum idle connections per host |
| `enable_retry(enable)` | Enable/disable retry logic (default: `true`) |
| `max_retries(max)` | Maximum retry attempts (default: `3`) |

Call `.build()` on the `ConfigBuilder` to get a `Result<Config, PusherError>`.

## Channel History

```rust
use sockudo_http::{HistoryParams, Sockudo};

async fn fetch_history(sockudo: &Sockudo) -> Result<(), sockudo_http::SockudoError> {
    let page = sockudo
        .channel_history_with_name(
            "my-channel",
            Some(&HistoryParams {
                limit: Some(50),
                direction: Some("newest_first".to_string()),
                ..Default::default()
            }),
        )
        .await?;

    if let Some(cursor) = page.next_cursor.clone() {
        let _next_page = sockudo
            .channel_history_with_name(
                "my-channel",
                Some(&HistoryParams {
                    cursor: Some(cursor),
                    ..Default::default()
                }),
            )
            .await?;
    }

    Ok(())
}
```

## Error Handling

All fallible methods return `Result<T, PusherError>`. The `PusherError` enum variants:

| Variant | Description |
|---------|-------------|
| `Request(RequestError)` | HTTP request errors (network issues, non-success status codes) |
| `Webhook(WebhookError)` | Webhook processing errors (signature validation, invalid body) |
| `Config { message }` | Invalid configuration (missing app ID, invalid encryption key) |
| `Validation { message }` | Input validation errors (invalid channel name, event name too long) |
| `Encryption { message }` | Encryption/decryption errors for encrypted channels |
| `Json(sonic_rs::Error)` | JSON serialization/deserialization errors |
| `Http(reqwest::Error)` | Underlying HTTP client errors |

## Contributing

Contributions are welcome! Please open issues for bugs or feature requests, or submit pull requests for improvements.

When contributing code, please ensure:
- Code is formatted with `cargo fmt`
- Clippy lints are addressed (`cargo clippy --all-targets --all-features`)
- New functionality is covered by tests
- Documentation is updated accordingly

## License

This project is licensed under the GNU Affero General Public License v3.0. See the `LICENSE.md` file for details.
