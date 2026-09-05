# Manifest inventory

Source: actual Cargo.toml files at the audited commit. The JSON inventory records complete manifests including every direct dependency declaration and feature expansion. Root Cargo workspace has 17 members; HTTP Rust SDK is excluded; sender and fuzz are auxiliary packages.

## sockudo-ai-benches — [benches/ai/Cargo.toml](/home/radud/Desktop/Code/Rust/sockudo/benches/ai/Cargo.toml)

**Features:** None declared.

**Direct dependencies:** `async-trait`, `criterion`, `sockudo-ai-transport`, `sockudo-core`, `sockudo-protocol`, `sonic-rs`, `tokio`.

## bench-sender — [benches/sender/Cargo.toml](/home/radud/Desktop/Code/Rust/sockudo/benches/sender/Cargo.toml)

**Features:** None declared.

**Direct dependencies:** `clap`, `pushers`, `sonic-rs`, `tokio`.

## sockudo-ably-compat — [crates/sockudo-ably-compat/Cargo.toml](/home/radud/Desktop/Code/Rust/sockudo/crates/sockudo-ably-compat/Cargo.toml)

**Features:** `default` → (empty marker); `bench` → (empty marker); `fuzzing` → (empty marker); `local` → `sockudo-adapter/local`; `ai-transport` → `sockudo-core/ai-transport`, `sockudo-protocol/ai-transport`, `sockudo-adapter/ai-transport`; `delta` → `sockudo-adapter/delta`, `dep:sockudo-delta`; `recovery` → `sockudo-adapter/recovery`; `tag-filtering` → `sockudo-adapter/tag-filtering`; `push` → `dep:sockudo-push`

**Direct dependencies:** `sockudo-core`, `sockudo-protocol`, `sockudo-adapter`, `sockudo-delta` (optional), `sockudo-push` (optional), `ahash`, `aes-gcm`, `async-trait`, `bytes`, `chrono`, `crossfire`, `axum`, `base64`, `dashmap`, `http`, `hmac`, `jsonwebtoken`, `sockudo-filter`, `rmp-serde`, `serde`, `serde_json`, `sha2`, `sockudo-ws`, `sonic-rs`, `thiserror`, `tokio`, `tracing`, `urlencoding`, `uuid`.

## sockudo-adapter — [crates/sockudo-adapter/Cargo.toml](/home/radud/Desktop/Code/Rust/sockudo/crates/sockudo-adapter/Cargo.toml)

**Features:** `default` → `local`, `v2`; `local` → (empty marker); `ai-transport` → `sockudo-core/ai-transport`, `sockudo-protocol/ai-transport`, `dep:sockudo-ai-transport`; `v2` → `delta`, `tag-filtering`, `recovery`; `delta` → `dep:sockudo-delta`; `tag-filtering` → `dep:sockudo-filter`; `recovery` → (empty marker); `full` → `redis`, `redis-cluster`, `nats`, `pulsar`, `rabbitmq`, `google-pubsub`, `kafka`, `iggy`, `v2`; `redis` → `sockudo-core/redis`, `dep:redis`; `redis-cluster` → `redis`, `sockudo-core/redis-cluster`; `nats` → `sockudo-core/nats`, `dep:async-nats`; `pulsar` → `sockudo-core/pulsar`, `dep:pulsar`; `rabbitmq` → `sockudo-core/rabbitmq`, `dep:lapin`; `google-pubsub` → `sockudo-core/google-pubsub`, `dep:google-cloud-auth`, `dep:google-cloud-pubsub`; `kafka` → `sockudo-core/kafka`, `dep:rdkafka`; `iggy` → `sockudo-core/iggy`, `dep:iggy`

**Direct dependencies:** `sockudo-core`, `sockudo-protocol`, `sockudo-filter` (optional), `sockudo-delta` (optional), `sockudo-ai-transport` (optional), `sockudo-metrics`, `sockudo-webhook`, `ahash`, `base64`, `async-nats` (optional), `async-trait`, `bytes`, `compact_str`, `crossfire`, `dashmap`, `futures`, `futures-util`, `google-cloud-pubsub` (optional), `google-cloud-auth` (optional), `lapin` (optional), `mockall`, `moka`, `num_cpus`, `parking_lot`, `pulsar` (optional), `rand`, `redis` (optional), `rdkafka` (optional), `iggy` (optional), `regex`, `serde`, `sockudo-ws`, `sonic-rs`, `tokio`, `tokio-util`, `tracing`, `uuid`.

## sockudo-ai-transport — [crates/sockudo-ai-transport/Cargo.toml](/home/radud/Desktop/Code/Rust/sockudo/crates/sockudo-ai-transport/Cargo.toml)

**Features:** None declared.

**Direct dependencies:** `ahash`, `bytes`, `parking_lot`, `sockudo-core`, `sockudo-protocol`, `sonic-rs`.

## sockudo-app — [crates/sockudo-app/Cargo.toml](/home/radud/Desktop/Code/Rust/sockudo/crates/sockudo-app/Cargo.toml)

**Features:** `default` → `local`; `local` → (empty marker); `full` → `mysql`, `postgres`, `dynamodb`, `surrealdb`, `scylladb`; `mysql` → `sockudo-core/mysql`, `dep:sqlx`, `dep:futures-util`, `dep:urlencoding`, `dep:regex`; `postgres` → `sockudo-core/postgres`, `dep:sqlx`, `dep:futures-util`, `dep:urlencoding`, `dep:regex`; `dynamodb` → `sockudo-core/dynamodb`, `dep:aws-config`, `dep:aws-sdk-dynamodb`; `surrealdb` → `sockudo-core/surrealdb`, `dep:surrealdb`, `dep:surrealdb-types`, `dep:regex`; `scylladb` → `sockudo-core/scylladb`, `dep:scylla`, `dep:futures`

**Direct dependencies:** `sockudo-core`, `ahash`, `async-trait`, `aws-config` (optional), `aws-sdk-dynamodb` (optional), `dashmap`, `futures` (optional), `futures-util` (optional), `moka`, `regex` (optional), `scylla` (optional), `serde`, `sonic-rs`, `sqlx` (optional), `surrealdb` (optional), `surrealdb-types` (optional), `tokio`, `tracing`, `urlencoding` (optional).

## sockudo-cache — [crates/sockudo-cache/Cargo.toml](/home/radud/Desktop/Code/Rust/sockudo/crates/sockudo-cache/Cargo.toml)

**Features:** `default` → `local`; `local` → (empty marker); `full` → `redis`, `redis-cluster`; `redis` → `sockudo-core/redis`, `dep:redis`; `redis-cluster` → `redis`, `sockudo-core/redis-cluster`

**Direct dependencies:** `sockudo-core`, `ahash`, `async-trait`, `moka`, `redis` (optional), `tokio`, `tracing`.

## sockudo-core — [crates/sockudo-core/Cargo.toml](/home/radud/Desktop/Code/Rust/sockudo/crates/sockudo-core/Cargo.toml)

**Features:** `default` → `local`; `local` → (empty marker); `ai-transport` → (empty marker); `full` → `redis`, `redis-cluster`, `nats`, `pulsar`, `rabbitmq`, `google-pubsub`, `kafka`, `iggy`, `mysql`, `postgres`, `dynamodb`, `surrealdb`, `scylladb`, `sqs`, `sns`, `lambda`; `redis` → `dep:redis`; `redis-cluster` → `redis`; `nats` → (empty marker); `pulsar` → (empty marker); `rabbitmq` → (empty marker); `google-pubsub` → (empty marker); `kafka` → (empty marker); `iggy` → (empty marker); `mysql` → (empty marker); `postgres` → (empty marker); `dynamodb` → (empty marker); `surrealdb` → (empty marker); `scylladb` → (empty marker); `sqs` → (empty marker); `sns` → (empty marker); `lambda` → (empty marker)

**Direct dependencies:** `sockudo-protocol`, `sockudo-filter`, `ahash`, `async-trait`, `base64`, `bytes`, `chrono`, `crossfire`, `dashmap`, `futures-util`, `hex`, `hmac`, `jsonwebtoken`, `md5`, `memchr`, `parking_lot`, `papaya`, `rand`, `redis` (optional), `regex`, `serde`, `serde_json`, `serde-aux`, `sha2`, `sockudo-ws`, `sonic-rs`, `thiserror`, `toml`, `tokio`, `tokio-util`, `tracing`, `url`, `urlencoding`, `uuid`.

## sockudo-delta — [crates/sockudo-delta/Cargo.toml](/home/radud/Desktop/Code/Rust/sockudo/crates/sockudo-delta/Cargo.toml)

**Features:** `default` → (empty marker); `redis` → `dep:redis`, `sockudo-core/redis`; `nats` → `dep:async-nats`

**Direct dependencies:** `sockudo-core`, `ahash`, `async-trait`, `async-nats` (optional), `base64`, `dashmap`, `fossil-delta`, `oxidelta`, `redis` (optional), `serde`, `sonic-rs`, `tokio`, `tracing`.

## sockudo-filter — [crates/sockudo-filter/Cargo.toml](/home/radud/Desktop/Code/Rust/sockudo/crates/sockudo-filter/Cargo.toml)

**Features:** None declared.

**Direct dependencies:** `ahash`, `dashmap`, `serde`, `serde_json`, `sonic-rs`, `memchr`, `jmespath`, `thiserror`.

## sockudo-metrics — [crates/sockudo-metrics/Cargo.toml](/home/radud/Desktop/Code/Rust/sockudo/crates/sockudo-metrics/Cargo.toml)

**Features:** None declared.

**Direct dependencies:** `sockudo-core`, `async-trait`, `chrono`, `metrics`, `metrics-exporter-prometheus`, `metrics-exporter-tcp`, `metrics-util`, `sonic-rs`, `tokio`, `tracing`.

## sockudo-protocol — [crates/sockudo-protocol/Cargo.toml](/home/radud/Desktop/Code/Rust/sockudo/crates/sockudo-protocol/Cargo.toml)

**Features:** `default` → (empty marker); `ai-transport` → (empty marker)

**Direct dependencies:** `ahash`, `prost`, `rmp-serde`, `serde`, `serde_bytes`, `serde_json`, `sonic-rs`, `uuid`.

## sockudo-push — [crates/sockudo-push/Cargo.toml](/home/radud/Desktop/Code/Rust/sockudo/crates/sockudo-push/Cargo.toml)

**Features:** `default` → `memory`; `memory` → (empty marker); `testing` → (empty marker); `monolith` → (empty marker); `push-fcm` → `dep:jsonwebtoken`, `dep:reqwest`, `jsonwebtoken/aws_lc_rs`; `push-apns` → `dep:reqwest`; `push-webpush` → `dep:reqwest`, `dep:web-push-native`; `push-hms` → `dep:reqwest`; `push-wns` → `dep:reqwest`; `postgres` → `dep:sqlx`; `mysql` → `dep:sqlx`; `dynamodb` → `dep:aws-sdk-dynamodb`; `surrealdb` → `dep:surrealdb`, `dep:surrealdb-types`; `scylladb` → `dep:scylla`; `redis` → (empty marker); `redis-cluster` → `redis`; `nats` → (empty marker); `pulsar` → (empty marker); `rabbitmq` → (empty marker); `google-pubsub` → (empty marker); `kafka` → (empty marker); `iggy` → (empty marker); `sqs` → (empty marker); `sns` → (empty marker)

**Direct dependencies:** `async-trait`, `base64`, `dashmap`, `futures-util`, `hex`, `httpdate`, `jsonwebtoken` (optional), `rand`, `reqwest` (optional), `aws-lc-rs`, `aws-sdk-dynamodb` (optional), `metrics`, `scylla` (optional), `serde`, `sha2`, `sonic-rs`, `sqlx` (optional), `surrealdb` (optional), `surrealdb-types` (optional), `thiserror`, `tokio`, `tracing`, `url`, `web-push-native` (optional), `zeroize`.

## sockudo-queue — [crates/sockudo-queue/Cargo.toml](/home/radud/Desktop/Code/Rust/sockudo/crates/sockudo-queue/Cargo.toml)

**Features:** `default` → `local`; `local` → (empty marker); `full` → `redis`, `redis-cluster`, `nats`, `rabbitmq`, `kafka`, `iggy`, `pulsar`, `google-pubsub`, `sqs`, `sns`; `redis` → `sockudo-core/redis`, `dep:redis`; `redis-cluster` → `redis`, `sockudo-core/redis-cluster`; `nats` → `sockudo-core/nats`, `dep:async-nats`; `rabbitmq` → `sockudo-core/rabbitmq`, `dep:lapin`; `kafka` → `sockudo-core/kafka`, `dep:rdkafka`; `iggy` → `sockudo-core/iggy`, `dep:iggy`; `pulsar` → `sockudo-core/pulsar`, `dep:pulsar`; `google-pubsub` → `sockudo-core/google-pubsub`, `dep:google-cloud-auth`, `dep:google-cloud-pubsub`; `sqs` → `sockudo-core/sqs`, `dep:aws-config`, `dep:aws-sdk-sqs`; `sns` → `sockudo-core/sns`, `dep:aws-config`, `dep:aws-sdk-sns`

**Direct dependencies:** `sockudo-core`, `async-trait`, `aws-config` (optional), `aws-sdk-sns` (optional), `aws-sdk-sqs` (optional), `bytes`, `futures-util`, `ahash`, `async-nats` (optional), `chrono`, `dashmap`, `parking_lot`, `google-cloud-auth` (optional), `google-cloud-pubsub` (optional), `lapin` (optional), `metrics`, `pulsar` (optional), `redis` (optional), `rdkafka` (optional), `iggy` (optional), `serde`, `sonic-rs`, `tokio`, `tokio-util`, `tracing`, `uuid`.

## sockudo-rate-limiter — [crates/sockudo-rate-limiter/Cargo.toml](/home/radud/Desktop/Code/Rust/sockudo/crates/sockudo-rate-limiter/Cargo.toml)

**Features:** `default` → `local`; `local` → (empty marker); `full` → `redis`, `redis-cluster`; `redis` → `sockudo-core/redis`, `dep:redis`, `dep:rand`; `redis-cluster` → `redis`, `sockudo-core/redis-cluster`

**Direct dependencies:** `sockudo-core`, `sockudo-metrics`, `async-trait`, `axum`, `ahash`, `dashmap`, `futures-util`, `http`, `hyper`, `redis` (optional), `rand` (optional), `sonic-rs`, `tokio`, `tower-layer`, `tower-service`, `tracing`.

## sockudo — [crates/sockudo-server/Cargo.toml](/home/radud/Desktop/Code/Rust/sockudo/crates/sockudo-server/Cargo.toml)

**Features:** `default` → `local`, `v2`; `versioned-messages` → (empty marker); `ai-transport` → `sockudo-core/ai-transport`, `sockudo-protocol/ai-transport`, `sockudo-adapter/ai-transport`, `versioned-messages`; `ably-compat` → `ai-transport`, `push`, `dep:sockudo-ably-compat`, `sockudo-ably-compat/ai-transport`, `sockudo-ably-compat/recovery`, `sockudo-ably-compat/delta`, `sockudo-ably-compat/push`; `local` → (empty marker); `v2` → `delta`, `tag-filtering`, `recovery`; `delta` → `sockudo-adapter/delta`, `sockudo-delta`; `tag-filtering` → `sockudo-adapter/tag-filtering`, `sockudo-filter`; `recovery` → `sockudo-adapter/recovery`; `push` → `dep:aes-gcm`, `dep:base64`, `dep:sha2`, `dep:sockudo-push`; `push-fcm` → `push`, `sockudo-push/push-fcm`; `push-apns` → `push`, `sockudo-push/push-apns`, `dep:jsonwebtoken`, `jsonwebtoken/aws_lc_rs`; `push-webpush` → `push`, `sockudo-push/push-webpush`; `push-hms` → `push`, `sockudo-push/push-hms`; `push-wns` → `push`, `sockudo-push/push-wns`; `monolith` → `push`, `sockudo-push/monolith`; `full` → `v2`, `versioned-messages`, `ai-transport`, `ably-compat`, `push`, `push-fcm`, `push-apns`, `push-webpush`, `push-hms`, `push-wns`, `redis`, `redis-cluster`, `nats`, `pulsar`, `rabbitmq`, `google-pubsub`, `kafka`, `iggy`, `mysql`, `postgres`, `dynamodb`, `surrealdb`, `scylladb`, `sqs`, `sns`, `lambda`; `redis` → `sockudo-core/redis`, `sockudo-adapter/redis`, `sockudo-cache/redis`, `sockudo-queue/redis`, `sockudo-rate-limiter/redis`, `sockudo-delta?/redis`, `sockudo-push?/redis`; `redis-cluster` → `redis`, `sockudo-core/redis-cluster`, `sockudo-adapter/redis-cluster`, `sockudo-cache/redis-cluster`, `sockudo-queue/redis-cluster`, `sockudo-rate-limiter/redis-cluster`, `sockudo-push?/redis-cluster`; `nats` → `sockudo-core/nats`, `sockudo-adapter/nats`, `sockudo-delta?/nats`, `sockudo-queue/nats`, `sockudo-push?/nats`; `pulsar` → `sockudo-core/pulsar`, `sockudo-adapter/pulsar`, `sockudo-queue/pulsar`, `sockudo-push?/pulsar`; `rabbitmq` → `sockudo-core/rabbitmq`, `sockudo-adapter/rabbitmq`, `sockudo-queue/rabbitmq`, `sockudo-push?/rabbitmq`; `google-pubsub` → `sockudo-core/google-pubsub`, `sockudo-adapter/google-pubsub`, `sockudo-queue/google-pubsub`, `sockudo-push?/google-pubsub`; `kafka` → `sockudo-core/kafka`, `sockudo-adapter/kafka`, `sockudo-queue/kafka`, `sockudo-push?/kafka`; `iggy` → `sockudo-core/iggy`, `sockudo-adapter/iggy`, `sockudo-queue/iggy`, `sockudo-push?/iggy`; `mysql` → `sockudo-core/mysql`, `sockudo-app/mysql`, `dep:sqlx`, `dep:urlencoding`, `sockudo-push?/mysql`; `postgres` → `sockudo-core/postgres`, `sockudo-app/postgres`, `dep:sqlx`, `dep:urlencoding`, `sockudo-push?/postgres`; `dynamodb` → `sockudo-core/dynamodb`, `sockudo-app/dynamodb`, `dep:aws-config`, `dep:aws-sdk-dynamodb`, `sockudo-push?/dynamodb`; `surrealdb` → `sockudo-core/surrealdb`, `sockudo-app/surrealdb`, `dep:surrealdb`, `dep:surrealdb-types`, `sockudo-push?/surrealdb`; `scylladb` → `sockudo-core/scylladb`, `sockudo-app/scylladb`, `dep:scylla`, `sockudo-push?/scylladb`; `sqs` → `sockudo-core/sqs`, `sockudo-queue/sqs`, `sockudo-push?/sqs`; `sns` → `sockudo-core/sns`, `sockudo-queue/sns`, `sockudo-push?/sns`; `lambda` → `sockudo-core/lambda`, `sockudo-webhook/lambda`

**Direct dependencies:** `sockudo-core`, `sockudo-protocol`, `sockudo-filter` (optional), `sockudo-app`, `sockudo-cache`, `sockudo-queue`, `sockudo-rate-limiter`, `sockudo-metrics`, `sockudo-webhook`, `sockudo-delta` (optional), `sockudo-adapter`, `sockudo-push` (optional), `sockudo-ably-compat` (optional), `aes-gcm` (optional), `ahash`, `async-trait`, `axum`, `axum-server`, `clap`, `cmake`, `crossfire`, `dashmap`, `futures-util`, `http`, `http-body-util`, `hyper`, `hyper-util`, `num_cpus`, `rustls`, `serde`, `serde_json`, `serde_urlencoded`, `sockudo-ws`, `sonic-rs`, `sysinfo`, `thiserror`, `tokio`, `tokio-util`, `tower`, `tower-http`, `tracing`, `tracing-subscriber`, `url`, `urlencoding` (optional), `uuid`, `sqlx` (optional), `scylla` (optional), `surrealdb` (optional), `surrealdb-types` (optional), `aws-config` (optional), `aws-sdk-dynamodb` (optional), `jsonwebtoken` (optional), `base64` (optional), `sha2` (optional).

## sockudo-simulator — [crates/sockudo-simulator/Cargo.toml](/home/radud/Desktop/Code/Rust/sockudo/crates/sockudo-simulator/Cargo.toml)

**Features:** None declared.

**Direct dependencies:** `bytes`, `clap`, `rand`, `serde`, `serde_json`, `sockudo-core`, `sockudo-protocol`, `sockudo-push`, `sonic-rs`, `thiserror`, `tokio`.

## sockudo-webhook — [crates/sockudo-webhook/Cargo.toml](/home/radud/Desktop/Code/Rust/sockudo/crates/sockudo-webhook/Cargo.toml)

**Features:** `default` → `local`; `local` → (empty marker); `full` → `lambda`; `lambda` → `sockudo-core/lambda`, `dep:aws-config`, `dep:aws-sdk-lambda`, `dep:dashmap`

**Direct dependencies:** `sockudo-core`, `ahash`, `async-trait`, `aws-config` (optional), `aws-sdk-lambda` (optional), `chrono`, `dashmap` (optional), `hex`, `hmac`, `regex`, `reqwest`, `serde`, `sha2`, `sonic-rs`, `parking_lot`, `tokio`, `tracing`, `url`, `uuid`.

## sockudo-fuzz — [fuzz/Cargo.toml](/home/radud/Desktop/Code/Rust/sockudo/fuzz/Cargo.toml)

**Features:** None declared.

**Direct dependencies:** `libfuzzer-sys`, `serde_json`, `sonic-rs`, `sockudo-core`, `sockudo-ably-compat`, `sockudo-protocol`, `sockudo-push`.

## sockudo-http — [server-sdks/sockudo-http-rust/Cargo.toml](/home/radud/Desktop/Code/Rust/sockudo/server-sdks/sockudo-http-rust/Cargo.toml)

**Features:** `default` → `rustls-tls`, `encryption`; `native-tls` → `reqwest/native-tls`; `rustls-tls` → `reqwest/rustls-tls`; `encryption` → `crypto_secretbox`

**Direct dependencies:** `reqwest`, `serde`, `sonic-rs`, `tokio`, `hmac`, `sha2`, `base64`, `thiserror`, `url`, `rand`, `md5`, `regex`, `hex`, `subtle`, `zeroize`, `crypto_secretbox` (optional).
