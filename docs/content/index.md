---
title: Sockudo Docs
description: Drop-in Pusher replacement built in Rust with production-ready docs for setup, scaling, security, APIs, and official clients.
navigation: false
seo:
  title: "Sockudo — Production-Ready Realtime Infrastructure"
  description: Drop-in Pusher replacement built in Rust. Own your WebSocket infrastructure with delta compression, tag filtering, multi-region scaling, and official clients.
ogImage:
  component: Sockudo
  props:
    title: "Sockudo — Production-Ready Realtime Infrastructure"
    description: "Drop-in Pusher replacement built in Rust with delta compression, tag filtering, multi-region scaling, and official clients."
---

::u-page-hero
---
orientation: horizontal
---
#title
Production-Ready Realtime Infrastructure

#description
Drop-in Pusher replacement built in Rust. Own your WebSocket infrastructure with enterprise features like delta compression, tag filtering, and multi-region scaling.

#default
  :::HeroCodeShowcase
  :::

#links
  :::ViteButton{to="/getting-started/installation" icon="i-lucide-rocket"}
  Get Started
  :::

  :::u-button
  ---
  color: neutral
  icon: i-simple-icons-github
  size: xl
  to: https://github.com/sockudo/sockudo
  variant: outline
  target: _blank
  ui:
    leadingIcon: scale-80
    trailingIcon: scale-80
  ---
  Star on GitHub
  :::

  :::u-button
  ---
  color: neutral
  icon: i-lucide-book-open
  size: xl
  to: /getting-started/introduction
  variant: ghost
  ui:
    leadingIcon: scale-80
    trailingIcon: scale-80
  ---
  Documentation
  :::
::

::u-page-section
---
align: center
---
#title
Why Sockudo?

#description
Built for teams that need control, performance, and advanced features beyond basic pub/sub.

#default
  :::div{class="asymmetric-grid grid grid-cols-1 lg:grid-cols-3 gap-6 w-full max-w-6xl mx-auto"}
    ::::FeatureCard{title="Blazing Fast" description="Written in Rust with async I/O. Handle millions of concurrent connections with minimal resource usage. Scales horizontally with Redis, Redis Cluster, NATS, Pulsar, RabbitMQ, Google Pub/Sub, or Kafka." icon="i-lucide-zap" gradient="true"}
    ::::

    ::::FeatureCard{title="Drop-in Compatible" description="Keep your existing Pusher integrations. Works seamlessly with pusher-js, Laravel Echo, and all official Pusher SDKs. Migrate without changing client code." icon="i-lucide-repeat"}
    ::::

    ::::FeatureCard{title="Advanced Features" description="Delta compression with conflation keys reduces bandwidth by 80-95%. Server-side tag filtering sends only relevant messages to clients." icon="i-lucide-sparkles"}
    ::::

    ::::FeatureCard{title="Production Ready" description="Built-in rate limiting, origin validation, per-app quotas, health checks, Prometheus metrics, and webhook batching. Deploy with confidence." icon="i-lucide-shield-check" gradient="true"}
    ::::

    ::::FeatureCard{title="Flexible Storage" description="Store app configs in memory, MySQL, PostgreSQL, DynamoDB, ScyllaDB, or Redis. Choose what fits your infrastructure." icon="i-lucide-database" gradient="true"}
    ::::

    ::::FeatureCard{title="Multi-Client SDKs" description="Official JavaScript, Swift, Kotlin, and Flutter clients with filter subscriptions, encrypted channels, and delta reconstruction." icon="i-lucide-code-2"}
    ::::
  :::
::

::u-page-section
---
align: center
---
#title
Core Features

#description
Everything you need to build production-grade realtime applications.

#default
  :::div{class="asymmetric-grid grid grid-cols-1 lg:grid-cols-3 gap-6 w-full max-w-6xl mx-auto"}
    ::::FeatureCard{title="Lightning Fast Websockets" description="Written in Rust with async I/O. Handle millions of concurrent connections with minimal resource usage." icon="i-lucide-layers" gradient="true" compact-demo="true"}
    :::::CodePanel
    ---
    language: Bash
    label: Server startup
    compact: true
    code: |
      $ sockudo --config config.toml
      [INFO] Listening on :6001
      [INFO] Ready for websocket traffic
    ---
    :::::
    ::::

    ::::FeatureCard{title="Drop-in Pusher Replacement" description="Keep your existing Pusher integrations. Works seamlessly with pusher-js, Laravel Echo, and all official SDKs." icon="i-lucide-plug" compact-demo="true"}
    :::::CodePanel
    ---
    language: JavaScript
    label: pusher-js
    compact: true
    code: |
      const pusher = new Pusher("app-key", {
        wsHost: "127.0.0.1",
        wsPort: 6001,
      });
    ---
    :::::
    ::::

    ::::FeatureCard{title="Delta Compression" description="Send only message differences instead of full payloads. Conflation keys group messages by entity for 80-95% bandwidth savings." icon="i-lucide-diff" compact-demo="true"}
    :::::CodePanel
    ---
    language: TypeScript
    label: Delta options
    compact: true
    code: |
      client.subscribe("ticker:btc", {
        delta: {
          enabled: true,
          algorithm: "xdelta3",
        },
      });
    ---
    :::::
    ::::

    ::::FeatureCard{title="Tag Filtering" description="Server-side message filtering with tags. Clients subscribe with filter expressions and receive only matching messages." icon="i-lucide-filter" gradient="true" compact-demo="true"}
    :::::CodePanel
    ---
    language: TypeScript
    label: Filter expressions
    compact: true
    code: |
      client.subscribe("market", {
        filter: Filter.and(
          Filter.eq("event", "trade"),
          Filter.gte("price", "100"),
        ),
      });
    ---
    :::::
    ::::
  :::
::

::u-page-section
---
align: center
---
#title
Get Started in Minutes

#description
Three ways to run Sockudo, from Docker compose to production clusters.

#default
  :::div{class="asymmetric-grid grid grid-cols-1 lg:grid-cols-3 gap-6 w-full max-w-6xl mx-auto"}
    ::::FeatureCard{title="Docker Compose" description="Clone the repo and run `docker compose up`. Perfect for local development and testing. Health checks and metrics included." icon="i-simple-icons-docker" gradient="true"}
    ::::

    ::::FeatureCard{title="Precompiled Binary" description="Install with `cargo binstall sockudo` for instant setup. Choose feature flags for Redis, NATS, SQL backends. No compilation required." icon="i-simple-icons-rust"}
    ::::

    ::::FeatureCard{title="Build from Source" description="Compile with custom features using Cargo. Full control over dependencies. Supports all backends and integrations." icon="i-lucide-code"}
    ::::
  :::
::

::u-page-section
---
align: center
---
#title
Official JavaScript Client

#description
`@sockudo/client` extends Pusher's API with advanced features while maintaining full compatibility.

#default
  :::div{class="asymmetric-grid grid grid-cols-1 lg:grid-cols-3 gap-6 w-full max-w-6xl mx-auto"}
    ::::FeatureCard{title="Filter API" description="Subscribe with server-side filters using a fluent API. Supports equality, pattern matching, and complex combinations." icon="i-lucide-filter" gradient="true"}
    ::::

    ::::FeatureCard{title="Delta Reconstruction" description="Automatic delta decoding with cache management. Track bandwidth savings with stats hooks. Per-subscription configuration." icon="i-lucide-package"}
    ::::

    ::::FeatureCard{title="Framework Integration" description="Works with Laravel Echo, React, Vue, Svelte, and vanilla JavaScript. Custom connector for Echo with full feature support." icon="i-lucide-plug"}
    ::::
  :::
::

::u-page-section
---
align: center
---
#title
Integrations & Ecosystem

#description
Connect Sockudo with your existing tools and frameworks.

#default
  :::div{class="asymmetric-grid grid grid-cols-1 lg:grid-cols-3 gap-6 w-full max-w-6xl mx-auto"}
    ::::FeatureCard{title="Laravel Echo" description="Drop-in replacement for Pusher with Laravel Broadcasting. Use `@sockudo/client` connector for advanced features." icon="i-simple-icons-laravel" gradient="true"}
    ::::

    ::::FeatureCard{title="Pusher-JS" description="Standard pusher-js works out of the box. No code changes needed. Point wsHost to your Sockudo server and you're ready." icon="i-lucide-radio-tower"}
    ::::

    ::::FeatureCard{title="Backend SDKs" description="Use official Pusher server libraries in PHP, Node.js, Python, Ruby, Go, and .NET. Publish events with ease." icon="i-lucide-webhook"}
    ::::

    ::::FeatureCard{title="Horizontal Adapters" description="Scale with Redis, Redis Cluster, NATS, Pulsar, RabbitMQ, Google Pub/Sub, or Kafka adapters with health monitoring." icon="i-lucide-container" gradient="true"}
    ::::

    ::::FeatureCard{title="Database Support" description="Store application configs in MySQL, PostgreSQL, DynamoDB, ScyllaDB, or Redis. In-memory mode for development." icon="i-lucide-database" gradient="true"}
    ::::

    ::::FeatureCard{title="Queue & Webhooks" description="Process webhooks asynchronously with Redis queues or AWS SQS. Automatic batching and retry logic included." icon="i-lucide-cloud"}
    ::::
  :::
::

::u-page-section
---
align: center
---
#title
Ready to Get Started?

#description
Install Sockudo, connect your first client, and start building realtime features in under 5 minutes.

#links
  :::ViteButton{to="/getting-started/installation" icon="i-lucide-rocket"}
  Quick Start
  :::

  :::u-button
  ---
  color: neutral
  size: xl
  to: /getting-started/introduction
  variant: outline
  trailing-icon: i-lucide-book-open
  ---
  Read Introduction
  :::

  :::u-button
  ---
  color: neutral
  icon: i-simple-icons-github
  size: xl
  to: https://github.com/sockudo/sockudo
  variant: ghost
  target: _blank
  ---
  View on GitHub
  :::
::
