import Link from 'next/link';
import {
  ArrowRight,
  BellRing,
  Blocks,
  Coffee,
  Database,
  GitBranch,
  History,
  Radar,
  Route,
  Server,
  ShieldCheck,
  Smartphone,
  Zap,
} from 'lucide-react';

export default function HomePage() {
  const features = [
    {
      title: 'Pusher-compatible by default',
      description:
        'Run existing Channels clients, Laravel Echo, and backend SDKs against a self-hosted Rust server without changing the public protocol surface.',
      icon: Route,
    },
    {
      title: 'Protocol V2 when you need more',
      description:
        'Enable native Sockudo event prefixes, serial continuity, message IDs, recovery, rewind, deltas, filters, annotations, and durable history.',
      icon: GitBranch,
    },
    {
      title: 'Horizontal fanout',
      description:
        'Scale across nodes with Redis, Redis Cluster, NATS, Kafka, RabbitMQ, Pulsar, Google Pub/Sub, or Apache Iggy adapters.',
      icon: Blocks,
    },
    {
      title: 'Operational controls',
      description:
        'Expose health, readiness, Prometheus metrics, webhooks, rate limits, quotas, origin checks, TLS, and failure visibility for production teams.',
      icon: ShieldCheck,
    },
    {
      title: 'History and recovery',
      description:
        'Use replay buffers for reconnect continuity, optional durable channel history, presence history, and versioned mutable messages.',
      icon: History,
    },
    {
      title: 'Push platform',
      description:
        'Register devices, manage credentials, publish async notifications, schedule delivery, and inspect push status across major providers.',
      icon: BellRing,
    },
  ];

  const sdkCards = [
    ['JavaScript', '@sockudo/client', '/docs/clients/javascript'],
    ['Swift', 'SockudoSwift', '/docs/clients/swift'],
    ['Kotlin', 'io.sockudo:sockudo-kotlin', '/docs/clients/kotlin'],
    ['Flutter', 'sockudo_flutter', '/docs/clients/flutter'],
    ['.NET realtime', 'Sockudo.Client', '/docs/clients/dotnet'],
    ['Node HTTP', 'sockudo', '/docs/server-sdks/node'],
    ['Python HTTP', 'sockudo-http-python', '/docs/server-sdks/python'],
    ['Rust HTTP', 'sockudo-http', '/docs/server-sdks/rust'],
  ];

  const blogPosts = [
    {
      title: 'Designing a Realtime Protocol That Can Evolve',
      href: '/blog/protocol-v2-design',
      meta: 'Architecture',
      description:
        'How Sockudo keeps Pusher compatibility intact while adding serial continuity, native features, and migration room.',
    },
    {
      title: 'Running Realtime Infrastructure Across Nodes',
      href: '/blog/horizontal-realtime-operations',
      meta: 'Operations',
      description:
        'Practical guidance for adapters, duplicate delivery, recovery, observability, and fanout failure modes.',
    },
    {
      title: 'Choosing SDK Surfaces for Product Teams',
      href: '/blog/sdk-surface-area',
      meta: 'SDKs',
      description:
        'A tour of Sockudo realtime and server SDKs, where credentials belong, and how client/server responsibilities divide.',
    },
  ];

  return (
    <div>
      <section className="home-shell hero-grid">
        <div className="hero-copy">
          <div className="hero-eyebrow">
            <div className="hero-kicker">
              <Radar className="size-4" />
              Rust realtime infrastructure for teams that need ownership
            </div>
            <Link
              className="button-support"
              href="https://buymeacoffee.com/radooku"
              rel="noreferrer"
              target="_blank"
            >
              Buy me a coffee <Coffee className="size-4" />
            </Link>
          </div>
          <h1 className="hero-title">
            Pusher-compatible realtime, <span className="accent">without the black box.</span>
          </h1>
          <p className="hero-description">
            Sockudo is a self-hosted realtime server with a Pusher-compatible
            Protocol V1, a Sockudo-native Protocol V2, horizontal fanout,
            durable history, recovery, filtering, push notifications, and SDKs
            for every production surface in this repository.
          </p>
          <div className="hero-actions">
            <Link className="button-primary" href="/docs/getting-started/installation">
              Start building <ArrowRight className="size-4" />
            </Link>
            <Link className="button-secondary" href="/docs/clients">
              Client SDKs <Smartphone className="size-4" />
            </Link>
            <Link className="button-secondary" href="/docs/server-sdks">
              Server SDKs <Server className="size-4" />
            </Link>
          </div>
          <div className="metric-row">
            <div className="metric">
              <strong>2 protocols</strong>
              <span>V1 compatibility and V2 native capabilities.</span>
            </div>
            <div className="metric">
              <strong>14 SDKs</strong>
              <span>Realtime clients plus HTTP server libraries.</span>
            </div>
            <div className="metric">
              <strong>9 fanout paths</strong>
              <span>Memory, Redis, NATS, Kafka, Iggy, and more.</span>
            </div>
          </div>
        </div>

        <div className="terminal-card" aria-label="Sockudo quick start code">
          <div className="terminal-header">
            <div className="terminal-dots">
              <span />
              <span />
              <span />
            </div>
            <span className="terminal-label">localhost:6001</span>
          </div>
          <div className="terminal-body">
            <div className="terminal-pane">
              <pre><code>{`# run the server
cargo run --release --features full

# or bring up local infrastructure
docker compose up sockudo redis`}</code></pre>
            </div>
            <div className="terminal-pane">
              <pre><code>{`import Sockudo from "@sockudo/client";

const client = new Sockudo("app-key", {
  wsHost: "127.0.0.1",
  wsPort: 6001,
  forceTLS: false,
  protocolVersion: 2,
  connectionRecovery: true,
});

client.subscribe("orders").bind("created", console.log);`}</code></pre>
            </div>
          </div>
        </div>
      </section>

      <section className="home-shell home-section">
        <div className="section-heading">
          <h2>Documentation designed around production decisions.</h2>
          <p>
            The new docs are organized by the way teams adopt Sockudo: get a
            local server running, choose a protocol, wire clients, publish from
            backends, then harden the cluster.
          </p>
        </div>
        <div className="feature-grid">
          {features.map((feature) => (
            <article className="feature-tile" key={feature.title}>
              <span className="feature-icon">
                <feature.icon className="size-5" />
              </span>
              <h3>{feature.title}</h3>
              <p>{feature.description}</p>
            </article>
          ))}
        </div>
      </section>

      <section className="home-shell home-section">
        <div className="section-heading">
          <h2>SDK examples are first-class, not footnotes.</h2>
          <p>
            Every official realtime client and server HTTP SDK has installation,
            configuration, auth, publish, history, and production guidance with
            syntax-highlighted examples.
          </p>
        </div>
        <div className="sdk-grid">
          {sdkCards.map(([name, packageName, href]) => (
            <Link className="sdk-tile" href={href} key={name}>
              <span>{packageName}</span>
              <h3>{name}</h3>
              <p>Open the guide <ArrowRight className="inline size-4" /></p>
            </Link>
          ))}
        </div>
      </section>

      <section className="home-shell home-section">
        <div className="section-heading">
          <h2>Engineering notes from the realtime edge.</h2>
          <p>
            The blog section documents the design choices behind Sockudo so
            operators can understand the tradeoffs, not just copy commands.
          </p>
        </div>
        <div className="blog-grid">
          {blogPosts.map((post) => (
            <Link className="blog-tile" href={post.href} key={post.href}>
              <div className="blog-meta">{post.meta}</div>
              <h3>{post.title}</h3>
              <p>{post.description}</p>
            </Link>
          ))}
        </div>
      </section>
    </div>
  );
}
