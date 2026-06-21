import Image from 'next/image';
import Link from 'next/link';
import {
  Activity,
  ArrowRight,
  BellRing,
  Blocks,
  BookOpen,
  Bot,
  CheckCircle2,
  ChevronRight,
  CircleDot,
  CloudCog,
  Code2,
  Coffee,
  Database,
  GitBranch,
  History,
  Layers3,
  LockKeyhole,
  Network,
  RadioTower,
  Route,
  Server,
  ShieldCheck,
  Smartphone,
  Sparkles,
  Terminal,
  Zap,
} from 'lucide-react';
import { TypeUIPanel } from '@/components/typeui-panel';

const paths = [
  {
    title: 'Launch the server',
    description: 'Install Sockudo, choose a runtime, configure apps, and get a local realtime endpoint running.',
    href: '/docs/getting-started/installation',
    icon: Terminal,
    meta: 'Start here',
  },
  {
    title: 'Connect clients',
    description: 'Use JavaScript, Swift, Kotlin, Flutter, and .NET clients with Protocol V1 or Protocol V2.',
    href: '/docs/clients',
    icon: Smartphone,
    meta: 'Realtime SDKs',
  },
  {
    title: 'Operate the cluster',
    description: 'Plan scaling, metrics, webhooks, rate limits, queues, durable history, and recovery paths.',
    href: '/docs/server/scaling',
    icon: CloudCog,
    meta: 'Production ops',
  },
  {
    title: 'Build AI transport',
    description: 'Layer agent streams, rollups, push, annotations, and recovery on the same durable primitives.',
    href: '/docs/server/ai-transport-overview',
    icon: Bot,
    meta: 'AI native',
  },
];

const capabilities = [
  {
    title: 'Pusher-compatible edge',
    description: 'Protocol V1 keeps Channels clients, Laravel Echo, and server SDK expectations intact.',
    icon: Route,
  },
  {
    title: 'Protocol V2 control',
    description: 'Native prefixes, message IDs, serial continuity, filters, deltas, annotations, and rewind.',
    icon: GitBranch,
  },
  {
    title: 'Horizontal fanout',
    description: 'Redis, Redis Cluster, NATS, Kafka, RabbitMQ, Pulsar, Google Pub/Sub, Iggy, or memory.',
    icon: Blocks,
  },
  {
    title: 'Durable recovery',
    description: 'Hot replay buffers, durable history, opaque cursors, presence history, and mutable messages.',
    icon: History,
  },
  {
    title: 'Push and webhooks',
    description: 'Device registration, provider status, scheduled notifications, retries, and delivery signals.',
    icon: BellRing,
  },
  {
    title: 'Operator visibility',
    description: 'Health probes, readiness, Prometheus metrics, webhooks, auth, quotas, and failure surfaces.',
    icon: ShieldCheck,
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
    title: 'Designing a realtime protocol that can evolve',
    href: '/blog/protocol-v2-design',
    meta: 'Architecture',
    description:
      'How Sockudo preserves Pusher compatibility while adding serial continuity, native features, and migration room.',
  },
  {
    title: 'Running realtime infrastructure across nodes',
    href: '/blog/horizontal-realtime-operations',
    meta: 'Operations',
    description:
      'Adapters, duplicate delivery, recovery, observability, and fanout failure modes for production clusters.',
  },
  {
    title: 'Choosing SDK surfaces for product teams',
    href: '/blog/sdk-surface-area',
    meta: 'SDKs',
    description:
      'Where credentials belong, how client and server responsibilities divide, and which SDK surface to reach for.',
  },
];

const eventRows = [
  {
    name: 'Presence sync',
    detail: 'cluster registry',
    value: '4 nodes',
    state: 'healthy',
  },
  {
    name: 'Order fanout',
    detail: 'cross-node publish',
    value: '18.4k/s',
    state: 'fanout',
  },
  {
    name: 'History rewind',
    detail: 'continuity check',
    value: '12 ms',
    state: 'gapless',
  },
  {
    name: 'Push publish',
    detail: 'provider queue',
    value: 'queued',
    state: 'durable',
  },
];

export default function HomePage() {
  return (
    <div className="home-page">
      <section className="home-hero" aria-labelledby="home-hero-title">
        <div className="home-container home-hero-grid">
          <div className="home-hero-copy">
            <div className="home-eyebrow-row">
              <span className="home-badge">
                <Image
                  src="/sockudo-logo/sockudo-icon-color.svg"
                  alt=""
                  width={20}
                  height={20}
                  style={{ width: 20, height: 20 }}
                  aria-hidden="true"
                />
                Sockudo documentation
              </span>
              <Link
                className="home-support-link"
                href="https://buymeacoffee.com/radooku"
                rel="noreferrer"
                target="_blank"
              >
                <Coffee className="size-4" />
                Support the project
              </Link>
            </div>

            <h1 id="home-hero-title">
              Own realtime infrastructure without giving up Pusher compatibility.
            </h1>
            <p className="home-lede">
              Sockudo is a Rust realtime server for teams that want the familiar Pusher protocol,
              Protocol V2 durability, horizontal fanout, recovery, push, SDKs, and AI transport in
              one self-hosted control plane.
            </p>

            <div className="home-actions" aria-label="Primary documentation links">
              <Link className="home-button home-button-primary" href="/docs/getting-started/installation">
                Start building
                <ArrowRight className="size-4" />
              </Link>
              <Link className="home-button home-button-secondary" href="/docs/getting-started/first-connection">
                First connection
                <Zap className="size-4" />
              </Link>
              <Link className="home-button home-button-ghost" href="/docs/reference/protocol">
                Protocol reference
              </Link>
            </div>

          </div>

          <div className="home-control-plane" aria-label="Sockudo realtime control plane preview">
            <div className="control-header">
              <div className="control-brand">
                <span className="control-logo">
                  <RadioTower className="size-5" />
                </span>
                <div>
                  <p>sockudo-control</p>
                  <span>localhost:6001</span>
                </div>
              </div>
              <span className="status-pill">
                <CircleDot className="size-3" />
                Online
              </span>
            </div>

            <div className="control-topology" aria-hidden="true">
              <span className="topology-protocol topology-protocol-v1">Protocol V1</span>
              <span className="topology-protocol topology-protocol-v2">Protocol V2</span>
              <div className="topology-flow">
                <div className="topology-card topology-card-clients">
                  <Smartphone className="size-4" />
                  <span>Clients</span>
                  <small>pusher-js / Echo</small>
                </div>
                <div className="topology-core">
                  <Image
                    src="/sockudo-logo/sockudo-icon-color.svg"
                    alt=""
                    width={44}
                    height={44}
                    style={{ width: 44, height: 44 }}
                  />
                  <strong>Sockudo</strong>
                  <small>Rust + Tokio</small>
                </div>
                <div className="topology-card topology-card-backends">
                  <Server className="size-4" />
                  <span>Backends</span>
                  <small>HTTP API</small>
                </div>
                <div className="topology-card topology-card-adapter">
                  <Database className="size-4" />
                  <span>Adapter</span>
                  <small>Redis / NATS</small>
                </div>
              </div>
            </div>

            <div className="control-grid">
              <div className="control-panel">
                <div className="panel-heading">
                  <Code2 className="size-4" />
                  Quick start
                </div>
                <div className="control-steps">
                  <div className="control-step">
                    <span>01</span>
                    <div>
                      <strong>Run the server</strong>
                      <code>cargo run --release --features full</code>
                    </div>
                  </div>
                  <div className="control-step">
                    <span>02</span>
                    <div>
                      <strong>Connect a client</strong>
                      <code>protocolVersion: 2</code>
                    </div>
                  </div>
                  <div className="control-step">
                    <span>03</span>
                    <div>
                      <strong>Publish safely</strong>
                      <code>idempotency_key: order-created</code>
                    </div>
                  </div>
                </div>
              </div>

              <div className="event-stream">
                <div className="panel-heading">
                  <Activity className="size-4" />
                  Live delivery
                </div>
                {eventRows.map(({ name, detail, value, state }) => (
                  <div className="event-row" key={name}>
                    <span className="event-label">
                      <span>{name}</span>
                      <small>{detail}</small>
                    </span>
                    <strong>{value}</strong>
                    <em>{state}</em>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      </section>

      <section className="home-section home-section-soft" aria-labelledby="home-paths-title">
        <div className="home-container">
          <div className="home-section-heading">
            <span className="home-badge home-badge-muted">
              <BookOpen className="size-4" />
              Documentation map
            </span>
            <h2 id="home-paths-title">Start with the decision you are making.</h2>
            <p>
              The homepage routes operators, backend teams, client developers, and AI product teams
              into the part of the docs that answers their next production question.
            </p>
          </div>

          <div className="home-path-grid">
            {paths.map((path) => (
              <Link className="home-card home-path-card" href={path.href} key={path.title}>
                <span className="home-card-icon">
                  <path.icon className="size-5" />
                </span>
                <span className="home-card-meta">{path.meta}</span>
                <h4>{path.title}</h4>
                <p>{path.description}</p>
                <span className="home-card-action">
                  Open guide
                  <ChevronRight className="size-4" />
                </span>
              </Link>
            ))}
          </div>
        </div>
      </section>

      <section className="home-section" aria-labelledby="home-architecture-title">
        <div className="home-container home-architecture-grid">
          <div className="home-section-heading home-section-heading-sticky">
            <span className="home-badge home-badge-muted">
              <Network className="size-4" />
              Runtime architecture
            </span>
            <h2 id="home-architecture-title">A compatibility layer that grows into a durable platform.</h2>
            <p>
              Sockudo keeps the Protocol V1 contract stable while Protocol V2 unlocks history,
              recovery, annotations, push delivery, and AI stream primitives on the same fanout core.
            </p>
            <Link className="home-inline-link" href="/docs/server/history-recovery">
              Read history and recovery
              <ArrowRight className="size-4" />
            </Link>
          </div>

          <div className="home-architecture-panel">
            <Image
              src="/diagrams/architecture.svg"
              alt="Sockudo architecture diagram showing clients, adapters, queues, cache, app manager, metrics, webhooks, and storage."
              width={760}
              height={500}
              className="architecture-image"
            />
          </div>
        </div>
      </section>

      <section className="home-section home-section-ink" aria-labelledby="home-capabilities-title">
        <div className="home-container">
          <div className="home-section-heading home-section-heading-invert">
            <span className="home-badge home-badge-dark">
              <Layers3 className="size-4" />
              Production surface
            </span>
            <h2 id="home-capabilities-title">Everything operators expect before realtime becomes critical path.</h2>
            <p>
              Compatibility is the on-ramp. The docs also cover the stateful, distributed, and
              observable pieces that make Sockudo viable when missed events are not acceptable.
            </p>
          </div>

          <div className="home-feature-grid">
            {capabilities.map((feature) => (
              <article className="home-card home-feature-card" key={feature.title}>
                <span className="home-card-icon">
                  <feature.icon className="size-5" />
                </span>
                <h4>{feature.title}</h4>
                <p>{feature.description}</p>
              </article>
            ))}
          </div>
        </div>
      </section>

      <section className="home-section home-section-soft" aria-labelledby="home-sdk-title">
        <div className="home-container home-sdk-layout">
          <div className="home-section-heading">
            <span className="home-badge home-badge-muted">
              <Server className="size-4" />
              SDK shelf
            </span>
            <h2 id="home-sdk-title">Client and server SDKs stay close to the protocol.</h2>
            <p>
              Every official SDK path includes installation, configuration, authentication, publish
              flows, history, and production guidance for teams adopting Sockudo incrementally.
            </p>
          </div>

          <div className="home-sdk-grid">
            {sdkCards.map(([name, packageName, href]) => (
              <Link className="home-sdk-card" href={href} key={name}>
                <span>{packageName}</span>
                <h4>{name}</h4>
                <p>
                  Open guide
                  <ArrowRight className="size-4" />
                </p>
              </Link>
            ))}
          </div>
        </div>
      </section>

      <section className="home-section" aria-labelledby="home-notes-title">
        <div className="home-container">
          <div className="home-notes-header">
            <div className="home-section-heading">
              <span className="home-badge home-badge-muted">
                <Sparkles className="size-4" />
                Engineering notes
              </span>
              <h2 id="home-notes-title">Design notes for the realtime edge.</h2>
              <p>
                The blog explains the tradeoffs behind the docs: protocol evolution, multi-node
                operations, SDK boundaries, and the practical shape of ownership.
              </p>
            </div>
            <div className="home-security-panel" aria-label="Production readiness highlights">
              <div>
                <LockKeyhole className="size-5" />
                <span>Auth, origin checks, TLS, quotas</span>
              </div>
              <div>
                <Database className="size-5" />
                <span>Durable history and version stores</span>
              </div>
              <div>
                <CheckCircle2 className="size-5" />
                <span>Metrics, health, readiness, webhooks</span>
              </div>
            </div>
          </div>

          <div className="home-blog-grid">
            {blogPosts.map((post) => (
              <Link className="home-card home-blog-card" href={post.href} key={post.href}>
                <span className="home-card-meta">{post.meta}</span>
                <h4>{post.title}</h4>
                <p>{post.description}</p>
              </Link>
            ))}
          </div>
        </div>
      </section>

      <TypeUIPanel defaultMinimized />
    </div>
  );
}
