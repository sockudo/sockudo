# sockudo-mcp

Model Context Protocol (MCP) server for [Sockudo](https://github.com/sockudo/sockudo). Built on
the official [`rmcp`](https://crates.io/crates/rmcp) SDK; this crate adds the Sockudo layer:

- a signed client for the Sockudo HTTP API contract over a pluggable transport (in-process router
  inside the `sockudo` binary, or `reqwest` to a remote deployment);
- bearer-token principals with `read` / `write` / `admin` scopes and per-app allow-lists;
- ~47 tools (channels, publish, durable history, versioned messages, annotations, presence history,
  push, signature helpers, health/stats/metrics), `sockudo://` resources, prompts, and argument
  completion.

## Embedded in the server

Build `sockudo` with the `mcp` feature and enable `[mcp]` in `config.toml`; MCP is served on
`/mcp` (or a dedicated port). See `docs/content/docs/server/mcp.mdx`.

## Standalone binary

```bash
cargo build -p sockudo-mcp --features cli --release
# stdio (Claude Desktop / Claude Code)
sockudo-mcp --url https://rt.example.com --app app-1:key:secret --scopes read,write
# Streamable HTTP
sockudo-mcp --transport http --listen 127.0.0.1:6100 \
  --token 'ops/read+write=<32+ char token>' --url https://rt.example.com --app app-1:key:secret
```

Claude Code registration:

```bash
claude mcp add sockudo -- sockudo-mcp --url https://rt.example.com --app app-1:key:secret
```

Logs go to stderr (`RUST_LOG=sockudo_mcp=debug`). Audit lines use the target `sockudo_mcp::audit`.
