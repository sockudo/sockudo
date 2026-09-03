# MCP smoke tests

Black-box checks for the Model Context Protocol surface. Unit and integration tests live in
`crates/sockudo-mcp` (`cargo test -p sockudo-mcp --all-features`) and
`crates/sockudo-server/src/mcp` (`cargo test -p sockudo --features mcp mcp::`); these scripts drive
real binaries over the wire.

```bash
make mcp-smoke                      # builds, starts a scratch server on :6011, runs both scripts
# or manually:
cargo build -p sockudo --features mcp && cargo build -p sockudo-mcp --features cli
./target/debug/sockudo --config tests/mcp/config.toml &
python3 tests/mcp/smoke_http.py     # embedded Streamable HTTP endpoint, two tokens (admin + read-only)
python3 tests/mcp/smoke_stdio.py    # standalone sockudo-mcp over stdio against the same server
```

`config.toml` uses memory backends, `smoke-app` credentials, and two MCP tokens (`ops` with every
scope, `viewer` read-only and limited to `smoke-app`). Tokens are test fixtures only.

Interactive exploration: `npx @modelcontextprotocol/inspector` and connect to
`http://127.0.0.1:6011/mcp` with header `Authorization: Bearer ops-token-0123456789abcdef`, or
`claude mcp add --transport http sockudo http://127.0.0.1:6011/mcp --header "Authorization: Bearer ops-token-0123456789abcdef"`.
