#!/usr/bin/env python3
"""End-to-end smoke test for the standalone `sockudo-mcp` binary over stdio.

Requires a running Sockudo with tests/mcp/config.toml and a built binary
(`cargo build -p sockudo-mcp --features cli`). Override paths with
SOCKUDO_MCP_BIN and SOCKUDO_URL / SOCKUDO_METRICS_URL.
"""
import json, subprocess, sys, os
BIN = os.environ.get("SOCKUDO_MCP_BIN", "./target/debug/sockudo-mcp")
URL = os.environ.get("SOCKUDO_URL", "http://127.0.0.1:6011")
METRICS = os.environ.get("SOCKUDO_METRICS_URL", "http://127.0.0.1:9611/metrics")
proc = subprocess.Popen(
    [BIN, "--url", URL, "--app", "smoke-app:smoke-key:smoke-secret",
     "--scopes", "read,write", "--metrics-url", METRICS],
    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=sys.stderr, text=True)
def send(obj):
    proc.stdin.write(json.dumps(obj) + "\n"); proc.stdin.flush()
def recv():
    line = proc.stdout.readline()
    if not line: raise SystemExit("stdio server closed unexpectedly")
    return json.loads(line)
send({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"stdio-smoke","version":"1"}}})
r = recv(); print("initialize        ->", r["result"]["serverInfo"]["name"], r["result"]["protocolVersion"])
send({"jsonrpc":"2.0","method":"notifications/initialized"})
send({"jsonrpc":"2.0","id":2,"method":"tools/list"}); r = recv()
names = {t["name"] for t in r["result"]["tools"]}
print("tools/list        ->", len(names), "tools; admin hidden:", "sockudo_reset_history" not in names)
send({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"sockudo_trigger_event","arguments":{"app_id":"smoke-app","channel":"orders","name":"stdio.event","data":"hello","info":"subscription_count"}}})
r = recv(); print("trigger_event     ->", json.dumps(r["result"]["structuredContent"]), "isError:", r["result"].get("isError"))
assert not r["result"].get("isError"), r
send({"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"sockudo_get_history","arguments":{"app_id":"smoke-app","channel":"orders","limit":3}}})
r = recv(); items = r["result"]["structuredContent"]["items"]; print("get_history       ->", len(items), "items; newest:", items[0]["event_name"])
send({"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"sockudo_server_info","arguments":{}}})
r = recv(); print("server_info       -> transport:", r["result"]["structuredContent"]["mcp_server"]["transport"], "mode:", r["result"]["structuredContent"]["sockudo"]["mode"])
send({"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"sockudo_server_metrics","arguments":{"filter":"sockudo_connected","max_lines":2}}})
r = recv(); print("server_metrics    -> matching:", r["result"]["structuredContent"]["matching_lines"])
send({"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"sockudo_list_channels","arguments":{"app_id":"wrong"}}})
r = recv(); print("unknown app       -> isError:", r["result"].get("isError"), r["result"]["structuredContent"]["error"])
send({"jsonrpc":"2.0","id":8,"method":"resources/read","params":{"uri":"sockudo://server/health"}})
r = recv(); print("resource health   ->", r["result"]["contents"][0]["text"][:40])
proc.stdin.close(); proc.wait(timeout=10); print("stdio exit        ->", proc.returncode); print("STDIO SMOKE OK")
