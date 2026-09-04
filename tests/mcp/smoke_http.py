#!/usr/bin/env python3
"""End-to-end smoke test for the embedded Sockudo MCP server (Streamable HTTP).

Start a server with tests/mcp/config.toml (or `make mcp-smoke`), then run this
script. It exercises authentication, scope filtering, tool calls across the
publish/history/operations surfaces, resources, prompts, completion, and
session teardown. Exit code 0 means every assertion held.
"""
import json, sys, urllib.request, urllib.error

import os
BASE = os.environ.get("SOCKUDO_MCP_URL", "http://127.0.0.1:6011/mcp")

def post(token, body, session=None, expect_json=True):
    data = json.dumps(body).encode()
    req = urllib.request.Request(BASE, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json, text/event-stream")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    if session:
        req.add_header("Mcp-Session-Id", session)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            status = resp.status
            headers = dict(resp.headers)
            text = resp.read().decode()
    except urllib.error.HTTPError as e:
        status = e.code; headers = dict(e.headers); text = e.read().decode()
    payload = None
    for line in text.splitlines():
        if line.startswith("data:") and line[5:].strip():
            payload = json.loads(line[5:].strip()); break
    if payload is None and text.strip().startswith("{"):
        payload = json.loads(text)
    return status, headers, payload, text

def rpc(i, method, params=None):
    b = {"jsonrpc": "2.0", "id": i, "method": method}
    if params is not None: b["params"] = params
    return b

def init(token):
    s, h, p, t = post(token, rpc(1, "initialize", {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "smoke", "version": "1"}}))
    assert s == 200, (s, t)
    sid = h.get("mcp-session-id") or h.get("Mcp-Session-Id")
    assert sid, h
    post(token, {"jsonrpc": "2.0", "method": "notifications/initialized"}, sid)
    return sid, p["result"]

def call(token, sid, i, name, args):
    s, _, p, t = post(token, rpc(i, "tools/call", {"name": name, "arguments": args}), sid)
    assert s == 200, (s, t)
    return p

OPS = "ops-token-0123456789abcdef"; VIEW = "viewer-token-0123456789abcdef"

# 1. unauthorized
s, _, _, t = post(None, rpc(0, "ping"))
print("no token          ->", s); assert s == 401, t
s, _, _, t = post("bogus-token-0123456789", rpc(0, "ping"))
print("bad token         ->", s); assert s == 401, t

# 2. ops session
sid, info = init(OPS)
print("initialize        -> protocol", info["protocolVersion"], "server", info["serverInfo"]["name"], info["serverInfo"]["version"])
s, _, p, _ = post(OPS, rpc(2, "tools/list"), sid)
tools = p["result"]["tools"]; names = {t["name"] for t in tools}
print("tools/list (ops)  ->", len(tools), "tools")
assert "sockudo_reset_history" in names and "sockudo_trigger_event" in names

r = call(OPS, sid, 3, "sockudo_list_apps", {})["result"]
print("list_apps         ->", r["structuredContent"]["count"], "app(s):", [a["id"] for a in r["structuredContent"]["apps"]])
assert "smoke-secret" not in json.dumps(r)

r = call(OPS, sid, 4, "sockudo_trigger_event", {"app_id": "smoke-app", "channel": "orders", "name": "order.created", "data": {"id": "ord_1"}, "info": "subscription_count", "idempotency_key": "smoke-1"})["result"]
print("trigger_event     ->", json.dumps(r["structuredContent"]), "isError:", r.get("isError"))
assert not r.get("isError")
r2 = call(OPS, sid, 5, "sockudo_trigger_event", {"app_id": "smoke-app", "channel": "orders", "name": "order.created", "data": {"id": "ord_1"}, "info": "subscription_count", "idempotency_key": "smoke-1"})["result"]
print("trigger_event dup ->", json.dumps(r2["structuredContent"]), "(idempotent replay)")

r = call(OPS, sid, 6, "sockudo_get_history", {"app_id": "smoke-app", "channel": "orders", "limit": 5})["result"]
sc = r["structuredContent"]
print("get_history       -> items:", len(sc["items"]), "stream:", sc["stream_state"]["durable_state"], "first event:", sc["items"][0]["event_name"] if sc["items"] else None)
assert not r.get("isError") and len(sc["items"]) >= 1

r = call(OPS, sid, 7, "sockudo_list_channels", {"app_id": "smoke-app", "info": "subscription_count"})["result"]
print("list_channels     ->", json.dumps(r["structuredContent"]))

r = call(OPS, sid, 8, "sockudo_server_health", {"app_id": "smoke-app"})["result"]
print("server_health     ->", r["content"][0]["text"][:80], "isError:", r.get("isError"))
r = call(OPS, sid, 9, "sockudo_server_stats", {})["result"]
print("server_stats      -> totals:", json.dumps(r["structuredContent"]["totals"]))
r = call(OPS, sid, 10, "sockudo_server_metrics", {"filter": "mcp_tool_calls_total", "max_lines": 5})["result"]
print("server_metrics    -> matching lines:", r["structuredContent"]["matching_lines"])
print("                    ", r["structuredContent"]["metrics"].splitlines()[-1][:110] if r["structuredContent"]["metrics"] else "(none yet)")

r = call(OPS, sid, 11, "sockudo_sign_channel_auth", {"app_id": "smoke-app", "socket_id": "1234.5678", "channel": "presence-room", "channel_data": {"user_id": "u1"}})["result"]
print("sign_channel_auth ->", r["structuredContent"]["auth"][:24] + "...")

r = call(OPS, sid, 12, "sockudo_get_channel", {"app_id": "smoke-app", "channel": "orders", "info": "user_count"})["result"]
print("get_channel(bad)  -> isError:", r.get("isError"), "message:", r["structuredContent"]["message"])
assert r.get("isError")

r = call(OPS, sid, 13, "sockudo_reset_history", {"app_id": "smoke-app", "channel": "orders", "reason": "smoke"})
print("reset no confirm  -> rpc error", r["error"]["code"], r["error"]["message"])
assert r["error"]["code"] == -32602

s, _, p, _ = post(OPS, rpc(14, "resources/read", {"uri": "sockudo://apps/smoke-app/channels/orders/history"}), sid)
print("resource history  ->", p["result"]["contents"][0]["mimeType"], len(json.loads(p["result"]["contents"][0]["text"])["items"]), "items")
s, _, p, _ = post(OPS, rpc(15, "prompts/get", {"name": "sockudo_debug_channel", "arguments": {"app_id": "smoke-app", "channel": "orders"}}), sid)
print("prompts/get       ->", p["result"]["messages"][0]["content"]["text"][:60] + "...")
s, _, p, _ = post(OPS, rpc(16, "completion/complete", {"ref": {"type": "ref/prompt", "name": "sockudo_debug_channel"}, "argument": {"name": "channel", "value": "ord"}, "context": {"arguments": {"app_id": "smoke-app"}}}), sid)
print("completion        ->", p["result"]["completion"]["values"])

# 3. viewer session: read-only
vsid, _ = init(VIEW)
s, _, p, _ = post(VIEW, rpc(2, "tools/list"), vsid)
vnames = {t["name"] for t in p["result"]["tools"]}
print("tools/list (view) ->", len(vnames), "tools; write tools hidden:", "sockudo_trigger_event" not in vnames)
r = call(VIEW, vsid, 3, "sockudo_trigger_event", {"app_id": "smoke-app", "channel": "orders", "name": "x"})
print("viewer publish    -> rpc error", r["error"]["code"], r["error"]["message"])
assert r["error"]["code"] == -32003
r = call(VIEW, vsid, 4, "sockudo_list_channels", {"app_id": "other-app"})
print("viewer other app  -> rpc error", r["error"]["code"], r["error"]["message"])
assert r["error"]["code"] == -32003

# 4. session teardown
req = urllib.request.Request(BASE, method="DELETE"); req.add_header("Authorization", f"Bearer {OPS}"); req.add_header("Mcp-Session-Id", sid)
with urllib.request.urlopen(req, timeout=5) as resp: print("DELETE session    ->", resp.status)
print("SMOKE OK")
