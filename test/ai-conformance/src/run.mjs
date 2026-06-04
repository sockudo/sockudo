import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { AitProtocolClient, aiExtras, normalizeTranscript } from "./protocol-client.mjs";

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const offline = process.env.AIT_CONFORMANCE_OFFLINE === "1";

const scenarios = [
  normalTurn,
  cancelTurn,
  abortPartial,
  errorTurn,
  suspendedContinuation,
  regenerate,
  edit,
  concurrentTurns,
  lateJoinHistory,
  recoverySmoke,
];

if (offline) {
  await validateGoldenFixtures();
  console.log("AI conformance offline fixture validation passed");
} else {
  const client = new AitProtocolClient();
  for (const scenario of scenarios) {
    await runScenario(client, scenario);
  }
  console.log(`AI conformance passed ${scenarios.length} raw-wire scenarios`);
}

async function runScenario(client, scenario) {
  const result = await scenario(client);
  const actual = normalizeTranscript(result.transcript);
  const golden = await readGolden(`${scenario.name}.json`);
  assert.deepEqual(actual, golden, `${scenario.name} transcript mismatch`);
  console.log(`ok ${scenario.name}`);
}

async function normalTurn(client) {
  const channel = uniqueChannel("private-ai-normal");
  const session = await client.connect();
  try {
    session.subscribe(channel);
    await session.waitForEvent((frame) => frame.event === "sockudo:subscription_succeeded", "subscribe");
    await client.publish({
      name: "ai-turn-start",
      channel,
      data: { ok: true },
      extras: aiExtras({ "turn-id": "turn-normal", "invocation-id": "invoke-normal" }),
    });
    const create = await client.publish({
      name: "ai-output",
      channel,
      data: "",
      messageId: "msg-normal",
      extras: aiExtras({ "turn-id": "turn-normal", "status": "streaming", stream: "true", role: "assistant" }),
    });
    const messageSerial = create.channels[channel].message_serial;
    await client.append({
      channel,
      messageSerial,
      data: "hello",
      opId: "op-normal-1",
      extras: aiExtras({ status: "streaming" }),
    });
    await client.append({
      channel,
      messageSerial,
      data: " world",
      opId: "op-normal-2",
      extras: aiExtras({ status: "complete" }),
    });
    await client.publish({
      name: "ai-turn-end",
      channel,
      data: { ok: true },
      extras: aiExtras({ "turn-id": "turn-normal", "turn-reason": "complete" }),
    });
    await session.waitForEvent((frame) => frame.event === "ai-turn-end", "turn end");
    return { transcript: session.transcript };
  } finally {
    session.close();
  }
}

async function cancelTurn(client) {
  return lifecycleOnly(client, "private-ai-cancel", [
    ["ai-turn-start", { "turn-id": "turn-cancel", "invocation-id": "invoke-cancel" }],
    ["ai-cancel", { "turn-id": "turn-cancel", "input-client-id": "client-1" }],
    ["ai-turn-end", { "turn-id": "turn-cancel", "turn-reason": "cancelled" }],
  ]);
}

async function abortPartial(client) {
  return mutableSequence(client, "private-ai-abort", "turn-abort", [
    ["append", "partial", "streaming"],
    ["update", "partial", "cancelled"],
  ]);
}

async function errorTurn(client) {
  return lifecycleOnly(client, "private-ai-error", [
    ["ai-turn-start", { "turn-id": "turn-error", "invocation-id": "invoke-error" }],
    ["ai-turn-end", { "turn-id": "turn-error", "turn-reason": "error", "error-code": "tool-timeout", "error-message": "timeout" }],
  ]);
}

async function suspendedContinuation(client) {
  return lifecycleOnly(client, "private-ai-suspended", [
    ["ai-turn-start", { "turn-id": "turn-suspend", "invocation-id": "invoke-1" }],
    ["ai-turn-end", { "turn-id": "turn-suspend", "turn-reason": "suspended" }],
    ["ai-turn-start", { "turn-id": "turn-suspend", "turn-continue": "true", "invocation-id": "invoke-2" }],
  ]);
}

async function regenerate(client) {
  return mutableSequence(client, "private-ai-regenerate", "turn-regenerate", [
    ["append", "new answer", "complete", { "msg-regenerate": "true", "fork-of": "old-message" }],
  ]);
}

async function edit(client) {
  return lifecycleOnly(client, "private-ai-edit", [
    ["ai-input", { "turn-id": "turn-edit", parent: "previous-input", role: "user" }],
    ["ai-output", { "turn-id": "turn-edit", parent: "previous-input", role: "assistant", discrete: "true" }],
  ]);
}

async function concurrentTurns(client) {
  return lifecycleOnly(client, "private-ai-concurrent", [
    ["ai-turn-start", { "turn-id": "turn-a", "invocation-id": "invoke-a" }],
    ["ai-turn-start", { "turn-id": "turn-b", "invocation-id": "invoke-b" }],
    ["ai-turn-end", { "turn-id": "turn-b", "turn-reason": "complete" }],
    ["ai-turn-end", { "turn-id": "turn-a", "turn-reason": "complete" }],
  ]);
}

async function lateJoinHistory(client) {
  const channel = uniqueChannel("private-ai-late");
  const create = await client.publish({
    name: "ai-output",
    channel,
    data: "",
    messageId: "msg-late",
    extras: aiExtras({ status: "streaming", role: "assistant" }),
  });
  await client.append({
    channel,
    messageSerial: create.channels[channel].message_serial,
    data: "late-state",
    opId: "op-late-1",
    extras: aiExtras({ status: "complete" }),
  });
  const latest = await client.getMessage({ channel, messageSerial: create.channels[channel].message_serial });
  assert.equal(latest.item.data, "late-state");
  return { transcript: [{ event: "history:get_latest", channel, data: latest.item }] };
}

async function recoverySmoke(client) {
  const channel = uniqueChannel("private-ai-recovery");
  const session = await client.connect();
  try {
    session.subscribe(channel);
    await session.waitForEvent((frame) => frame.event === "sockudo:subscription_succeeded", "subscribe");
    await client.publish({
      name: "ai-output",
      channel,
      data: "recoverable",
      messageId: "msg-recovery",
      extras: aiExtras({ discrete: "true", status: "complete" }),
    });
    await session.waitForEvent((frame) => frame.event === "ai-output", "ai output");
    return { transcript: session.transcript };
  } finally {
    session.close();
  }
}

async function lifecycleOnly(client, prefix, events) {
  const channel = uniqueChannel(prefix);
  const session = await client.connect();
  try {
    session.subscribe(channel);
    await session.waitForEvent((frame) => frame.event === "sockudo:subscription_succeeded", "subscribe");
    for (const [name, transport] of events) {
      await client.publish({
        name,
        channel,
        data: { scenario: prefix },
        extras: aiExtras(transport),
      });
    }
    const lastEvent = events.at(-1)[0];
    await session.waitForEvent((frame) => frame.event === lastEvent, lastEvent);
    return { transcript: session.transcript };
  } finally {
    session.close();
  }
}

async function mutableSequence(client, prefix, turnId, steps) {
  const channel = uniqueChannel(prefix);
  const session = await client.connect();
  try {
    session.subscribe(channel);
    await session.waitForEvent((frame) => frame.event === "sockudo:subscription_succeeded", "subscribe");
    const create = await client.publish({
      name: "ai-output",
      channel,
      data: "",
      messageId: `msg-${turnId}`,
      extras: aiExtras({ "turn-id": turnId, status: "streaming", stream: "true", role: "assistant" }),
    });
    const messageSerial = create.channels[channel].message_serial;
    for (const [kind, data, status, extra = {}] of steps) {
      if (kind === "update") {
        await client.update({
          channel,
          messageSerial,
          data,
          opId: `op-${turnId}-${kind}`,
          extras: aiExtras({ status, ...extra }),
        });
      } else {
        await client.append({
          channel,
          messageSerial,
          data,
          opId: `op-${turnId}-${data}`,
          extras: aiExtras({ status, ...extra }),
        });
      }
    }
    await session.waitForEvent((frame) => frame.event === "sockudo:message.append" || frame.event === "sockudo:message.update", "mutation");
    return { transcript: session.transcript };
  } finally {
    session.close();
  }
}

async function validateGoldenFixtures() {
  for (const scenario of scenarios) {
    const golden = await readGolden(`${scenario.name}.json`);
    assert(Array.isArray(golden), `${scenario.name}.json must contain an array`);
  }
  const forwardDir = path.join(root, "fixtures", "forward-compat");
  const files = await fs.readdir(forwardDir);
  assert(files.length > 0, "forward compatibility fixtures are required");
}

async function readGolden(file) {
  return JSON.parse(await fs.readFile(path.join(root, "fixtures", "golden", file), "utf8"));
}

function uniqueChannel(prefix) {
  return `${prefix}-${process.env.AIT_CONFORMANCE_RUN_ID ?? "golden"}`;
}
