#!/usr/bin/env node
import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import { performance } from "node:perf_hooks";

const args = parseArgs(process.argv.slice(2));
const execute = Boolean(args.execute);
const output = args.output ?? "docs/specs/ai-transport-results/rolling-upgrade-redis.json";
const started = performance.now();

const requiredConfirm = process.env.AIT_RELEASE_EVIDENCE_CONFIRM === "external-rc";
if (execute && !requiredConfirm) {
  console.error("refusing rolling-upgrade execution without AIT_RELEASE_EVIDENCE_CONFIRM=external-rc");
  process.exit(64);
}

const phases = [
  {
    name: "old-nodes-ai-disabled",
    hook: args.preHook,
    description: "previous release on shared Redis with V1/V2 non-AI traffic",
    aiEnabled: false,
  },
  {
    name: "mixed-old-new-ai-disabled",
    hook: args.mixedHook,
    description: "mixed previous release and release candidate on shared Redis",
    aiEnabled: false,
  },
  {
    name: "all-new-ai-enabled",
    hook: args.postHook,
    description: "all nodes upgraded to release candidate, AI Transport enabled",
    aiEnabled: true,
  },
];

const results = [];
for (const phase of phases) {
  const phaseStarted = performance.now();
  if (!execute) {
    results.push({
      ...phase,
      status: "planned",
      durationSeconds: 0,
    });
    continue;
  }

  const hookResult = phase.hook ? await runShell(phase.hook) : { status: "skipped", stdoutTail: "", stderrTail: "" };
  const profileArgs = [
    "test/load/ai-scale-runner.mjs",
    "--profile",
    args.profile ?? "test/load/profiles/smoke.json",
    "--execute",
    "--name",
    `rolling-upgrade-${phase.name}`,
    "--output",
    `target/ai-rolling-upgrade/${phase.name}.json`,
    "--activeStreams",
    args.activeStreams ?? "16",
    "--durationSeconds",
    args.durationSeconds ?? "10",
    "--sampleTranscriptCount",
    args.sampleTranscriptCount ?? "16",
  ];
  const loadResult = await runCommand("node", profileArgs);
  results.push({
    ...phase,
    status: hookResult.status === "passed" || hookResult.status === "skipped"
      ? loadResult.status
      : "failed",
    hook: hookResult,
    load: loadResult,
    durationSeconds: secondsSince(phaseStarted),
  });
}

const failed = results.filter((phase) => phase.status !== "passed");
const manifest = {
  schema: "sockudo.ai-transport.rolling-upgrade-result.v1",
  status: execute ? (failed.length === 0 ? "passed" : "failed") : "planned",
  generatedAt: new Date().toISOString(),
  host: {
    hostname: os.hostname(),
    platform: os.platform(),
    release: os.release(),
    cpus: os.cpus().length,
  },
  sharedRedis: true,
  requiredBehavior: [
    "V1 traffic unaffected in old and mixed phases",
    "Protocol V2 non-AI traffic unaffected in old and mixed phases",
    "AI Transport remains disabled or degrades cleanly until all nodes are upgraded",
    "AI stream/recovery smoke passes after all nodes are upgraded and AI is enabled",
  ],
  phases: results,
  durationSeconds: secondsSince(started),
};

await fs.mkdir(dirname(output), { recursive: true });
await fs.writeFile(output, `${JSON.stringify(manifest, null, 2)}\n`);
process.stdout.write(`${JSON.stringify(manifest, null, 2)}\n`);

if (execute && manifest.status !== "passed") {
  process.exitCode = 1;
}

async function runShell(command) {
  return runCommand("bash", ["-lc", command]);
}

function runCommand(command, args) {
  return new Promise((resolve) => {
    const child = spawn(command, args, {
      env: process.env,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
      process.stdout.write(chunk);
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
      process.stderr.write(chunk);
    });
    child.on("error", (error) => {
      resolve({ status: "failed", exitCode: 127, stdoutTail: tail(stdout), stderrTail: tail(`${stderr}\n${error.message}`) });
    });
    child.on("close", (code) => {
      resolve({ status: code === 0 ? "passed" : "failed", exitCode: code, stdoutTail: tail(stdout), stderrTail: tail(stderr) });
    });
  });
}

function tail(text, max = 6000) {
  if (!text) {
    return "";
  }
  return text.length > max ? text.slice(text.length - max) : text;
}

function dirname(path) {
  const parts = path.split("/");
  parts.pop();
  return parts.join("/") || ".";
}

function secondsSince(startedAt) {
  return Number(((performance.now() - startedAt) / 1000).toFixed(3));
}

function parseArgs(argv) {
  const parsed = {};
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (!arg.startsWith("--")) {
      continue;
    }
    const key = arg.slice(2);
    const next = argv[index + 1];
    if (next === undefined || next.startsWith("--")) {
      parsed[key] = true;
    } else {
      parsed[key] = next;
      index += 1;
    }
  }
  return parsed;
}
