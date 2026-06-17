#!/usr/bin/env node
import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";

const args = parseArgs(process.argv.slice(2));
const execute = Boolean(args.execute);
const output = args.output ?? "docs/specs/ai-transport-results/sdk-compat-full-matrix.json";
const root = new URL("..", import.meta.url).pathname;
const started = performance.now();
const profiles = [
  {
    name: "default",
    baseUrl: args.defaultBaseUrl ?? process.env.SOCKUDO_DEFAULT_BASE_URL ?? "http://127.0.0.1:6001",
  },
  {
    name: "ai-enabled",
    baseUrl: args.aiBaseUrl ?? process.env.SOCKUDO_AI_BASE_URL ?? "http://127.0.0.1:6001",
  },
];

const lanes = [
  client("sockudo-js", "client-sdks/sockudo-js", ["bash", "-lc", "bun install --frozen-lockfile && bun run check"]),
  client("sockudo-dotnet", "client-sdks/sockudo-dotnet", ["dotnet", "test"]),
  client("sockudo-flutter", "client-sdks/sockudo-flutter", ["bash", "-lc", "flutter pub get && flutter test"]),
  client("sockudo-kotlin", "client-sdks/sockudo-kotlin", ["./gradlew", "test"]),
  client("sockudo-swift", "client-sdks/sockudo-swift", ["swift", "test"]),
  server("sockudo-http-node", "server-sdks/sockudo-http-node", ["bash", "-lc", "npm install && npm test"]),
  server("sockudo-http-dotnet", "server-sdks/sockudo-http-dotnet", ["dotnet", "test"]),
  server("sockudo-http-go", "server-sdks/sockudo-http-go", ["go", "test", "./..."]),
  server("sockudo-http-java", "server-sdks/sockudo-http-java", ["./gradlew", "test"]),
  server("sockudo-http-php", "server-sdks/sockudo-http-php", ["bash", "-lc", "composer install && ./vendor/bin/phpunit"]),
  server("sockudo-http-python", "server-sdks/sockudo-http-python", ["bash", "-lc", "python3 -m pip install -e '.[dev]' && python3 -m pytest"]),
  server("sockudo-http-ruby", "server-sdks/sockudo-http-ruby", ["bash", "-lc", "bundle install && bundle exec rake test"]),
  server("sockudo-http-rust", "server-sdks/sockudo-http-rust", ["cargo", "test"]),
  server("sockudo-http-swift", "server-sdks/sockudo-http-swift", ["swift", "test"]),
  {
    kind: "canary",
    name: "pusher-js-v1-canary",
    cwd: "tests/ai-conformance",
    command: ["bash", "-lc", "AIT_CONFORMANCE_OFFLINE=1 node src/run.mjs"],
    profiles: ["default", "ai-enabled"],
  },
];

const results = [];
for (const lane of lanes) {
  const laneStarted = performance.now();
  if (!execute) {
    results.push({
      ...publicLane(lane),
      status: "planned",
      profileResults: profiles.map((profile) => ({
        profile: profile.name,
        baseUrl: profile.baseUrl,
        status: "planned",
        durationSeconds: 0,
      })),
      durationSeconds: 0,
    });
    continue;
  }
  results.push(await runLane(lane, laneStarted));
}

const failed = results.filter((lane) => lane.status !== "passed");
const manifest = {
  schema: "sockudo.ai-transport.sdk-compat-matrix.v1",
  status: execute ? (failed.length === 0 ? "passed" : "failed") : "planned",
  generatedAt: new Date().toISOString(),
  host: {
    hostname: os.hostname(),
    platform: os.platform(),
    release: os.release(),
    cpus: os.cpus().length,
  },
  profiles: profiles.map((profile) => ({ ...profile })),
  requiredCounts: {
    clientSdks: 5,
    serverHttpSdks: 9,
    canaries: 1,
  },
  counts: {
    clientSdks: results.filter((lane) => lane.kind === "client").length,
    serverHttpSdks: results.filter((lane) => lane.kind === "server-http").length,
    canaries: results.filter((lane) => lane.kind === "canary").length,
    passed: results.filter((lane) => lane.status === "passed").length,
    failed: results.filter((lane) => lane.status === "failed").length,
    planned: results.filter((lane) => lane.status === "planned").length,
  },
  lanes: results,
  durationSeconds: secondsSince(started),
};

await fs.mkdir(path.dirname(output), { recursive: true });
await fs.writeFile(output, `${JSON.stringify(manifest, null, 2)}\n`);
process.stdout.write(`${JSON.stringify(manifest, null, 2)}\n`);

if (execute && manifest.status !== "passed") {
  process.exitCode = 1;
}

function client(name, cwd, command) {
  return { kind: "client", name, cwd, command };
}

function server(name, cwd, command) {
  return { kind: "server-http", name, cwd, command };
}

function publicLane(lane) {
  return {
    kind: lane.kind,
    name: lane.name,
    cwd: lane.cwd,
    command: lane.command.join(" "),
  };
}

async function runLane(lane, laneStarted) {
  const absoluteCwd = path.join(root, lane.cwd);
  try {
    await fs.access(absoluteCwd);
  } catch {
    return {
      ...publicLane(lane),
      status: "failed",
      exitCode: null,
      reason: "lane directory missing",
      durationSeconds: secondsSince(laneStarted),
    };
  }

  const profileResults = [];
  for (const profile of profiles) {
    const [command, ...commandArgs] = lane.command;
    const result = await spawnCapture(command, commandArgs, absoluteCwd, profile);
    profileResults.push({
      profile: profile.name,
      baseUrl: profile.baseUrl,
      status: result.code === 0 ? "passed" : "failed",
      exitCode: result.code,
      durationSeconds: result.durationSeconds,
      stdoutTail: tail(result.stdout),
      stderrTail: tail(result.stderr),
    });
  }
  const failed = profileResults.filter((profile) => profile.status !== "passed");
  return {
    ...publicLane(lane),
    status: failed.length === 0 ? "passed" : "failed",
    profileResults,
    durationSeconds: secondsSince(laneStarted),
  };
}

function spawnCapture(command, commandArgs, cwd, profile) {
  return new Promise((resolve) => {
    const startedAt = performance.now();
    const child = spawn(command, commandArgs, {
      cwd,
      env: {
        ...process.env,
        SOCKUDO_COMPAT_PROFILE: profile.name,
        SOCKUDO_BASE_URL: profile.baseUrl,
        SOCKUDO_WS_URL: profile.baseUrl.replace(/^http:/, "ws:").replace(/^https:/, "wss:").replace(/\/$/, "") + "/app/app-key?protocol=2&client=sdk-compat&version=0",
        SOCKUDO_APP_ID: process.env.SOCKUDO_APP_ID ?? "app-id",
        SOCKUDO_APP_KEY: process.env.SOCKUDO_APP_KEY ?? "app-key",
        SOCKUDO_APP_SECRET: process.env.SOCKUDO_APP_SECRET ?? "app-secret",
      },
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
      resolve({ code: 127, stdout, stderr: `${stderr}\n${error.message}`, durationSeconds: secondsSince(startedAt) });
    });
    child.on("close", (code) => {
      resolve({ code, stdout, stderr, durationSeconds: secondsSince(startedAt) });
    });
  });
}

function tail(text, max = 6000) {
  if (!text) {
    return "";
  }
  return text.length > max ? text.slice(text.length - max) : text;
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
