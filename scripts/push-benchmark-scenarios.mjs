#!/usr/bin/env node
import { spawn } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const benchmarkScript = resolve(here, "push-benchmark.mjs");
const args = parseArgs(process.argv.slice(2));

const config = {
  baseUrl: args.url ?? "http://127.0.0.1:6001",
  appId: args.appId ?? "app-id",
  key: args.key ?? "app-key",
  secret: args.secret ?? "app-secret",
  profile: args.profile ?? "all",
  execute: booleanArg(args.execute, false),
  providerDispatch: args.providerDispatch ?? "disabled",
  prefix: args.prefix ?? `push-suite-${Date.now()}`,
};

if (!["disabled", "mocked"].includes(config.providerDispatch)) {
  fail("--provider-dispatch must be disabled or mocked for suite benchmarks");
}

const scenarios = selectScenarios(config.profile);
const commands = scenarios.flatMap((scenario, index) =>
  scenario.commands.map((command) => ({
    scenario: scenario.name,
    argv: [
      benchmarkScript,
      "--url",
      config.baseUrl,
      "--app-id",
      config.appId,
      "--key",
      config.key,
      "--secret",
      config.secret,
      "--prefix",
      `${config.prefix}-${index}-${scenario.name}`,
      ...command,
    ],
  })),
);

if (!config.execute) {
  printPlan(commands);
  process.exit(0);
}

for (const command of commands) {
  console.error(`\n# ${command.scenario}`);
  console.error(shellCommand("node", redactSecret(command.argv)));
  await run("node", command.argv);
}

function selectScenarios(profile) {
  const providers = ["fcm", "apns", "webpush", "hms", "wns"];
  const all = [
    {
      name: "admission-baseline",
      commands: [
        [
          "--mode",
          "admission",
          "--provider",
          "fcm",
          "--channels",
          "100",
          "--concurrency",
          "100",
          "--duration",
          "120",
        ],
      ],
    },
    {
      name: "registry-fanout",
      commands: [
        [
          "--mode",
          "all",
          "--provider",
          "fcm",
          "--devices",
          "100000",
          "--channels",
          "100",
          "--concurrency",
          "100",
          "--duration",
          "300",
        ],
      ],
    },
    {
      name: "hot-channel",
      commands: [
        [
          "--mode",
          "all",
          "--provider",
          "fcm",
          "--devices",
          "100000",
          "--channels",
          "1",
          "--hot-channel",
          "--concurrency",
          "50",
          "--duration",
          "120",
        ],
      ],
    },
    {
      name: "all-provider-admission",
      commands: providers.map((provider) => [
        "--mode",
        "admission",
        "--provider",
        provider,
        "--channels",
        "100",
        "--concurrency",
        "50",
        "--duration",
        "60",
      ]),
    },
    {
      name: "all-provider-fanout",
      commands: providers.map((provider) => [
        "--mode",
        "all",
        "--provider",
        provider,
        "--devices",
        "10000",
        "--channels",
        "100",
        "--concurrency",
        "50",
        "--duration",
        "120",
        "--status-sample",
        "50",
      ]),
    },
    {
      name: "quota-sweep",
      commands: [10, 50, 100, 200].map((concurrency) => [
        "--mode",
        "admission",
        "--provider",
        "fcm",
        "--channels",
        "100",
        "--concurrency",
        String(concurrency),
        "--duration",
        "60",
      ]),
    },
    {
      name: "soak",
      commands: [
        [
          "--mode",
          "all",
          "--provider",
          "fcm",
          "--devices",
          "50000",
          "--channels",
          "100",
          "--concurrency",
          "50",
          "--duration",
          "1800",
          "--status-sample",
          "200",
        ],
      ],
    },
  ];

  if (profile === "all") {
    return all;
  }
  const selected = all.filter((scenario) => scenario.name === profile);
  if (selected.length === 0) {
    fail(
      `unknown --profile ${profile}; expected all or one of ${all
        .map((scenario) => scenario.name)
        .join(", ")}`,
    );
  }
  return selected;
}

function printPlan(commands) {
  console.log("# Push benchmark scenario plan");
  console.log("# This is dry-run output. Add --execute to run.");
  console.log("# Keep real provider dispatch disabled or pointed at a mock provider.");
  for (const command of commands) {
    console.log(`\n# ${command.scenario}`);
    console.log(shellCommand("node", redactSecret(command.argv)));
  }
}

function redactSecret(argv) {
  const output = [...argv];
  for (let index = 0; index < output.length; index += 1) {
    if (output[index] === "--secret" && output[index + 1]) {
      output[index + 1] = "[REDACTED]";
    }
  }
  return output;
}

function shellCommand(binary, argv) {
  return [binary, ...argv].map(shellQuote).join(" ");
}

function shellQuote(value) {
  if (/^[A-Za-z0-9_./:=@+-]+$/.test(value)) {
    return value;
  }
  return `'${value.replaceAll("'", "'\\''")}'`;
}

function run(binary, argv) {
  return new Promise((resolveRun, reject) => {
    const child = spawn(binary, argv, { stdio: "inherit" });
    child.on("error", reject);
    child.on("exit", (code, signal) => {
      if (code === 0) {
        resolveRun();
      } else {
        reject(new Error(`${binary} exited with ${signal || code}`));
      }
    });
  });
}

function parseArgs(values) {
  const parsed = {};
  for (let index = 0; index < values.length; index += 1) {
    const arg = values[index];
    if (!arg.startsWith("--")) {
      fail(`unexpected argument ${arg}`);
    }
    const [rawKey, inlineValue] = arg.slice(2).split("=", 2);
    const key = rawKey.replace(/-([a-z])/g, (_, char) => char.toUpperCase());
    if (inlineValue !== undefined) {
      parsed[key] = inlineValue;
    } else if (values[index + 1] && !values[index + 1].startsWith("--")) {
      parsed[key] = values[index + 1];
      index += 1;
    } else {
      parsed[key] = "true";
    }
  }
  return parsed;
}

function booleanArg(value, fallback) {
  if (value === undefined) {
    return fallback;
  }
  return value === true || value === "true" || value === "1";
}

function fail(message) {
  console.error(message);
  process.exit(2);
}
