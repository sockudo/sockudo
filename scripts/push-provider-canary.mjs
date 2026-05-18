#!/usr/bin/env node
import crypto from "node:crypto";
import { readFile } from "node:fs/promises";
import { performance } from "node:perf_hooks";

const args = parseArgs(process.argv.slice(2));
const provider = normalizeProvider(args.provider ?? "fcm");
const config = {
  baseUrl: args.url ?? "http://127.0.0.1:6001",
  appId: args.appId ?? "app-id",
  key: args.key ?? "app-key",
  secret: args.secret ?? "app-secret",
  provider,
  channel: args.channel ?? `push-canary-${provider}-${Date.now()}`,
  prefix: args.prefix ?? `push-canary-${Date.now()}`,
  maxSends: numberArg(args.maxSends, 10),
  maxProviderAttempts: numberArg(args.maxProviderAttempts, 100),
  intervalMs: numberArg(args.intervalMs, 1000),
  execute: booleanArg(args.execute, false),
  uploadCredential: booleanArg(args.uploadCredential, false),
};

if (!config.execute) {
  console.log(
    JSON.stringify(
      {
        dryRun: true,
        message: "Add --execute to run the bounded real-provider canary.",
        config: redactConfig(config),
        credentialUpload: credentialPlan(),
        recipientInput: recipientPlan(),
      },
      null,
      2,
    ),
  );
  process.exit(0);
}

const recipients = await loadRecipients();
if (recipients.length === 0) {
  fail("no recipients supplied; use --token, --token-file, or --recipient-json-file");
}
const providerAttempts = recipients.length * config.maxSends;
if (providerAttempts > config.maxProviderAttempts) {
  fail(
    `refusing ${providerAttempts} provider attempts; raise --max-provider-attempts deliberately if this is intended`,
  );
}

const summary = {
  startedAt: new Date().toISOString(),
  config: redactConfig(config),
  recipients: recipients.length,
  providerAttempts,
  credential: null,
  devices: [],
  publishes: [],
};

if (config.uploadCredential) {
  summary.credential = await uploadCredential();
}

for (let index = 0; index < recipients.length; index += 1) {
  summary.devices.push(await registerRecipient(index, recipients[index]));
}

for (let index = 0; index < config.maxSends; index += 1) {
  const started = performance.now();
  const publishId = `${config.prefix}-publish-${index}`;
  const publish = await signedJson("POST", "/push/publish", {
    publishId,
    recipients: [{ type: "channel", channel: config.channel }],
    payload: {
      title: "Sockudo Provider Canary",
      body: `Canary ${index + 1}/${config.maxSends} for ${config.provider}`,
      collapseKey: publishId,
      templateData: {
        provider: config.provider,
        index,
        startedAt: summary.startedAt,
      },
    },
    providerOverrides: [],
    sync: false,
  }, pushAdminHeaders());
  const status = await signedJson(
    "GET",
    `/push/publish/${encodeURIComponent(publish.publishId ?? publish.publish_id ?? publishId)}/status`,
    undefined,
    pushAdminHeaders(),
  );
  summary.publishes.push({
    publishId: publish.publishId ?? publish.publish_id ?? publishId,
    status: publish.status,
    expectedRecipients: publish.expectedRecipients,
    state: status.state,
    counters: status.counters,
    latencyMs: round(performance.now() - started),
  });
  if (index + 1 < config.maxSends) {
    await sleep(config.intervalMs);
  }
}

console.log(JSON.stringify(summary, null, 2));

async function uploadCredential() {
  switch (config.provider) {
    case "fcm":
      return signedJson("POST", "/push/credentials/fcm", {
        credentialId: args.credentialId ?? "fcm",
        serviceAccountJson: await readJsonFile(requiredArg("service-account-json-file")),
      }, pushAdminHeaders());
    case "apns":
      return signedJson("POST", "/push/credentials/apns", {
        credentialId: args.credentialId ?? "apns",
        p12: args.p12File ? await readBase64File(args.p12File) : undefined,
        p12Password: args.p12Password,
        pem: args.pemFile ? await readTextFile(args.pemFile) : undefined,
        teamId: args.apnsTeamId,
        keyId: args.apnsKeyId,
        privateKey: args.apnsPrivateKeyFile
          ? await readTextFile(args.apnsPrivateKeyFile)
          : args.apnsPrivateKey,
      }, pushAdminHeaders());
    case "webPush":
      return signedJson("POST", "/push/credentials/webpush", {
        credentialId: args.credentialId ?? "webpush",
        publicKey: requiredArg("vapid-public-key"),
        privateKey: requiredArg("vapid-private-key"),
      }, pushAdminHeaders());
    case "hms":
      return signedJson("POST", "/push/credentials/hms", {
        credentialId: args.credentialId ?? "hms",
        hmsAppId: requiredArg("hms-app-id"),
        clientSecret: requiredArg("hms-client-secret"),
      }, pushAdminHeaders());
    case "wns":
      return signedJson("POST", "/push/credentials/wns", {
        credentialId: args.credentialId ?? "wns",
        packageSid: requiredArg("wns-package-sid"),
        clientSecret: requiredArg("wns-client-secret"),
      }, pushAdminHeaders());
    default:
      fail(`unsupported provider: ${config.provider}`);
  }
}

async function registerRecipient(index, recipient) {
  const deviceId = `${config.prefix}-device-${index}`;
  const activation = await signedJson("POST", "/push/deviceRegistrations", {
    appId: config.appId,
    id: deviceId,
    clientId: `${config.prefix}-client-${index}`,
    deviceSecret: `${config.prefix}-secret-${index}`,
    formFactor: formFactorForProvider(config.provider),
    platform: platformForProvider(config.provider),
    timezone: "UTC",
    locale: "en",
    push: {
      state: "ACTIVE",
      recipient,
    },
  }, {
    ...pushAdminHeaders(),
    "x-sockudo-rotate-device-identity-token": "true",
  });
  const subscription = await signedJson("POST", "/push/channelSubscriptions", {
    appId: config.appId,
    channel: config.channel,
    deviceId,
    clientId: `${config.prefix}-client-${index}`,
    provider: config.provider,
    tokenHash: activation.tokenHash,
    credentialVersion: 1,
  }, pushAdminHeaders());
  return {
    deviceId,
    change: activation.change,
    tokenHashLength: activation.tokenHash?.length,
    subscription: {
      channel: subscription.channel,
      provider: subscription.provider,
    },
  };
}

async function loadRecipients() {
  if (args.recipientJsonFile) {
    const parsed = await readJsonFile(args.recipientJsonFile);
    return Array.isArray(parsed) ? parsed : [parsed];
  }
  const tokens = [];
  if (args.token) {
    tokens.push(args.token);
  }
  if (args.tokenFile) {
    const fileTokens = (await readTextFile(args.tokenFile))
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean)
      .filter((line) => !line.startsWith("#"));
    tokens.push(...fileTokens);
  }
  if (config.provider === "webPush" && tokens.length === 0 && args.webpushEndpoint) {
    return [
      {
        transportType: "web",
        endpoint: args.webpushEndpoint,
        p256dh: requiredArg("webpush-p256dh"),
        auth: requiredArg("webpush-auth"),
      },
    ];
  }
  return tokens.map(tokenToRecipient);
}

function tokenToRecipient(token) {
  switch (config.provider) {
    case "fcm":
      return { transportType: "gcm", registrationToken: token };
    case "apns":
      return { transportType: "apns", deviceToken: token };
    case "hms":
      return { transportType: "hms", registrationToken: token };
    case "wns":
      return { transportType: "wns", channelUri: token };
    case "webPush":
      fail("webPush canary requires --recipient-json-file or --webpush-endpoint/--webpush-p256dh/--webpush-auth");
      break;
    default:
      fail(`unsupported provider: ${config.provider}`);
  }
}

async function signedJson(method, pushPath, body, headers = {}) {
  const fullPath = `/apps/${config.appId}${pushPath}`;
  const bodyText = body === undefined ? undefined : JSON.stringify(stripUndefined(body));
  const query = {
    auth_key: config.key,
    auth_timestamp: `${Math.floor(Date.now() / 1_000)}`,
    auth_version: "1.0",
  };
  if (bodyText !== undefined) {
    query.body_md5 = crypto.createHash("md5").update(bodyText).digest("hex");
  }
  const canonicalQuery = Object.keys(query)
    .map((key) => [key.toLowerCase(), query[key]])
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => `${key}=${value}`)
    .join("&");
  const stringToSign = `${method}\n${fullPath}\n${canonicalQuery}`;
  query.auth_signature = crypto
    .createHmac("sha256", config.secret)
    .update(stringToSign)
    .digest("hex");

  const url = new URL(`${config.baseUrl}${fullPath}`);
  for (const [key, value] of Object.entries(query)) {
    url.searchParams.set(key, value);
  }

  const response = await fetch(url, {
    method,
    headers: {
      "content-type": "application/json",
      ...headers,
    },
    body: bodyText,
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${text}`);
  }
  return text ? JSON.parse(text) : {};
}

function credentialPlan() {
  if (!config.uploadCredential) {
    return "skipped; pass --upload-credential to write provider credentials";
  }
  return `upload ${config.provider} credential before registering devices`;
}

function recipientPlan() {
  return {
    token: Boolean(args.token),
    tokenFile: args.tokenFile ?? null,
    recipientJsonFile: args.recipientJsonFile ?? null,
    webpushEndpoint: args.webpushEndpoint ?? null,
  };
}

function pushAdminHeaders() {
  return { "x-sockudo-push-capability": "push-admin" };
}

function stripUndefined(value) {
  if (Array.isArray(value)) {
    return value.map(stripUndefined);
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .filter(([, entry]) => entry !== undefined)
        .map(([key, entry]) => [key, stripUndefined(entry)]),
    );
  }
  return value;
}

function platformForProvider(provider) {
  switch (provider) {
    case "fcm":
    case "hms":
      return "android";
    case "apns":
      return "ios";
    case "webPush":
      return "browser";
    case "wns":
      return "windows";
    default:
      return "other";
  }
}

function formFactorForProvider(provider) {
  return provider === "webPush" || provider === "wns" ? "desktop" : "phone";
}

async function readJsonFile(path) {
  return JSON.parse(await readTextFile(path));
}

async function readTextFile(path) {
  return readFile(path, "utf8");
}

async function readBase64File(path) {
  return (await readFile(path)).toString("base64");
}

function requiredArg(name) {
  const key = name.replace(/-([a-z])/g, (_, char) => char.toUpperCase());
  if (!args[key]) {
    fail(`missing required --${name}`);
  }
  return args[key];
}

function sleep(ms) {
  return new Promise((resolveSleep) => setTimeout(resolveSleep, ms));
}

function round(value) {
  return Math.round(value * 100) / 100;
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

function numberArg(value, fallback) {
  if (value === undefined) {
    return fallback;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    fail(`invalid numeric argument: ${value}`);
  }
  return parsed;
}

function booleanArg(value, fallback) {
  if (value === undefined) {
    return fallback;
  }
  return value === true || value === "true" || value === "1";
}

function normalizeProvider(value) {
  const normalized = String(value).toLowerCase();
  switch (normalized) {
    case "fcm":
    case "gcm":
      return "fcm";
    case "apns":
    case "apple":
      return "apns";
    case "webpush":
    case "web-push":
    case "web_push":
      return "webPush";
    case "hms":
      return "hms";
    case "wns":
      return "wns";
    default:
      fail(`unknown --provider ${value}; expected fcm, apns, webpush, hms, or wns`);
  }
}

function redactConfig(input) {
  return {
    ...input,
    secret: "[REDACTED]",
  };
}

function fail(message) {
  console.error(message);
  process.exit(2);
}
