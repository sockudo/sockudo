import { spawn } from 'node:child_process';
import { createHmac, randomBytes } from 'node:crypto';
import {
  closeSync,
  mkdirSync,
  mkdtempSync,
  openSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { createServer } from 'node:http';
import { createRequire } from 'node:module';
import { createServer as createTcpServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const FULL_CAPABILITY = '{"*":["*"]}';
const MAX_REQUEST_BYTES = 5 * 1024 * 1024;
const ACTION = Object.freeze({ CONNECTED: 4, ERROR: 9, ATTACH: 10, ATTACHED: 11, PRESENCE: 14 });
const PRESENCE_ENTER = 2;

function token(bytes) {
  return randomBytes(bytes).toString('base64url');
}

function encodedJson(value) {
  return Buffer.from(JSON.stringify(value)).toString('base64url');
}

export function createJwt(url) {
  const keyName = url.searchParams.get('keyName');
  const keySecret = url.searchParams.get('keySecret');
  const expiresInText = url.searchParams.get('expiresIn') ?? '3600';
  if (!keyName || Buffer.byteLength(keyName) > 512) throw new Error('invalid JWT key name');
  if (!keySecret || Buffer.byteLength(keySecret) > 4096) throw new Error('invalid JWT key secret');
  if (!/^\d+$/.test(expiresInText)) throw new Error('invalid JWT expiry');
  const expiresIn = Number(expiresInText);
  if (!Number.isSafeInteger(expiresIn) || expiresIn < 1 || expiresIn > 86_400) {
    throw new Error('invalid JWT expiry');
  }
  const now = Math.floor(Date.now() / 1000);
  const claims = { iat: now, exp: now + expiresIn };
  for (const [queryName, claimName] of [
    ['clientId', 'x-ably-clientId'],
    ['capability', 'x-ably-capability'],
    ['revocationKey', 'x-ably-revocation-key'],
  ]) {
    const value = url.searchParams.get(queryName);
    if (value) claims[claimName] = value;
  }
  const signingInput = `${encodedJson({ typ: 'JWT', alg: 'HS256', kid: keyName })}.${encodedJson(claims)}`;
  const signature = createHmac('sha256', keySecret).update(signingInput).digest('base64url');
  return `${signingInput}.${signature}`;
}

function tomlString(value) {
  return `"${String(value).replaceAll('\\', '\\\\').replaceAll('"', '\\"')}"`;
}

function capabilityString(capability) {
  if (capability === undefined || capability === null) return FULL_CAPABILITY;
  return typeof capability === 'string' ? capability : JSON.stringify(capability);
}

function regexEscape(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

export function createAppDefinition(request) {
  const appId = `app-${token(6)}`;
  const requestedKeys = request.keys?.length ? request.keys : [{}];
  const configKeys = [];
  const responseKeys = [];

  requestedKeys.forEach((requested, index) => {
    const id = `k${index}`;
    const secret = token(18);
    const keyName = `${appId}.${id}`;
    const keyStr = `${keyName}:${secret}`;
    const capability = capabilityString(requested.capability);

    configKeys.push({
      id,
      secret,
      capability: requested.capability == null ? undefined : capability,
      revocableTokens: requested.revocableTokens === true,
    });
    responseKeys.push({
      ...requested,
      id,
      value: secret,
      keyName,
      keySecret: secret,
      keyStr,
      capability,
    });
  });

  return {
    accountId: `account-${token(4)}`,
    appId,
    channels: request.channels ?? [],
    configKeys,
    namespaces: request.namespaces ?? [],
    primaryKey: responseKeys[0].keyStr,
    responseKeys,
  };
}

function namespaceConfig(namespace) {
  const id = String(namespace.id);
  const values = [
    '[[app_manager.array.apps.policy.channels.channel_namespaces]]',
    `name = ${tomlString(id)}`,
    `channel_name_pattern = ${tomlString(`^${regexEscape(id)}:.+$`)}`,
    'max_channel_name_length = 200',
  ];
  if (namespace.mutableMessages === true) values.push('annotations_enabled = true');
  return values.join('\n');
}

function appManagerConfig(definition) {
  const primary = definition.configKeys[0];
  const namespaces = definition.namespaces.map(namespaceConfig).join('\n\n');
  return `[[app_manager.array.apps]]
id = ${tomlString(definition.appId)}
key = ${tomlString(`${definition.appId}.${primary.id}`)}
secret = ${tomlString(primary.secret)}
enabled = true

[app_manager.array.apps.policy.limits]
max_connections = 100000
max_client_events_per_second = 1000

[app_manager.array.apps.policy.features]
enable_client_messages = true
enable_user_authentication = true

[app_manager.array.apps.policy.channels]
allowed_origins = ["*"]

${namespaces}

[app_manager.array.apps.policy.idempotency]
enabled = true
ttl_seconds = 300

[app_manager.array.apps.policy.connection_recovery]
enabled = true
`;
}

function compatibilityConfig(definition) {
  const keys = definition.configKeys
    .map((key) => {
      const values = [
        '[[ably_compat.keys]]',
        `app_id = ${tomlString(definition.appId)}`,
        `key_name = ${tomlString(`${definition.appId}.${key.id}`)}`,
        `secret = ${tomlString(key.secret)}`,
        'enabled = true',
      ];
      if (key.capability !== undefined) values.push(`capability = ${tomlString(key.capability)}`);
      if (key.revocableTokens) values.push('revocable_tokens = true');
      return values.join('\n');
    })
    .join('\n\n');

  return `enabled = true
stats_fixture_ingest_enabled = true

${keys}
`;
}

function replaceSection(source, startMarker, endMarker, body) {
  const start = new RegExp(`^${regexEscape(startMarker)}$`, 'm').exec(source);
  const end = new RegExp(`^${regexEscape(endMarker)}$`, 'm').exec(source);
  if (!start || !end || end.index <= start.index) {
    throw new Error(`config template is missing ${startMarker} or ${endMarker}`);
  }
  return `${source.slice(0, start.index + startMarker.length)}\n${body}\n${source.slice(end.index)}`;
}

export function renderConfig(template, port, definition) {
  let config = template.replace(/^port\s*=\s*\d+\s*$/m, `port = ${port}`);
  config = replaceSection(
    config,
    '[app_manager.array]',
    '[app_manager.cache]',
    appManagerConfig(definition),
  );
  config = replaceSection(config, '[ably_compat]', '[cache]', compatibilityConfig(definition));
  // Each fixture runs locally without external push services or provider credentials.
  config = replaceSection(config, '[push]', '[push.retry]', `
storage_driver = "memory"
queue_driver = "memory"
allow_memory_drivers = true
fcm_enabled = false
apns_enabled = false
webpush_enabled = false
hms_enabled = false
wns_enabled = false
`);
  return config.replace(/^enabled\s*=\s*true\s*\n(driver\s*=\s*"prometheus")/m, 'enabled = false\n$1');
}

function readBody(request) {
  return new Promise((resolveBody, reject) => {
    const chunks = [];
    let size = 0;
    request.on('data', (chunk) => {
      size += chunk.length;
      if (size > MAX_REQUEST_BYTES) {
        reject(new Error('request body is too large'));
        request.destroy();
        return;
      }
      chunks.push(chunk);
    });
    request.on('end', () => resolveBody(Buffer.concat(chunks).toString('utf8')));
    request.on('error', reject);
  });
}

function sendJson(response, status, value) {
  const body = JSON.stringify(value);
  response.writeHead(status, {
    'content-length': Buffer.byteLength(body),
    'content-type': 'application/json',
  });
  response.end(body);
}

function sendText(response, status, body) {
  response.writeHead(status, { 'content-type': 'text/plain; charset=utf-8' });
  response.end(body);
}

function lastLogLines(path, count = 20) {
  try {
    return readFileSync(path, 'utf8').trim().split('\n').slice(-count).join('\n');
  } catch {
    return '';
  }
}

function portIsAvailable(port) {
  return new Promise((resolveAvailable) => {
    const server = createTcpServer();
    server.once('error', () => resolveAvailable(false));
    server.listen(port, '127.0.0.1', () => server.close(() => resolveAvailable(true)));
  });
}

function wait(milliseconds) {
  return new Promise((resolveWait) => setTimeout(resolveWait, milliseconds));
}

export function createSandbox({
  binary,
  configTemplate,
  logDirectory,
  childPortBase = 7100,
  WebSocketImplementation = globalThis.WebSocket,
}) {
  if (!binary || !configTemplate || !logDirectory) {
    throw new Error('binary, configTemplate, and logDirectory are required');
  }

  mkdirSync(logDirectory, { recursive: true });
  const children = new Map();
  let portCursor = childPortBase;

  async function allocatePort() {
    for (let attempt = 0; attempt < 1000; attempt += 1) {
      const port = portCursor;
      portCursor += 1;
      if (await portIsAvailable(port)) return port;
    }
    throw new Error('no free child port found');
  }

  function stopChild(child) {
    if (child.presenceSocket) child.presenceSocket.close();
    if (child.process.exitCode === null && child.process.signalCode === null) {
      child.process.kill('SIGKILL');
    }
    rmSync(child.directory, { force: true, recursive: true });
  }

  async function waitUntilReady(child) {
    for (let attempt = 0; attempt < 150; attempt += 1) {
      if (child.process.exitCode !== null || child.process.signalCode !== null) {
        throw new Error(`Sockudo exited during startup:\n${lastLogLines(child.logPath)}`);
      }
      try {
        if ((await fetch(`http://127.0.0.1:${child.port}/time`)).ok) return;
      } catch {
        // The listener is not ready yet.
      }
      await wait(200);
    }
    throw new Error(`Sockudo did not start within 30 seconds:\n${lastLogLines(child.logPath)}`);
  }

  async function seedPresence(child, definition) {
    const channels = definition.channels.filter(
      (channel) => channel.name && channel.presence?.length,
    );
    if (channels.length === 0) return;
    if (!WebSocketImplementation) {
      throw new Error('presence fixtures require a WebSocket implementation');
    }

    await new Promise((resolveSeed, reject) => {
      const socket = new WebSocketImplementation(
        `ws://127.0.0.1:${child.port}/?key=${encodeURIComponent(definition.primaryKey)}&format=json&v=3`,
      );
      const pending = new Map(channels.map((channel) => [channel.name, channel]));
      const timeout = setTimeout(
        () => finish(new Error('presence fixture seeding timed out')),
        15_000,
      );

      function finish(error) {
        clearTimeout(timeout);
        socket.removeEventListener('message', onMessage);
        socket.removeEventListener('error', onError);
        if (error) {
          socket.close();
          reject(error);
        } else {
          child.presenceSocket = socket;
          resolveSeed();
        }
      }

      function onError() {
        finish(new Error('presence fixture WebSocket failed'));
      }

      function onMessage(event) {
        let message;
        try {
          message = JSON.parse(event.data);
        } catch {
          return;
        }

        if (message.action === ACTION.CONNECTED) {
          for (const channel of channels) {
            socket.send(JSON.stringify({ action: ACTION.ATTACH, channel: channel.name }));
          }
          return;
        }
        if (message.action === ACTION.ATTACHED && pending.has(message.channel)) {
          const channel = pending.get(message.channel);
          socket.send(
            JSON.stringify({
              action: ACTION.PRESENCE,
              channel: channel.name,
              presence: channel.presence.map((member) => ({
                action: PRESENCE_ENTER,
                clientId: member.clientId,
                ...(member.data === undefined ? {} : { data: member.data }),
                ...(member.encoding === undefined ? {} : { encoding: member.encoding }),
              })),
            }),
          );
          pending.delete(message.channel);
          if (pending.size === 0) finish();
          return;
        }
        if (message.action === ACTION.ERROR) {
          finish(
            new Error(`presence fixture rejected with code ${message.error?.code ?? 'unknown'}`),
          );
        }
      }

      socket.addEventListener('message', onMessage);
      socket.addEventListener('error', onError);
    });
  }

  async function startChild(definition) {
    const port = await allocatePort();
    const directory = mkdtempSync(join(tmpdir(), 'sockudo-upstream-'));
    const configPath = join(directory, 'config.toml');
    const logPath = join(logDirectory, `${definition.appId}.log`);
    writeFileSync(configPath, renderConfig(configTemplate, port, definition));

    const logFile = openSync(logPath, 'w');
    const process = spawn(binary, ['--config', configPath], {
      stdio: ['ignore', logFile, logFile],
    });
    closeSync(logFile);
    const child = { definition, directory, logPath, port, presenceSocket: undefined, process };

    try {
      await waitUntilReady(child);
      await seedPresence(child, definition);
      return child;
    } catch (error) {
      stopChild(child);
      throw error;
    }
  }

  function appIdFromRequest(request) {
    const authorization = request.headers.authorization;
    const rawKey = authorization?.startsWith('Basic ')
      ? Buffer.from(authorization.slice(6), 'base64').toString('utf8')
      : new URL(request.url, 'http://127.0.0.1').searchParams.get('key') ?? '';
    return rawKey.split(':', 1)[0].split('.', 1)[0] || undefined;
  }

  async function createApp(request, response) {
    const body = JSON.parse(await readBody(request));
    const definition = createAppDefinition(body);
    const child = await startChild(definition);
    children.set(definition.appId, child);
    sendJson(response, 201, {
      accountId: definition.accountId,
      appId: definition.appId,
      endpoint: '127.0.0.1',
      keys: definition.responseKeys,
      namespaces: definition.namespaces,
      port: child.port,
      tls: false,
    });
  }

  function deleteApp(response, appId) {
    const child = children.get(appId);
    if (child) {
      children.delete(appId);
      stopChild(child);
    }
    response.writeHead(204).end();
  }

  async function forwardStats(request, response) {
    const body = await readBody(request);
    const appId = appIdFromRequest(request);
    const child = appId ? children.get(appId) : undefined;
    if (!child) {
      sendText(response, 404, 'unknown fixture app');
      return;
    }

    const upstream = await fetch(`http://127.0.0.1:${child.port}/stats`, {
      body,
      headers: {
        'content-type': request.headers['content-type'] ?? 'application/json',
        ...(request.headers.authorization ? { authorization: request.headers.authorization } : {}),
      },
      method: 'POST',
    });
    response.writeHead(upstream.status, {
      'content-type': upstream.headers.get('content-type') ?? 'application/json',
    });
    response.end(await upstream.text());
  }

  const server = createServer(async (request, response) => {
    try {
      const url = new URL(request.url, 'http://127.0.0.1');
      const path = url.pathname;
      if (request.method === 'GET' && path === '/') {
        sendText(response, 200, 'ready');
      } else if (request.method === 'GET' && path === '/is-the-internet-up.txt') {
        sendText(response, 200, 'yes');
      } else if (request.method === 'GET' && path === '/createJWT') {
        sendText(response, 200, createJwt(url));
      } else if (request.method === 'POST' && path === '/apps') {
        await createApp(request, response);
      } else if (request.method === 'DELETE' && path.startsWith('/apps/')) {
        deleteApp(response, decodeURIComponent(path.slice('/apps/'.length)));
      } else if (request.method === 'POST' && path === '/stats') {
        await forwardStats(request, response);
      } else {
        sendText(response, 404, 'not found');
      }
    } catch (error) {
      console.error(
        '[upstream-sandbox] request failed',
        error instanceof Error ? error.message : String(error),
      );
      if (!response.headersSent) sendText(response, 500, 'fixture provisioning failed');
      else response.end();
    }
  });

  return {
    close: () =>
      new Promise((resolveClose) => {
        for (const child of children.values()) stopChild(child);
        children.clear();
        server.close(resolveClose);
      }),
    listen: (port, host = '127.0.0.1') =>
      new Promise((resolveListen, reject) => {
        server.once('error', reject);
        server.listen(port, host, () => resolveListen(server.address()));
      }),
  };
}

function argument(name, fallback) {
  const index = process.argv.indexOf(name);
  return index === -1 ? fallback : process.argv[index + 1];
}

const invokedPath = process.argv[1] ? resolve(process.argv[1]) : '';
if (invokedPath === fileURLToPath(import.meta.url)) {
  const binary = argument('--sockudo-bin', process.env.SOCKUDO_BIN);
  const configPath = argument('--config', process.env.SOCKUDO_CONFIG_TEMPLATE ?? 'config/config.toml');
  const listen = argument('--listen', process.env.ABLY_LOCAL_SANDBOX_LISTEN ?? '127.0.0.1:9080');
  const logDirectory = argument(
    '--log-dir',
    process.env.SANDBOX_LOG_DIR ?? mkdtempSync(join(tmpdir(), 'sockudo-upstream-logs-')),
  );
  const wsPackageRoot = argument('--ws-package-root', process.env.SANDBOX_WS_PACKAGE_ROOT);
  if (!binary) throw new Error('--sockudo-bin is required');

  let WebSocketImplementation = globalThis.WebSocket;
  if (!WebSocketImplementation && wsPackageRoot) {
    const requireFromPackage = createRequire(join(resolve(wsPackageRoot), 'package.json'));
    WebSocketImplementation = requireFromPackage('ws');
  }

  const separator = listen.lastIndexOf(':');
  const host = listen.slice(0, separator) || '127.0.0.1';
  const port = Number(listen.slice(separator + 1));
  const sandbox = createSandbox({
    binary: resolve(binary),
    configTemplate: readFileSync(resolve(configPath), 'utf8'),
    logDirectory: resolve(logDirectory),
    WebSocketImplementation,
  });
  const address = await sandbox.listen(port, host);
  console.log(`[upstream-sandbox] listening on http://${address.address}:${address.port}`);

  let closing = false;
  async function shutdown() {
    if (closing) return;
    closing = true;
    await sandbox.close();
  }
  process.once('SIGINT', shutdown);
  process.once('SIGTERM', shutdown);
}
