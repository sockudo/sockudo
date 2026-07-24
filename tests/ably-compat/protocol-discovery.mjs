import * as Ably from 'ably';
import assert from 'node:assert/strict';
import { readFile, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import process from 'node:process';
import { fileURLToPath } from 'node:url';

const endpoint = process.env.ABLY_ENDPOINT ?? '127.0.0.1';
const port = Number(process.env.ABLY_PORT ?? '6001');
const tls = process.env.ABLY_TLS === 'true';
const key = process.env.ABLY_KEY ?? 'app-key:app-secret';
const baseClientId = process.env.ABLY_CLIENT_ID ?? 'sockudo-ably-protocol';
const runId = process.env.ABLY_PROTOCOL_RUN_ID ?? String(Date.now());
const timeoutMs = Number(process.env.ABLY_PROTOCOL_TIMEOUT_MS ?? '15000');
const strict = process.env.ABLY_PROTOCOL_STRICT === '1';
const outputPath = process.env.ABLY_PROTOCOL_OUTPUT;
const browserOriginPorts = (process.env.ABLY_BROWSER_ORIGIN_PORTS ?? '4173,3001,5174')
  .split(',')
  .map((value) => Number(value.trim()))
  .filter(Number.isFinite);

const ablyPackage = JSON.parse(
  await readFile(new URL('./node_modules/ably/package.json', import.meta.url), 'utf8'),
);
const ablyBrowserBundlePath = fileURLToPath(
  new URL('./node_modules/ably/build/ably.min.js', import.meta.url),
);

function realtimeOptions({ clientId, binary }) {
  return {
    key,
    clientId,
    endpoint,
    port,
    tls,
    useBinaryProtocol: binary,
    transports: ['web_socket'],
    autoConnect: false,
    logLevel: 0,
  };
}

function restOptions({ clientId, binary }) {
  return {
    key,
    clientId,
    endpoint,
    port,
    tls,
    useBinaryProtocol: binary,
    logLevel: 0,
  };
}

function tokenRestOptions({ clientId, binary, tokenDetails }) {
  return {
    tokenDetails,
    clientId,
    endpoint,
    port,
    tls,
    useBinaryProtocol: binary,
    useTokenAuth: true,
    logLevel: 0,
  };
}

function withTimeout(promise, label, ms = timeoutMs) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
    }),
  ]).finally(() => clearTimeout(timer));
}

function onceConnection(client, state) {
  if (client.connection.state === state) {
    return Promise.resolve();
  }
  return new Promise((resolve, reject) => {
    const onState = (change) => {
      if (change.current === state) {
        client.connection.off(onState);
        resolve(change);
      } else if (change.current === 'failed' || change.current === 'closed') {
        client.connection.off(onState);
        reject(change.reason ?? new Error(`connection ${change.current}`));
      }
    };
    client.connection.on(onState);
  });
}

async function closeRealtime(client) {
  if (!client) {
    return;
  }
  client.close();
  try {
    await withTimeout(onceConnection(client, 'closed'), 'close', 2000);
  } catch {
    // Closing is best-effort for discovery. A failed close should not hide the
    // actual scenario result.
  }
}

function expectMessage(message, expectedName, expectedData) {
  assert.equal(message.name, expectedName);
  assert.deepEqual(message.data, expectedData);
}

async function realtimePubSub({ binary }) {
  const clientId = `${baseClientId}-${binary ? 'msgpack' : 'json'}-${runId}`;
  const client = new Ably.Realtime(realtimeOptions({ clientId, binary }));
  const channelName = `ably-protocol-${binary ? 'msgpack' : 'json'}-${runId}`;
  const historyData = { seed: true, binary, runId };
  const rest = new Ably.Rest(restOptions({ clientId, binary }));
  await withTimeout(
    rest.channels.get(channelName).publish('protocol-discovery', historyData),
    'seed history before attach',
  );
  try {
    client.connect();
    await withTimeout(onceConnection(client, 'connected'), 'connect');

    const channel = client.channels.get(channelName);
    await withTimeout(channel.attach(), 'attach');

    const received = withTimeout(
      new Promise((resolve) => {
        channel.subscribe('protocol-discovery', resolve);
      }),
      'subscribe delivery',
    );

    const data = { ok: true, binary, runId };
    await withTimeout(channel.publish('protocol-discovery', data), 'publish');
    expectMessage(await received, 'protocol-discovery', data);

    const history = await withTimeout(channel.history({ limit: 1, untilAttach: true }), 'history');
    assert.ok(history.items.length >= 1, 'expected at least one history item');
    expectMessage(history.items[0], 'protocol-discovery', historyData);

    await withTimeout(channel.detach(), 'detach');
    return { channel: channelName, clientId };
  } finally {
    await closeRealtime(client);
  }
}

async function restTime({ binary }) {
  const clientId = `${baseClientId}-rest-time-${binary ? 'msgpack' : 'json'}-${runId}`;
  const rest = new Ably.Rest(restOptions({ clientId, binary }));
  const now = await withTimeout(rest.time(), 'rest time');
  assert.equal(typeof now, 'number');
  assert.ok(now > 0, 'expected positive timestamp');
  return { timestamp: now, clientId };
}

async function restPublishHistory({ binary }) {
  const clientId = `${baseClientId}-rest-history-${binary ? 'msgpack' : 'json'}-${runId}`;
  const rest = new Ably.Rest(restOptions({ clientId, binary }));
  const channelName = `ably-rest-history-${binary ? 'msgpack' : 'json'}-${runId}`;
  const channel = rest.channels.get(channelName);
  const data = { via: 'rest', binary, runId };

  await withTimeout(channel.publish('rest-protocol-discovery', data), 'rest publish');
  const history = await withTimeout(channel.history({ limit: 1 }), 'rest history');
  assert.ok(history.items.length >= 1, 'expected at least one REST history item');
  expectMessage(history.items[0], 'rest-protocol-discovery', data);

  return { channel: channelName, clientId };
}

async function realtimePresence({ binary }) {
  const clientId = `${baseClientId}-presence-${binary ? 'msgpack' : 'json'}-${runId}`;
  const client = new Ably.Realtime(realtimeOptions({ clientId, binary }));
  const channelName = `ably-presence-${binary ? 'msgpack' : 'json'}-${runId}`;
  try {
    client.connect();
    await withTimeout(onceConnection(client, 'connected'), 'connect');

    const channel = client.channels.get(channelName);
    await withTimeout(channel.attach(), 'attach');
    await withTimeout(channel.presence.enter({ status: 'online', runId }), 'presence enter');

    const members = await withTimeout(channel.presence.get(), 'presence get');
    assert.ok(
      members.some((member) => member.clientId === clientId),
      `expected ${clientId} in presence set`,
    );

    await withTimeout(channel.presence.update({ status: 'busy', runId }), 'presence update');
    await withTimeout(channel.presence.leave({ status: 'offline', runId }), 'presence leave');
    await withTimeout(channel.detach(), 'detach');

    return { channel: channelName, clientId, members: members.length };
  } finally {
    await closeRealtime(client);
  }
}

async function authRequestToken() {
  const clientId = `${baseClientId}-token-${runId}`;
  const rest = new Ably.Rest(restOptions({ clientId, binary: false }));
  const token = await withTimeout(rest.auth.requestToken({ clientId }), 'request token');
  assert.equal(typeof token.token, 'string');
  assert.ok(token.token.length > 0, 'expected token string');
  return { clientId, tokenId: token.id ?? null };
}

async function authTokenCapabilityEnforcement() {
  const clientId = `${baseClientId}-token-caps-${runId}`;
  const issuer = new Ably.Rest(restOptions({ clientId, binary: false }));
  const allowedChannelName = `ably-token-allowed-${runId}`;
  const deniedChannelName = `ably-token-denied-${runId}`;
  const token = await withTimeout(
    issuer.auth.requestToken({
      clientId,
      capability: {
        [allowedChannelName]: ['publish', 'history'],
        [deniedChannelName]: ['history'],
      },
    }),
    'restricted token request',
  );
  assert.equal(typeof token.token, 'string');
  assert.equal(typeof token.capability, 'string');

  const rest = new Ably.Rest(tokenRestOptions({ clientId, binary: false, tokenDetails: token }));
  const data = { via: 'restricted-token', runId };
  const allowedChannel = rest.channels.get(allowedChannelName);
  await withTimeout(
    allowedChannel.publish('restricted-token-publish', data),
    'restricted token allowed publish',
  );
  const history = await withTimeout(
    allowedChannel.history({ limit: 1 }),
    'restricted token allowed history',
  );
  assert.ok(history.items.length >= 1, 'expected restricted token history item');
  expectMessage(history.items[0], 'restricted-token-publish', data);

  await assert.rejects(
    () =>
      withTimeout(
        rest.channels.get(deniedChannelName).publish('restricted-token-publish', data),
        'restricted token denied publish',
      ),
    /capability|40160|403|Forbidden/i,
  );

  return {
    clientId,
    allowedChannel: allowedChannelName,
    deniedChannel: deniedChannelName,
  };
}

async function startBrowserOriginServer() {
  if (browserOriginPorts.length === 0) {
    throw new Error('ABLY_BROWSER_ORIGIN_PORTS did not contain any usable ports');
  }

  const html = '<!doctype html><html><head><meta charset="utf-8"><title>Ably browser discovery</title></head><body></body></html>';
  const errors = [];

  for (const originPort of browserOriginPorts) {
    const server = createServer((_, response) => {
      response.writeHead(200, {
        'content-type': 'text/html; charset=utf-8',
        'cache-control': 'no-store',
      });
      response.end(html);
    });

    try {
      await new Promise((resolve, reject) => {
        const onError = (error) => {
          server.off('listening', onListening);
          reject(error);
        };
        const onListening = () => {
          server.off('error', onError);
          resolve();
        };

        server.once('error', onError);
        server.once('listening', onListening);
        server.listen(originPort, '127.0.0.1');
      });

      return {
        origin: `http://127.0.0.1:${originPort}/`,
        close: () =>
          new Promise((resolve, reject) => {
            server.close((error) => (error ? reject(error) : resolve()));
          }),
      };
    } catch (error) {
      errors.push(`${originPort}: ${error.message}`);
      if (error.code !== 'EADDRINUSE' && error.code !== 'EACCES') {
        throw error;
      }
    }
  }

  throw new Error(
    `unable to start browser origin on ports ${browserOriginPorts.join(', ')} (${errors.join('; ')})`,
  );
}

async function browserChromiumPubSub() {
  const { chromium } = await import('playwright');
  const originServer = await startBrowserOriginServer();
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  const browserErrors = [];

  page.on('pageerror', (error) => browserErrors.push(error.message));
  page.on('console', (message) => {
    if (message.type() === 'error') {
      browserErrors.push(message.text());
    }
  });

  try {
    await page.goto(originServer.origin, { waitUntil: 'domcontentloaded' });
    await page.addScriptTag({ path: ablyBrowserBundlePath });

    const clientId = `${baseClientId}-browser-json-${runId}`;
    const channelName = `ably-browser-json-${runId}`;
    const details = await withTimeout(
      page.evaluate(
        async ({ channelName, clientId, endpoint, key, port, runId, timeoutMs, tls }) => {
          const AblyBrowser = window.Ably;
          if (!AblyBrowser?.Realtime) {
            throw new Error('Ably browser bundle did not expose window.Ably.Realtime');
          }

          function withBrowserTimeout(promise, label, ms = timeoutMs) {
            let timer;
            return Promise.race([
              promise,
              new Promise((_, reject) => {
                timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
              }),
            ]).finally(() => clearTimeout(timer));
          }

          function onceBrowserConnection(client, state) {
            if (client.connection.state === state) {
              return Promise.resolve();
            }
            return new Promise((resolve, reject) => {
              const onState = (change) => {
                if (change.current === state) {
                  client.connection.off(onState);
                  resolve(change);
                } else if (change.current === 'failed' || change.current === 'closed') {
                  client.connection.off(onState);
                  const reason = change.reason?.message ?? `connection ${change.current}`;
                  reject(new Error(reason));
                }
              };
              client.connection.on(onState);
            });
          }

          function expectBrowserMessage(message, expectedName, expectedData) {
            if (message.name !== expectedName) {
              throw new Error(`expected message name ${expectedName}, got ${message.name}`);
            }
            const stable = (value) => {
              if (Array.isArray(value)) {
                return value.map(stable);
              }
              if (value && typeof value === 'object') {
                return Object.fromEntries(
                  Object.entries(value)
                    .sort(([left], [right]) => left.localeCompare(right))
                    .map(([key, nested]) => [key, stable(nested)]),
                );
              }
              return value;
            };
            if (JSON.stringify(stable(message.data)) !== JSON.stringify(stable(expectedData))) {
              throw new Error(
                `expected message data ${JSON.stringify(expectedData)}, got ${JSON.stringify(message.data)}`,
              );
            }
          }

          const historyData = { seed: true, via: 'browser', runId };
          const rest = new AblyBrowser.Rest({
            key,
            clientId,
            endpoint,
            port,
            tls,
            useBinaryProtocol: false,
            logLevel: 0,
          });
          await withBrowserTimeout(
            rest.channels.get(channelName).publish('browser-protocol-discovery', historyData),
            'browser seed history before attach',
          );

          const client = new AblyBrowser.Realtime({
            key,
            clientId,
            endpoint,
            port,
            tls,
            useBinaryProtocol: false,
            transports: ['web_socket'],
            autoConnect: false,
            logLevel: 0,
          });

          try {
            client.connect();
            await withBrowserTimeout(onceBrowserConnection(client, 'connected'), 'browser connect');

            const channel = client.channels.get(channelName);
            await withBrowserTimeout(channel.attach(), 'browser attach');

            let resolveMessage;
            const received = new Promise((resolve) => {
              resolveMessage = resolve;
            });
            await withBrowserTimeout(
              Promise.resolve(channel.subscribe('browser-protocol-discovery', resolveMessage)),
              'browser subscribe',
            );

            const data = { via: 'browser', runId };
            await withBrowserTimeout(
              channel.publish('browser-protocol-discovery', data),
              'browser publish',
            );

            const message = await withBrowserTimeout(received, 'browser delivery');
            expectBrowserMessage(message, 'browser-protocol-discovery', data);

            const history = await withBrowserTimeout(
              channel.history({ limit: 1, untilAttach: true }),
              'browser history',
            );
            if (!history.items.length) {
              throw new Error('expected at least one browser history item');
            }
            expectBrowserMessage(history.items[0], 'browser-protocol-discovery', historyData);

            await withBrowserTimeout(channel.detach(), 'browser detach');
            return {
              channel: channelName,
              clientId,
              userAgent: navigator.userAgent,
            };
          } finally {
            client.close();
            try {
              await withBrowserTimeout(onceBrowserConnection(client, 'closed'), 'browser close', 2000);
            } catch {
              // Closing is best-effort; the browser process is torn down after the lane.
            }
          }
        },
        { channelName, clientId, endpoint, key, port, runId, timeoutMs, tls },
      ),
      'browser chromium pubsub',
    );

    if (browserErrors.length > 0) {
      details.browserErrors = browserErrors.slice(0, 10);
    }
    return { ...details, origin: originServer.origin };
  } finally {
    await browser.close();
    await originServer.close();
  }
}

function errorSummary(error) {
  const reason = error?.reason ?? error;
  return {
    name: reason?.name ?? error?.name ?? 'Error',
    message: String(reason?.message ?? error?.message ?? error).slice(0, 500),
    code: reason?.code ?? error?.code,
    statusCode: reason?.statusCode ?? error?.statusCode,
  };
}

const scenarios = [
  {
    id: 'node-realtime-json-pubsub',
    required: true,
    protocol: 'json',
    runtime: 'node',
    area: 'realtime',
    claim: 'Ably Pub/Sub subset for AI Transport',
    run: () => realtimePubSub({ binary: false }),
  },
  {
    id: 'node-rest-json-time',
    required: false,
    protocol: 'json',
    runtime: 'node',
    area: 'rest',
    claim: 'broader Ably protocol discovery',
    run: () => restTime({ binary: false }),
  },
  {
    id: 'node-rest-json-publish-history',
    required: false,
    protocol: 'json',
    runtime: 'node',
    area: 'rest',
    claim: 'broader Ably protocol discovery',
    run: () => restPublishHistory({ binary: false }),
  },
  {
    id: 'node-rest-msgpack-publish-history',
    required: false,
    protocol: 'msgpack',
    runtime: 'node',
    area: 'rest',
    claim: 'broader Ably protocol discovery',
    run: () => restPublishHistory({ binary: true }),
  },
  {
    id: 'node-realtime-json-presence',
    required: false,
    protocol: 'json',
    runtime: 'node',
    area: 'presence',
    claim: 'broader Ably protocol discovery',
    run: () => realtimePresence({ binary: false }),
  },
  {
    id: 'node-auth-json-request-token',
    required: false,
    protocol: 'json',
    runtime: 'node',
    area: 'auth',
    claim: 'broader Ably protocol discovery',
    run: authRequestToken,
  },
  {
    id: 'node-auth-json-token-capability-enforcement',
    required: false,
    protocol: 'json',
    runtime: 'node',
    area: 'auth',
    claim: 'broader Ably protocol discovery',
    run: authTokenCapabilityEnforcement,
  },
  {
    id: 'node-realtime-msgpack-pubsub',
    required: false,
    protocol: 'msgpack',
    runtime: 'node',
    area: 'realtime',
    claim: 'broader Ably protocol discovery',
    run: () => realtimePubSub({ binary: true }),
  },
  {
    id: 'node-rest-msgpack-time',
    required: false,
    protocol: 'msgpack',
    runtime: 'node',
    area: 'rest',
    claim: 'broader Ably protocol discovery',
    run: () => restTime({ binary: true }),
  },
  {
    id: 'browser-chromium-json-pubsub',
    required: false,
    protocol: 'json',
    runtime: 'chromium',
    area: 'realtime',
    claim: 'broader Ably protocol discovery',
    run: browserChromiumPubSub,
  },
];

const results = [];

for (const scenario of scenarios) {
  const startedAt = Date.now();
  if (scenario.skip) {
    results.push({
      id: scenario.id,
      status: 'not-run',
      classification: 'not-run',
      required: scenario.required,
      protocol: scenario.protocol,
      runtime: scenario.runtime,
      area: scenario.area,
      claim: scenario.claim,
      note: scenario.skip,
      durationMs: 0,
    });
    continue;
  }

  try {
    const details = await withTimeout(scenario.run(), scenario.id);
    results.push({
      id: scenario.id,
      status: 'passed',
      classification: 'supported',
      required: scenario.required,
      protocol: scenario.protocol,
      runtime: scenario.runtime,
      area: scenario.area,
      claim: scenario.claim,
      durationMs: Date.now() - startedAt,
      details,
    });
  } catch (error) {
    results.push({
      id: scenario.id,
      status: 'failed',
      classification: scenario.required ? 'regression' : 'not-yet-implemented',
      required: scenario.required,
      protocol: scenario.protocol,
      runtime: scenario.runtime,
      area: scenario.area,
      claim: scenario.claim,
      durationMs: Date.now() - startedAt,
      error: errorSummary(error),
    });
  }
}

const summary = {
  schema: 'sockudo.ably-protocol-discovery.v1',
  generatedAt: new Date().toISOString(),
  sdk: {
    name: 'ably',
    version: ablyPackage.version,
  },
  target: {
    endpoint,
    port,
    tls,
    keyName: key.split(':')[0] ?? null,
  },
  strict,
  counts: results.reduce(
    (counts, result) => {
      counts[result.status] = (counts[result.status] ?? 0) + 1;
      return counts;
    },
    {},
  ),
  requiredFailures: results
    .filter((result) => result.required && result.status !== 'passed')
    .map((result) => result.id),
  strictFailures: strict
    ? results
        .filter((result) => result.status !== 'passed')
        .map((result) => result.id)
    : [],
  results,
};

if (outputPath) {
  await writeFile(outputPath, `${JSON.stringify(summary, null, 2)}\n`);
}

for (const result of results) {
  const marker = result.status.padEnd(7);
  const requiredMarker = result.required ? 'required' : 'optional';
  const note = result.error?.message ?? result.note ?? '';
  console.log(`${marker} ${requiredMarker.padEnd(8)} ${result.id} ${note}`);
}

console.log(JSON.stringify(summary, null, 2));

if (summary.requiredFailures.length > 0) {
  process.exitCode = 1;
} else if (strict && summary.strictFailures.length > 0) {
  process.exitCode = 1;
}
