import * as Ably from 'ably';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';

const endpoint = process.env.ABLY_ENDPOINT ?? '127.0.0.1';
const port = Number(process.env.ABLY_PORT ?? '6001');
const tls = process.env.ABLY_TLS === 'true';
const testAppFile = process.env.ABLY_TEST_APP_FILE;
assert.ok(testAppFile, 'ABLY_TEST_APP_FILE must point at the generated six-key fixture');
const testApp = JSON.parse(fs.readFileSync(testAppFile, 'utf8'));
assert.equal(testApp.keys.length, 6);

function options(key, clientId) {
  return {
    key: key.keyStr,
    ...(clientId == null ? {} : { clientId }),
    endpoint,
    port,
    tls,
    useBinaryProtocol: false,
    autoConnect: false,
  };
}

function withTimeout(promise, label, ms = 10_000) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
    }),
  ]).finally(() => clearTimeout(timer));
}

function connected(client) {
  if (client.connection.state === 'connected') return Promise.resolve();
  return new Promise((resolve, reject) => {
    client.connection.on((change) => {
      if (change.current === 'connected') resolve();
      if (change.current === 'failed' || change.current === 'closed') {
        reject(change.reason ?? new Error(`connection ${change.current}`));
      }
    });
  });
}

async function expectAblyError(responsePromise, status, code) {
  const response = await responsePromise;
  assert.equal(response.status, status);
  const body = await response.json();
  assert.equal(body.error.statusCode, status);
  assert.equal(body.error.code, code);
  assert.equal('token' in body, false);
}

function signedTokenRequest(key, overrides = {}) {
  const request = {
    keyName: key.keyName,
    timestamp: Date.now(),
    nonce: crypto.randomBytes(16).toString('hex'),
    ...overrides,
  };
  const signingInput = `${request.keyName}\n${request.ttl ?? ''}\n${request.capability ?? ''}\n${request.clientId ?? ''}\n${request.timestamp}\n${request.nonce}\n`;
  request.mac = crypto.createHmac('sha256', key.keySecret).update(signingInput).digest('base64');
  return request;
}

function tokenRequest(key, body) {
  const scheme = tls ? 'https' : 'http';
  return fetch(`${scheme}://${endpoint}:${port}/keys/${encodeURIComponent(key.keyName)}/requestToken`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', accept: 'application/json' },
    body: JSON.stringify(body),
  });
}

const publisher = new Ably.Realtime(options(testApp.keys[2], 'multi-key-publisher'));
const observer = new Ably.Realtime(options(testApp.keys[0], 'multi-key-observer'));
const channelName = 'channel4';

try {
  publisher.connect();
  observer.connect();
  await Promise.all([
    withTimeout(connected(publisher), 'publisher connect'),
    withTimeout(connected(observer), 'observer connect'),
  ]);

  const publishChannel = publisher.channels.get(channelName);
  const observeChannel = observer.channels.get(channelName);
  await Promise.all([
    withTimeout(publishChannel.attach(), 'publisher attach'),
    withTimeout(observeChannel.attach(), 'observer attach'),
  ]);

  const delivery = withTimeout(
    new Promise((resolve) => observeChannel.subscribe('cross-key', resolve)),
    'cross-key delivery',
  );
  await withTimeout(publishChannel.publish('cross-key', { shared: true }), 'cross-key publish');
  assert.deepEqual((await delivery).data, { shared: true });

  const history = await withTimeout(observeChannel.history({ limit: 10 }), 'cross-key history');
  assert.ok(history.items.some((message) => message.name === 'cross-key'));

  const presenceDelivery = withTimeout(
    new Promise((resolve) => observeChannel.presence.subscribe('enter', resolve)),
    'cross-key presence delivery',
  );
  await withTimeout(publishChannel.presence.enter({ shared: true }), 'cross-key presence enter');
  const member = await presenceDelivery;
  assert.equal(member.clientId, 'multi-key-publisher');
  assert.deepEqual(member.data, { shared: true });

  const subscribeOnly = new Ably.Rest(options(testApp.keys[3], undefined));
  await assert.rejects(
    subscribeOnly.channels.get(channelName).publish('forbidden', 'no'),
    (error) => error.statusCode === 403 && error.code === 40160,
  );

  const scheme = tls ? 'https' : 'http';
  await expectAblyError(
    fetch(`${scheme}://${endpoint}:${port}/channels/${channelName}/messages?key=${encodeURIComponent(testApp.keys[0].keyName)}`),
    401,
    40101,
  );
  await expectAblyError(
    fetch(`${scheme}://${endpoint}:${port}/channels/${channelName}/messages?key=${encodeURIComponent(`${testApp.keys[0].keyName}:wrong`)}`),
    401,
    40101,
  );

  const badMac = signedTokenRequest(testApp.keys[0]);
  badMac.mac = 'bogus';
  await expectAblyError(tokenRequest(testApp.keys[0], badMac), 401, 40101);
  await expectAblyError(
    tokenRequest(
      testApp.keys[0],
      signedTokenRequest(testApp.keys[0], { timestamp: Date.now() - 30 * 60 * 1000 }),
    ),
    401,
    40104,
  );
  await expectAblyError(
    tokenRequest(testApp.keys[0], signedTokenRequest(testApp.keys[0], { ttl: -1 })),
    400,
    40000,
  );
  await expectAblyError(
    tokenRequest(testApp.keys[0], signedTokenRequest(testApp.keys[0], { ttl: 'not-a-number' })),
    400,
    40000,
  );
  const replayed = signedTokenRequest(testApp.keys[0], { nonce: 'multi-key-replayed-nonce' });
  const issued = await tokenRequest(testApp.keys[0], replayed);
  assert.equal(issued.status, 200);
  assert.ok((await issued.json()).token);
  await expectAblyError(tokenRequest(testApp.keys[0], replayed), 401, 40105);

  console.log(JSON.stringify({ ok: true, channel: channelName }));
} finally {
  publisher.close();
  observer.close();
}
