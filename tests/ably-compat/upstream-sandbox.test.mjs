import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { createAppDefinition, createJwt, renderConfig } from './upstream-sandbox.mjs';

const template = readFileSync(new URL('../../config/config.toml', import.meta.url), 'utf8');

test('renders all fixture keys onto one isolated app', () => {
  const definition = createAppDefinition({
    keys: [
      {},
      { capability: { 'chat:*': ['publish'] }, revocableTokens: true },
    ],
    namespaces: [{ id: 'chat', mutableMessages: true }],
  });
  const rendered = renderConfig(template, 7123, definition);

  assert.match(rendered, /^port = 7123$/m);
  assert.match(rendered, new RegExp(`id = "${definition.appId}"`));
  assert.match(rendered, /name = "chat"/);
  assert.match(rendered, /annotations_enabled = true/);
  assert.equal((rendered.match(/^\[ably_compat\]$/gm) ?? []).length, 1);
  assert.equal((rendered.match(/\[\[ably_compat\.keys\]\]/g) ?? []).length, 2);
  assert.match(rendered, /capability = "\{\\"chat:\*\\":\[\\"publish\\"\]\}"/);
  assert.match(rendered, /revocable_tokens = true/);
  assert.match(rendered, /stats_fixture_ingest_enabled = true/);
  const push = rendered.split('[push]')[1].split('[push.retry]')[0];
  assert.match(push, /storage_driver = "memory"/);
  assert.match(push, /queue_driver = "memory"/);
  assert.match(push, /allow_memory_drivers = true/);
  for (const provider of ['fcm', 'apns', 'webpush', 'hms', 'wns']) {
    assert.match(push, new RegExp(`${provider}_enabled = false`));
  }
});

test('uses a full-capability response when a fixture omits capability', () => {
  const definition = createAppDefinition({ keys: [{}] });

  assert.equal(definition.responseKeys[0].capability, '{"*":["*"]}');
  assert.equal(definition.configKeys[0].capability, undefined);
});

test('creates a bounded local HS256 JWT for the Go authURL fixture', () => {
  const url = new URL('http://127.0.0.1/createJWT');
  url.searchParams.set('keyName', 'app.key');
  url.searchParams.set('keySecret', 'secret');
  url.searchParams.set('expiresIn', '30');
  url.searchParams.set('clientId', 'client');

  const [header, payload, signature] = createJwt(url).split('.');
  assert.deepEqual(JSON.parse(Buffer.from(header, 'base64url')), {
    typ: 'JWT',
    alg: 'HS256',
    kid: 'app.key',
  });
  const claims = JSON.parse(Buffer.from(payload, 'base64url'));
  assert.equal(claims.exp - claims.iat, 30);
  assert.equal(claims['x-ably-clientId'], 'client');
  assert.equal(typeof signature, 'string');
  assert.throws(
    () => createJwt(new URL('http://127.0.0.1/createJWT?keyName=app.key&keySecret=secret&expiresIn=0')),
    /invalid JWT expiry/,
  );
});
