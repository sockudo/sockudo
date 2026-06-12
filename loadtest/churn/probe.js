// Independent publisher + probe, run as its OWN k6 process (separately scheduled
// from the bulk load). The publisher POSTs signed Pusher events to a hot channel
// with an embedded send timestamp; the probe holds ONE subscriber connection on a
// DIFFERENT node and records true publish->receive latency. Because the probe is
// tiny and isolated, its latency reflects the SERVER's delivery time, not
// load-gen-client saturation.
import ws from 'k6/ws';
import http from 'k6/http';
import crypto from 'k6/crypto';
import { sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

const PUB_HOST = __ENV.PUB_HOST || 'localhost:6011';     // publish target node (HTTP)
const PROBE_WS = __ENV.PROBE_WS || 'ws://localhost:6012'; // probe subscribes on a different node
const APP_ID = __ENV.APP_ID || 'light';
const KEY = __ENV.APP_KEY || 'lightkey';
const SECRET = __ENV.APP_SECRET || 'lightsecret';
const CHANNEL = __ENV.CHANNEL || 'hot-room';
const EVENT = __ENV.EVENT || 'msg';
const PUB_INTERVAL_MS = parseInt(__ENV.PUB_INTERVAL_MS || '100');
const DURATION = __ENV.DURATION || '40s';
const INFO = __ENV.INFO || ''; // e.g. "subscription_count" -> forces per-publish cross-node count

const probeLat = new Trend('probe_latency', true);
const probeRecv = new Counter('probe_recv');
const pubOk = new Counter('pub_ok');
const pubFail = new Counter('pub_fail');
const reportedCount = new Trend('reported_count'); // subscription_count returned by the API

export const options = {
  scenarios: {
    publisher: { executor: 'constant-vus', vus: 1, duration: DURATION, exec: 'publisher', gracefulStop: '2s' },
    probe: { executor: 'constant-vus', vus: 1, duration: DURATION, exec: 'probe', gracefulStop: '2s' },
  },
};

// Pusher REST auth: HMAC-SHA256 over METHOD\nPATH\nsorted-query (incl. body_md5).
export function publisher() {
  const data = JSON.stringify({ t: Date.now() });
  const payload = { name: EVENT, channels: [CHANNEL], data: data };
  if (INFO) payload.info = INFO;
  const body = JSON.stringify(payload);
  const bodyMd5 = crypto.md5(body, 'hex');
  const ts = Math.floor(Date.now() / 1000).toString();
  const qs = 'auth_key=' + KEY + '&auth_timestamp=' + ts + '&auth_version=1.0&body_md5=' + bodyMd5;
  const toSign = 'POST\n/apps/' + APP_ID + '/events\n' + qs;
  const sig = crypto.hmac('sha256', SECRET, toSign, 'hex');
  const url = 'http://' + PUB_HOST + '/apps/' + APP_ID + '/events?' + qs + '&auth_signature=' + sig;
  const res = http.post(url, body, { headers: { 'Content-Type': 'application/json' } });
  if (res.status === 200) {
    pubOk.add(1);
    if (INFO) {
      try {
        const c = JSON.parse(res.body).channels[CHANNEL].subscription_count;
        if (typeof c === 'number') reportedCount.add(c);
      } catch (e) { /* ignore */ }
    }
  } else {
    pubFail.add(1);
  }
  sleep(PUB_INTERVAL_MS / 1000);
}

export function probe() {
  const url = PROBE_WS + '/app/' + KEY + '?protocol=7&client=probe&version=1.0.0';
  ws.connect(url, {}, function (socket) {
    socket.on('open', function () {
      socket.send(JSON.stringify({ event: 'pusher:subscribe', data: { channel: CHANNEL } }));
    });
    socket.on('message', function (m) {
      if (m.indexOf('"event":"' + EVENT + '"') === -1) return;
      try {
        const msg = JSON.parse(m);
        if (msg.event !== EVENT || !msg.data) return;
        const d = typeof msg.data === 'string' ? JSON.parse(msg.data) : msg.data;
        if (d && d.t) { probeLat.add(Date.now() - d.t); probeRecv.add(1); }
      } catch (e) { /* ignore */ }
    });
    socket.on('error', function () {});
  });
}
