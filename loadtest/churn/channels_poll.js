// Polls GET /apps/{id}/channels with signed Pusher auth, measuring response time.
// Used to A/B the cross-node fan-out rate before vs after the /channels cache.
import http from 'k6/http';
import crypto from 'k6/crypto';
import { sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

const HOST = __ENV.HOST || 'localhost:6011';
const APP_ID = __ENV.APP_ID || 'light';
const KEY = __ENV.APP_KEY || 'lightkey';
const SECRET = __ENV.APP_SECRET || 'lightsecret';
const INFO = __ENV.INFO || 'subscription_count';
const POLL_MS = parseInt(__ENV.POLL_MS || '100');
const DURATION = __ENV.DURATION || '20s';

const chLat = new Trend('channels_latency', true);
const chOk = new Counter('channels_ok');
const chFail = new Counter('channels_fail');

export const options = {
  scenarios: { poll: { executor: 'constant-vus', vus: 1, duration: DURATION } },
};

export default function () {
  const ts = Math.floor(Date.now() / 1000).toString();
  // Pusher auth: params sorted alphabetically (auth_key, auth_timestamp, auth_version, info)
  const qs = 'auth_key=' + KEY + '&auth_timestamp=' + ts + '&auth_version=1.0&info=' + INFO;
  const toSign = 'GET\n/apps/' + APP_ID + '/channels\n' + qs;
  const sig = crypto.hmac('sha256', SECRET, toSign, 'hex');
  const url = 'http://' + HOST + '/apps/' + APP_ID + '/channels?' + qs + '&auth_signature=' + sig;
  const res = http.get(url);
  chLat.add(res.timings.duration);
  if (res.status === 200) chOk.add(1);
  else chFail.add(1);
  sleep(POLL_MS / 1000);
}
