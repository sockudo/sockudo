// Bulk subscribers: many WS connections from ONE machine, all subscribed to a
// single hot channel, holding + reading. Each records its OWN view of
// publish->receive latency (from the timestamp the publisher embeds). This is the
// "saturated load-gen client" measurement that may be inflated by the host, not
// the server.
import ws from 'k6/ws';
import { Trend, Counter } from 'k6/metrics';

const NODES = (__ENV.NODES || 'ws://localhost:6011,ws://localhost:6012,ws://localhost:6013').split(',');
const KEY = __ENV.APP_KEY || 'lightkey';
const CHANNEL = __ENV.CHANNEL || 'hot-room';
const EVENT = __ENV.EVENT || 'msg';
const VUS = parseInt(__ENV.VUS || '2500');
const DURATION = __ENV.DURATION || '70s';

const bulkLat = new Trend('bulk_latency', true);
const bulkRecv = new Counter('bulk_recv');

export const options = {
  scenarios: { bulk: { executor: 'constant-vus', vus: VUS, duration: DURATION, gracefulStop: '3s' } },
};

export default function () {
  const node = NODES[(__VU - 1) % NODES.length];
  const url = node + '/app/' + KEY + '?protocol=7&client=bulk&version=1.0.0';
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
        if (d && d.t) { bulkLat.add(Date.now() - d.t); bulkRecv.add(1); }
      } catch (e) { /* ignore */ }
    });
    socket.on('error', function () {});
  });
}
