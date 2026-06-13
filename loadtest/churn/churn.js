// k6 room-switch churn loader (Pusher protocol, public channels).
// Each VU holds one WebSocket connection and repeatedly unsubscribes its current
// room and subscribes a new random one — exactly the room-switch pattern that is
// swaglive's bottleneck. No publishing: publish throughput was already fine; the
// variable under test is subscribe/unsubscribe frequency.
import ws from 'k6/ws';
import { Counter } from 'k6/metrics';

const NODES = (__ENV.NODES || 'ws://localhost:6011,ws://localhost:6012,ws://localhost:6013').split(',');
const KEY = __ENV.APP_KEY || 'heavykey';
const ROOMS = parseInt(__ENV.ROOMS || '400');
const CHURN_MS = parseInt(__ENV.CHURN_MS || '1000');
const VUS = parseInt(__ENV.VUS || '200');
const DURATION = __ENV.DURATION || '60s';

const subsSent = new Counter('subs_sent');
const unsubsSent = new Counter('unsubs_sent');
const subAcks = new Counter('sub_acks');

export const options = {
  scenarios: {
    churn: { executor: 'constant-vus', vus: VUS, duration: DURATION, gracefulStop: '2s' },
  },
};

function room() { return 'room-' + Math.floor(Math.random() * ROOMS); }

export default function () {
  const node = NODES[(__VU - 1) % NODES.length];
  const url = node + '/app/' + KEY + '?protocol=7&client=k6&version=1.0.0';
  let current = null;

  ws.connect(url, {}, function (socket) {
    socket.on('open', function () {
      current = room();
      socket.send(JSON.stringify({ event: 'pusher:subscribe', data: { channel: current } }));
      subsSent.add(1);

      socket.setInterval(function () {
        if (current) {
          socket.send(JSON.stringify({ event: 'pusher:unsubscribe', data: { channel: current } }));
          unsubsSent.add(1);
        }
        current = room();
        socket.send(JSON.stringify({ event: 'pusher:subscribe', data: { channel: current } }));
        subsSent.add(1);
      }, CHURN_MS);
    });
    socket.on('message', function (msg) {
      if (msg.indexOf('subscription_succeeded') !== -1) subAcks.add(1);
    });
    socket.on('error', function () {});
  });
}
