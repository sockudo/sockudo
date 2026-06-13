import ws from 'k6/ws';
import { Counter, Rate, Trend } from 'k6/metrics';

/*
 * Subscription churn benchmark.
 *
 * Churn rate is approximately VUS / (ROOM_SWITCH_INTERVAL_MS / 1000).
 * The reported staging case, 10k users and 500 unsub+sub/sec, is:
 *
 *   k6 run \
 *     -e WS_HOSTS=wss://example.com/app/swag-dev-key \
 *     -e VUS=10000 -e CHANNEL_COUNT=4000 \
 *     -e ROOM_SWITCH_INTERVAL_MS=20000 \
 *     -e SOCKET_LIFETIME_MS=600000 -e DURATION=10m \
 *     benches/subscription-churn.js
 *
 * To ramp instead of holding a constant user count:
 *
 *   k6 run \
 *     -e STAGES='[{"duration":"10m","target":10000},{"duration":"20m","target":10000}]' \
 *     -e WS_HOSTS=wss://example.com/app/swag-dev-key \
 *     -e CHANNEL_COUNT=4000 -e ROOM_SWITCH_INTERVAL_MS=20000 \
 *     -e SOCKET_LIFETIME_MS=900000 \
 *     benches/subscription-churn.js
 */

const wsHosts = (__ENV.WS_HOSTS || __ENV.WS_HOST || 'ws://127.0.0.1:6001/app/app-key')
    .split(',')
    .map((host) => host.trim())
    .filter(Boolean);
const vus = Number(__ENV.VUS || 100);
const duration = __ENV.DURATION || '2m';
const stages = __ENV.STAGES ? JSON.parse(__ENV.STAGES) : null;
const channelCount = Number(__ENV.CHANNEL_COUNT || 4000);
const channelPrefix = __ENV.CHANNEL_PREFIX || 'load-room-';
const switchIntervalMs = Number(__ENV.ROOM_SWITCH_INTERVAL_MS || 20000);
const socketLifetimeMs = Number(__ENV.SOCKET_LIFETIME_MS || 120000);
const initialJitterMs = Number(__ENV.INITIAL_JITTER_MS || switchIntervalMs);
const pingIntervalMs = Number(__ENV.PING_INTERVAL_MS || 30000);

const subscribeAttempts = new Counter('sockudo_churn_subscribe_attempts');
const unsubscribeAttempts = new Counter('sockudo_churn_unsubscribe_attempts');
const subscriptionSuccessRate = new Rate('sockudo_churn_subscription_success_rate');
const socketConnectedRate = new Rate('sockudo_churn_socket_connected_rate');
const socketErrorRate = new Rate('sockudo_churn_socket_error_rate');
const subscribeAckMs = new Trend('sockudo_churn_subscribe_ack_ms');

export const options = {
    scenarios: {
        churn: stages
            ? {
                executor: 'ramping-vus',
                stages,
                gracefulRampDown: __ENV.GRACEFUL_RAMP_DOWN || '30s',
            }
            : {
                executor: 'constant-vus',
                vus,
                duration,
                gracefulStop: __ENV.GRACEFUL_STOP || '30s',
            },
    },
    thresholds: {
        sockudo_churn_socket_connected_rate: ['rate>0.99'],
        sockudo_churn_socket_error_rate: ['rate<0.01'],
        sockudo_churn_subscription_success_rate: ['rate>0.99'],
        sockudo_churn_subscribe_ack_ms: ['p(95)<1000'],
    },
};

function hostForVu() {
    return wsHosts[(__VU - 1) % wsHosts.length];
}

function channelFor(step) {
    const index = ((__VU - 1) * 997 + step) % channelCount;
    return `${channelPrefix}${index}`;
}

function safeJson(raw) {
    try {
        return JSON.parse(raw);
    } catch (_) {
        return null;
    }
}

function isSubscriptionSucceeded(message) {
    return typeof message.event === 'string' && message.event.endsWith(':subscription_succeeded');
}

export default function () {
    const pendingSubscriptions = {};
    let step = __ITER;
    let currentChannel = null;

    ws.connect(hostForVu(), null, (socket) => {
        socket.setTimeout(() => socket.close(), socketLifetimeMs);

        socket.on('open', () => {
            socketConnectedRate.add(true);
            socket.setInterval(() => {
                socket.send(JSON.stringify({
                    event: 'pusher:ping',
                    data: JSON.stringify({}),
                }));
            }, pingIntervalMs);
        });

        socket.on('message', (raw) => {
            const message = safeJson(raw);
            if (!message) {
                return;
            }

            if (message.event === 'pusher:connection_established') {
                const initialDelay = Math.max(1, Math.floor(Math.random() * initialJitterMs));
                socket.setTimeout(() => switchRoom(socket), initialDelay);
                socket.setInterval(() => switchRoom(socket), switchIntervalMs);
                return;
            }

            if (isSubscriptionSucceeded(message)) {
                currentChannel = message.channel || currentChannel;
                subscriptionSuccessRate.add(true);
                const startedAt = pendingSubscriptions[currentChannel];
                if (startedAt) {
                    subscribeAckMs.add(Date.now() - startedAt);
                    delete pendingSubscriptions[currentChannel];
                }
                return;
            }

            if (message.event === 'pusher:error' || message.event === 'sockudo:error') {
                subscriptionSuccessRate.add(false);
            }
        });

        socket.on('error', () => {
            socketErrorRate.add(true);
            subscriptionSuccessRate.add(false);
        });

        socket.on('close', () => {
            socketErrorRate.add(false);
        });

        function switchRoom(activeSocket) {
            if (currentChannel) {
                activeSocket.send(JSON.stringify({
                    event: 'pusher:unsubscribe',
                    data: { channel: currentChannel },
                }));
                unsubscribeAttempts.add(1);
            }

            step += 1;
            const nextChannel = channelFor(step);
            pendingSubscriptions[nextChannel] = Date.now();
            activeSocket.send(JSON.stringify({
                event: 'pusher:subscribe',
                data: { channel: nextChannel },
            }));
            subscribeAttempts.add(1);
            currentChannel = nextChannel;
        }
    });
}
