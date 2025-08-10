const Pusher = require('pusher-js');
const EventEmitter = require('events');

class TestClient extends EventEmitter {
    constructor(config = {}) {
        super();
        this.config = {
            key: config.key || process.env.TEST_APP_KEY || 'test-key',
            cluster: 'mt1',
            wsHost: config.host || process.env.SOCKUDO_HOST || 'localhost',
            wsPort: parseInt(config.port || process.env.SOCKUDO_PORT || 6005),
            forceTLS: config.ssl === 'true' || false,
            enabledTransports: ['ws', 'wss'],
            authEndpoint: config.authEndpoint || `http://${process.env.AUTH_SERVER_HOST || 'localhost'}:${process.env.AUTH_SERVER_PORT || 3005}/pusher/auth`,
            auth: config.auth || {},
            disableStats: true
        };
        
        this.pusher = null;
        this.channels = new Map();
        this.connectionState = 'disconnected';
        this.socketId = null;
    }

    async connect() {
        return new Promise((resolve, reject) => {
            this.pusher = new Pusher(this.config.key, this.config);
            
            const timeout = setTimeout(() => {
                reject(new Error('Connection timeout'));
            }, 10000);
            
            this.pusher.connection.bind('connected', () => {
                clearTimeout(timeout);
                this.connectionState = 'connected';
                this.socketId = this.pusher.connection.socket_id;
                this.emit('connected', this.socketId);
                resolve(this.socketId);
            });
            
            this.pusher.connection.bind('error', (error) => {
                clearTimeout(timeout);
                this.connectionState = 'error';
                // Don't emit error event to avoid unhandled error
                reject(new Error(`Connection failed: ${error.error?.data?.code || 'Unknown error'}`));
            });

            // Handle general websocket errors without crashing
            this.pusher.connection.bind('disconnected', () => {
                this.connectionState = 'disconnected';
                this.emit('disconnected');
            });
            
            this.pusher.connection.bind('state_change', (states) => {
                this.connectionState = states.current;
                this.emit('state_change', states);
            });
        });
    }

    async subscribe(channelName, eventHandlers = {}) {
        if (!this.pusher) {
            throw new Error('Client not connected');
        }

        return new Promise((resolve, reject) => {
            const channel = this.pusher.subscribe(channelName);
            this.channels.set(channelName, channel);

            const timeout = setTimeout(() => {
                reject(new Error(`Subscription timeout for channel: ${channelName}`));
            }, 5000);

            channel.bind('pusher:subscription_succeeded', (data) => {
                clearTimeout(timeout);
                
                // Bind event handlers
                Object.entries(eventHandlers).forEach(([event, handler]) => {
                    channel.bind(event, handler);
                });
                
                resolve({ channel, data });
            });

            channel.bind('pusher:subscription_error', (error) => {
                clearTimeout(timeout);
                this.channels.delete(channelName);
                const errorMsg = error.message || error.error || error;
                reject(new Error(`Subscription error for ${channelName}: ${errorMsg}`));
            });
        });
    }

    async unsubscribe(channelName) {
        if (!this.pusher) {
            throw new Error('Client not connected');
        }

        const channel = this.channels.get(channelName);
        if (channel) {
            this.pusher.unsubscribe(channelName);
            this.channels.delete(channelName);
        }
    }

    async trigger(channelName, eventName, data) {
        const channel = this.channels.get(channelName);
        if (!channel) {
            throw new Error(`Not subscribed to channel: ${channelName}`);
        }

        // Only private and presence channels support client events
        if (!channelName.startsWith('private-') && !channelName.startsWith('presence-')) {
            throw new Error('Client events only supported on private/presence channels');
        }

        // Ensure event name starts with 'client-'
        const clientEventName = eventName.startsWith('client-') ? eventName : `client-${eventName}`;
        
        console.log(`Triggering client event: ${clientEventName} on channel: ${channelName} with data:`, data);
        const result = channel.trigger(clientEventName, data);
        console.log(`Trigger result:`, result);
        return result;
    }

    async disconnect() {
        if (this.pusher) {
            this.pusher.disconnect();
            this.pusher = null;
            this.channels.clear();
            this.connectionState = 'disconnected';
            this.socketId = null;
            this.emit('disconnected');
        }
    }

    getChannel(channelName) {
        return this.channels.get(channelName);
    }

    isConnected() {
        return this.connectionState === 'connected';
    }

    getSocketId() {
        return this.socketId;
    }

    getConnectionState() {
        return this.connectionState;
    }

    // Wait for an event with timeout
    waitForEvent(channelName, eventName, timeout = 5000) {
        return new Promise((resolve, reject) => {
            const channel = this.channels.get(channelName);
            if (!channel) {
                reject(new Error(`Not subscribed to channel: ${channelName}`));
                return;
            }

            const timer = setTimeout(() => {
                channel.unbind(eventName, handler);
                reject(new Error(`Timeout waiting for event: ${eventName}`));
            }, timeout);

            const handler = (data) => {
                console.log(`Event ${eventName} received on channel ${channelName} with data:`, data);
                clearTimeout(timer);
                channel.unbind(eventName, handler);
                resolve(data);
            };

            channel.bind(eventName, handler);
        });
    }

    // Get presence channel members
    getPresenceMembers(channelName) {
        const channel = this.channels.get(channelName);
        if (!channel || !channelName.startsWith('presence-')) {
            return null;
        }
        return {
            count: channel.members.count,
            members: channel.members.members,
            me: channel.members.me
        };
    }
}

module.exports = TestClient;