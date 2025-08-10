const fetch = require('node-fetch').default;
const crypto = require('crypto');

class TestUtils {
    /**
     * Wait for a specific duration
     */
    static async wait(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    /**
     * Retry a function until it succeeds or timeout
     */
    static async retry(fn, options = {}) {
        const maxAttempts = options.maxAttempts || 10;
        const interval = options.interval || 500;
        const timeout = options.timeout || 5000;
        
        const startTime = Date.now();
        let lastError;
        
        for (let i = 0; i < maxAttempts; i++) {
            try {
                return await fn();
            } catch (error) {
                lastError = error;
                
                if (Date.now() - startTime > timeout) {
                    throw new Error(`Timeout after ${timeout}ms: ${lastError.message}`);
                }
                
                if (i < maxAttempts - 1) {
                    await this.wait(interval);
                }
            }
        }
        
        throw lastError;
    }

    /**
     * Generate a random channel name
     */
    static generateChannelName(prefix = 'test') {
        const random = Math.random().toString(36).substring(7);
        return `${prefix}-channel-${random}`;
    }

    /**
     * Generate auth signature for private/presence channels
     */
    static generateAuth(socketId, channelName, appKey, appSecret, userData = null) {
        let stringToSign = `${socketId}:${channelName}`;
        
        if (userData) {
            const userDataJson = JSON.stringify(userData);
            stringToSign = `${socketId}:${channelName}:${userDataJson}`;
        }
        
        const signature = crypto
            .createHmac('sha256', appSecret)
            .update(stringToSign)
            .digest('hex');
            
        return `${appKey}:${signature}`;
    }

    /**
     * Send HTTP API event to Sockudo
     */
    static async sendApiEvent(config) {
        const {
            host = 'localhost',
            port = 6005,
            appId = 'test-app',
            appKey = 'test-key',
            appSecret = 'test-secret',
            channels,
            event,
            data
        } = config;

        const url = `http://${host}:${port}/apps/${appId}/events`;
        
        const body = {
            name: event,
            channels: Array.isArray(channels) ? channels : [channels],
            data: typeof data === 'string' ? data : JSON.stringify(data)
        };

        // Generate auth signature for API request
        const timestamp = Math.floor(Date.now() / 1000);
        const bodyMd5 = crypto.createHash('md5').update(JSON.stringify(body)).digest('hex');
        
        const queryString = `auth_key=${appKey}&auth_timestamp=${timestamp}&auth_version=1.0&body_md5=${bodyMd5}`;
        const authString = `POST\n/apps/${appId}/events\n${queryString}`;

        const authSignature = crypto
            .createHmac('sha256', appSecret)
            .update(authString)
            .digest('hex');

        const urlWithAuth = `${url}?${queryString}&auth_signature=${authSignature}`;

        const response = await fetch(urlWithAuth, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(body)
        });

        if (!response.ok) {
            const error = await response.text();
            throw new Error(`API event failed: ${response.status} - ${error}`);
        }

        return response.json();
    }

    /**
     * Get channel info via HTTP API
     */
    static async getChannelInfo(config) {
        const {
            host = 'localhost',
            port = 6005,
            appId = 'test-app',
            appKey = 'test-key',
            appSecret = 'test-secret',
            channel,
            info = []
        } = config;

        const timestamp = Math.floor(Date.now() / 1000);
        const infoParam = info.length > 0 ? `info=${info.join(',')}` : '';
        const queryString = `auth_key=${appKey}&auth_timestamp=${timestamp}&auth_version=1.0${infoParam ? `&${infoParam}` : ''}`;
        const authString = `GET\n/apps/${appId}/channels/${channel}\n${queryString}`;

        const authSignature = crypto
            .createHmac('sha256', appSecret)
            .update(authString)
            .digest('hex');

        const url = `http://${host}:${port}/apps/${appId}/channels/${channel}?${queryString}&auth_signature=${authSignature}`;

        const response = await fetch(url);
        
        if (!response.ok) {
            const error = await response.text();
            throw new Error(`Failed to get channel info: ${response.status} - ${error}`);
        }

        return response.json();
    }

    /**
     * Get all channels via HTTP API
     */
    static async getChannels(config) {
        const {
            host = 'localhost',
            port = 6005,
            appId = 'test-app',
            appKey = 'test-key',
            appSecret = 'test-secret',
            filter = {},
            info = []
        } = config;

        const timestamp = Math.floor(Date.now() / 1000);
        const params = [`auth_key=${appKey}`, `auth_timestamp=${timestamp}`, `auth_version=1.0`];
        if (filter.prefix) params.push(`filter_by_prefix=${filter.prefix}`);
        if (info.length > 0) params.push(`info=${info.join(',')}`);
        
        const queryString = params.join('&');
        const authString = `GET\n/apps/${appId}/channels\n${queryString}`;

        const authSignature = crypto
            .createHmac('sha256', appSecret)
            .update(authString)
            .digest('hex');

        const url = `http://${host}:${port}/apps/${appId}/channels?${queryString}&auth_signature=${authSignature}`;

        const response = await fetch(url);
        
        if (!response.ok) {
            const error = await response.text();
            throw new Error(`Failed to get channels: ${response.status} - ${error}`);
        }

        return response.json();
    }

    /**
     * Create multiple test clients
     */
    static async createClients(count, config = {}) {
        const TestClient = require('./TestClient');
        const clients = [];
        
        for (let i = 0; i < count; i++) {
            const client = new TestClient(config);
            await client.connect();
            clients.push(client);
        }
        
        return clients;
    }

    /**
     * Cleanup multiple test clients
     */
    static async cleanupClients(clients) {
        await Promise.all(clients.map(client => client.disconnect()));
    }

    /**
     * Wait for Sockudo server to be ready
     */
    static async waitForServer(host = 'localhost', port = 6005, timeout = 30000) {
        const startTime = Date.now();
        
        while (Date.now() - startTime < timeout) {
            try {
                const response = await fetch(`http://${host}:${port}/up/test-app`);
                if (response.ok) {
                    return true;
                }
            } catch (error) {
                // Server not ready yet
            }
            
            await this.wait(1000);
        }
        
        throw new Error(`Server not ready after ${timeout}ms`);
    }

    /**
     * Generate large payload for testing size limits
     */
    static generateLargePayload(sizeInKb) {
        const sizeInBytes = sizeInKb * 1024;
        const chunk = 'x'.repeat(100);
        let payload = '';
        
        while (Buffer.byteLength(payload) < sizeInBytes) {
            payload += chunk;
        }
        
        return payload.substring(0, sizeInBytes);
    }

    /**
     * Assert event received within timeout
     */
    static async assertEventReceived(client, channelName, eventName, timeout = 5000) {
        return client.waitForEvent(channelName, eventName, timeout);
    }

    /**
     * Measure event latency
     */
    static async measureLatency(senderClient, receiverClient, channelName) {
        const eventName = 'client-latency-test';
        const timestamp = Date.now();
        
        // Set up receiver
        const receivePromise = receiverClient.waitForEvent(channelName, eventName);
        
        // Send event
        await senderClient.trigger(channelName, eventName, { timestamp });
        
        // Wait for reception
        const receivedData = await receivePromise;
        if (!receivedData || !receivedData.timestamp) {
            throw new Error('Event was not received or missing timestamp');
        }
        const latency = Date.now() - receivedData.timestamp;
        
        return latency;
    }

    /**
     * Clear rate limits by waiting for reset
     */
    static async clearRateLimits() {
        // Wait for rate limit windows to reset
        await this.wait(2000);
    }

    /**
     * Reset server state between tests
     */
    static async resetServerState() {
        // Clear rate limits
        await this.clearRateLimits();
        
        // Additional cleanup if needed
        await this.wait(500);
    }
}

module.exports = TestUtils;