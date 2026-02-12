/**
 * Delta Compression Client Example for Sockudo
 *
 * This example demonstrates how to:
 * 1. Enable delta compression on the client side
 * 2. Handle cache synchronization on subscription
 * 3. Reconstruct full messages from deltas
 * 4. Manage conflation key caches
 *
 * Usage:
 *   node examples/client_delta_compression.js
 *
 * Requirements:
 *   npm install pusher-js
 */

const Pusher = require('pusher-js');

// Delta reconstruction algorithms
const DeltaAlgorithms = {
    FOSSIL: 'fossil',
    XDELTA3: 'xdelta3'
};

/**
 * Delta Compression Manager
 * Handles cache synchronization and delta reconstruction
 */
class DeltaCompressionManager {
    constructor() {
        // Per-channel cache: channel -> { conflationKey, maxMessages, states }
        this.channelCaches = new Map();

        // Statistics
        this.stats = {
            fullMessages: 0,
            deltaMessages: 0,
            bytesReceived: 0,
            bytesReconstructed: 0,
            errors: 0
        };
    }

    /**
     * Handle cache sync event from server
     * This is sent when subscribing to a channel with conflation enabled
     */
    handleCacheSync(channel, data) {
        console.log(`📦 Cache sync received for channel: ${channel}`);

        const { conflation_key, max_messages_per_key, states } = data;

        // Initialize cache for this channel
        const cache = {
            conflationKey: conflation_key,
            maxMessagesPerKey: max_messages_per_key,
            states: new Map()
        };

        // Load initial states
        for (const [key, messages] of Object.entries(states)) {
            const messageQueue = messages.map(msg => ({
                content: JSON.parse(msg.content),
                sequence: msg.seq
            }));
            cache.states.set(key, messageQueue);

            console.log(`  - Loaded ${messageQueue.length} cached messages for key: ${key}`);
        }

        this.channelCaches.set(channel, cache);
        console.log(`✅ Cache initialized with ${cache.states.size} conflation groups`);
    }

    /**
     * Process a message (full or delta)
     */
    processMessage(channel, event, data) {
        // Check if this is a delta-compressed message
        if (data.__delta) {
            return this.reconstructDelta(channel, data);
        } else if (data.__delta_seq !== undefined) {
            // Full message with sequence tracking
            this.updateCache(channel, data);
            return data;
        } else {
            // Regular message (delta compression not enabled or not applicable)
            return data;
        }
    }

    /**
     * Reconstruct a full message from a delta
     */
    reconstructDelta(channel, deltaData) {
        this.stats.deltaMessages++;

        const {
            __delta: deltaBase64,
            __delta_seq: sequence,
            __delta_algorithm: algorithm,
            __conflation_key: conflationKey,
            __base_index: baseIndex,
            ...metadata
        } = deltaData;

        try {
            // Get the base message from cache
            const baseMessage = this.getBaseMessage(channel, conflationKey, baseIndex);
            if (!baseMessage) {
                console.error(`❌ Base message not found for channel ${channel}, key ${conflationKey}, index ${baseIndex}`);
                this.stats.errors++;
                return null;
            }

            // Decode delta from base64
            const deltaBytes = Buffer.from(deltaBase64, 'base64');

            // Apply delta to reconstruct full message
            const reconstructed = this.applyDelta(
                JSON.stringify(baseMessage.content),
                deltaBytes,
                algorithm
            );

            const fullMessage = JSON.parse(reconstructed);

            // Update cache with reconstructed message
            this.updateCache(channel, { ...fullMessage, __delta_seq: sequence, __conflation_key: conflationKey });

            // Track statistics
            this.stats.bytesReceived += deltaBase64.length;
            this.stats.bytesReconstructed += reconstructed.length;

            return fullMessage;
        } catch (error) {
            console.error(`❌ Failed to reconstruct delta:`, error);
            this.stats.errors++;
            return null;
        }
    }

    /**
     * Get base message from cache
     */
    getBaseMessage(channel, conflationKey, baseIndex) {
        const cache = this.channelCaches.get(channel);
        if (!cache) return null;

        const stateKey = conflationKey || '';
        const messages = cache.states.get(stateKey);
        if (!messages || baseIndex >= messages.length) return null;

        return messages[baseIndex];
    }

    /**
     * Update cache with a new full message
     */
    updateCache(channel, data) {
        const cache = this.channelCaches.get(channel);
        if (!cache) return;

        const conflationKey = data.__conflation_key || '';
        const sequence = data.__delta_seq;

        // Remove delta metadata from cached content
        const { __delta_seq, __delta_full, __conflation_key, ...content } = data;

        // Get or create message queue for this conflation key
        let messages = cache.states.get(conflationKey);
        if (!messages) {
            messages = [];
            cache.states.set(conflationKey, messages);
        }

        // Add message to cache
        messages.push({ content, sequence });

        // Enforce max messages per key (FIFO eviction)
        while (messages.length > cache.maxMessagesPerKey) {
            messages.shift();
        }

        this.stats.fullMessages++;
    }

    /**
     * Apply delta to base using the specified algorithm
     * Note: This is a placeholder - actual implementation would use native modules
     */
    applyDelta(baseString, deltaBytes, algorithm) {
        // In a real implementation, you would use:
        // - fossil-delta npm package for Fossil algorithm
        // - xdelta3 npm package for Xdelta3 algorithm

        console.warn(`⚠️  Delta reconstruction not fully implemented - would use ${algorithm} algorithm`);
        console.warn(`    Base size: ${baseString.length} bytes, Delta size: ${deltaBytes.length} bytes`);

        // For this example, we'll return a mock reconstructed message
        // In production, this should be replaced with actual delta reconstruction
        throw new Error('Delta reconstruction requires native modules (fossil-delta or xdelta3)');
    }

    /**
     * Get statistics
     */
    getStats() {
        const compressionRatio = this.stats.bytesReconstructed > 0
            ? ((1 - this.stats.bytesReceived / this.stats.bytesReconstructed) * 100).toFixed(1)
            : 0;

        return {
            ...this.stats,
            compressionRatio: `${compressionRatio}%`
        };
    }

    /**
     * Clear cache for a channel (on unsubscribe)
     */
    clearChannelCache(channel) {
        this.channelCaches.delete(channel);
        console.log(`🗑️  Cleared cache for channel: ${channel}`);
    }
}

/**
 * Main Example
 */
async function main() {
    console.log('🚀 Sockudo Delta Compression Client Example\n');

    // Initialize Pusher client
    const pusher = new Pusher('app-key', {
        wsHost: 'localhost',
        wsPort: 6001,
        forceTLS: false,
        disableStats: true,
        enabledTransports: ['ws']
    });

    // Initialize delta compression manager
    const deltaManager = new DeltaCompressionManager();

    // Enable delta compression after connection
    pusher.connection.bind('connected', () => {
        console.log('✅ Connected to Sockudo\n');
        console.log('📤 Enabling delta compression...');

        // Send enable delta compression event
        pusher.send_event('pusher:enable_delta_compression', {});
        console.log('✅ Delta compression enabled\n');
    });

    // Subscribe to a channel
    console.log('📡 Subscribing to channel: market-data\n');
    const channel = pusher.subscribe('market-data');

    // Handle cache sync
    channel.bind('pusher:delta_cache_sync', (data) => {
        deltaManager.handleCacheSync('market-data', JSON.parse(data));
    });

    // Handle price updates
    channel.bind('price-update', (data) => {
        // Process message (handles both full and delta)
        const fullMessage = deltaManager.processMessage('market-data', 'price-update', data);

        if (fullMessage) {
            console.log(`💰 Price Update:`, {
                asset: fullMessage.asset,
                price: fullMessage.price,
                volume: fullMessage.volume
            });
        }
    });

    // Handle subscription success
    channel.bind('pusher:subscription_succeeded', () => {
        console.log('✅ Subscribed to market-data channel\n');
        console.log('Waiting for messages...\n');
    });

    // Handle errors
    channel.bind('pusher:error', (error) => {
        console.error('❌ Error:', error);
    });

    // Print statistics every 5 seconds
    setInterval(() => {
        const stats = deltaManager.getStats();
        if (stats.fullMessages > 0 || stats.deltaMessages > 0) {
            console.log('\n📊 Statistics:', stats);
        }
    }, 5000);

    // Graceful shutdown
    process.on('SIGINT', () => {
        console.log('\n\n👋 Shutting down...');
        console.log('\n📊 Final Statistics:', deltaManager.getStats());
        pusher.disconnect();
        process.exit(0);
    });
}

// Run example
main().catch(console.error);
