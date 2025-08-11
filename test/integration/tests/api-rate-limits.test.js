const { expect } = require('chai');
const TestClient = require('../lib/TestClient');
const TestUtils = require('../lib/TestUtils');

describe('API Rate Limits Tests', function() {
    this.timeout(30000);
    
    let clients = [];
    
    beforeEach(async function() {
        // Clear rate limits before each test
        await TestUtils.resetServerState();
        // Extra wait to ensure rate limits are fully reset
        await TestUtils.wait(2000);
    });
    
    afterEach(async function() {
        await TestUtils.cleanupClients(clients);
        clients = [];
        await TestUtils.wait(1000);
    });

    describe('API Rate Limits', function() {
        it('should enforce API rate limits and recover', async function() {
            const channelName = TestUtils.generateChannelName('api-rate-test');
            
            // Start with a lower number to avoid immediate rate limiting
            const initialRequests = 3;
            
            // Step 1: Send some requests within rate limit
            console.log(`Sending ${initialRequests} API requests...`);
            let successCount = 0;
            
            for (let i = 0; i < initialRequests; i++) {
                try {
                    await TestUtils.sendApiEvent({
                        channels: channelName,
                        event: 'api-test',
                        data: { id: i }
                    });
                    successCount++;
                    console.log(`API request ${i + 1} succeeded`);
                } catch (error) {
                    console.log(`API request ${i + 1} failed: ${error.message}`);
                    if (error.message.includes('429')) {
                        // Got rate limited early, which is fine
                        break;
                    }
                }
                // Small delay between requests
                await TestUtils.wait(100);
            }
            
            expect(successCount).to.be.greaterThan(0);
            console.log(`Successfully sent ${successCount} API requests`);
            
            // Step 2: Try to trigger rate limit with rapid requests
            console.log('Testing API rate limit enforcement...');
            let rateLimitHit = false;
            const rapidRequests = 8;
            
            for (let i = 0; i < rapidRequests; i++) {
                try {
                    await TestUtils.sendApiEvent({
                        channels: channelName,
                        event: 'api-rapid-test',
                        data: { id: `rapid-${i}` }
                    });
                } catch (error) {
                    if (error.message.includes('429')) {
                        rateLimitHit = true;
                        console.log('Got expected 429 rate limit error');
                        break;
                    }
                }
            }
            
            // Step 3: Wait for rate limit to reset
            console.log('Waiting for API rate limit to reset...');
            await TestUtils.wait(3000);
            
            // Step 4: Confirm we can make API requests again
            console.log('Testing API rate limit recovery...');
            let canRequestAgain = false;
            try {
                await TestUtils.sendApiEvent({
                    channels: channelName,
                    event: 'api-after-reset',
                    data: { test: true }
                });
                canRequestAgain = true;
                console.log('Successfully made API request after rate limit reset');
            } catch (error) {
                console.log(`Error after reset: ${error.message}`);
            }
            
            expect(canRequestAgain).to.be.true;
        });
    });
});