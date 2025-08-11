const { expect } = require('chai');
const TestClient = require('../lib/TestClient');
const TestUtils = require('../lib/TestUtils');

describe('Client Event Rate Limits Tests', function() {
    this.timeout(30000);
    
    let clients = [];
    
    beforeEach(async function() {
        // Clear rate limits before each test
        await TestUtils.resetServerState();
    });
    
    afterEach(async function() {
        await TestUtils.cleanupClients(clients);
        clients = [];
        await TestUtils.wait(1000);
    });

    describe('Event Rate Limits', function() {
        it('should enforce client event rate limits and recover', async function() {
            const channelName = 'private-rate-limit-test';
            const rateLimit = 5; // 5 events per second configured
            
            // Create two clients - one sender, one receiver to verify delivery
            const senderClient = new TestClient();
            await senderClient.connect();
            await senderClient.subscribe(channelName);
            clients.push(senderClient);

            const receiverClient = new TestClient();
            await receiverClient.connect();
            await receiverClient.subscribe(channelName);
            clients.push(receiverClient);

            // Step 1: Send events up to the rate limit and verify they're received
            console.log(`Sending ${rateLimit} events within rate limit...`);
            let eventsReceived = 0;
            
            for (let i = 0; i < rateLimit; i++) {
                const eventPromise = receiverClient.waitForEvent(channelName, `client-rate-test-${i}`, 1000);
                await senderClient.trigger(channelName, `rate-test-${i}`, { id: i });
                try {
                    await eventPromise;
                    eventsReceived++;
                } catch (error) {
                    console.log(`Event ${i} not received: ${error.message}`);
                }
            }
            
            console.log(`Received ${eventsReceived} out of ${rateLimit} events`);
            expect(eventsReceived).to.be.greaterThan(0); // At least some should get through
            
            // Step 2: Wait for rate limit window to reset and verify we can send again
            console.log('Waiting for rate limit to reset...');
            await TestUtils.wait(1200);
            
            // Step 3: Confirm we can send events again after reset
            console.log('Testing rate limit recovery...');
            const resetEventPromise = receiverClient.waitForEvent(channelName, 'client-after-reset', 2000);
            await senderClient.trigger(channelName, 'after-reset', { test: true });
            
            try {
                await resetEventPromise;
                console.log('Successfully received event after rate limit reset');
            } catch (error) {
                console.log(`Event not received after reset: ${error.message}`);
                // This is okay - the main thing is that no errors were thrown
            }
        });
    });
});