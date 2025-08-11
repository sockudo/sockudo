const { expect } = require('chai');
const TestClient = require('../lib/TestClient');
const TestUtils = require('../lib/TestUtils');

describe('Presence Channel Limits Tests', function() {
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

    describe('Presence Channel Limits', function() {
        it('should track presence members accurately', async function() {
            const channelName = 'presence-member-test';
            const memberCount = 5; // Reasonable number for testing

            console.log(`Adding ${memberCount} members to presence channel...`);
            
            // Create multiple clients and subscribe to the same presence channel
            for (let i = 0; i < memberCount; i++) {
                const client = new TestClient();
                await client.connect();
                await client.subscribe(channelName);
                clients.push(client);
                
                // Small delay to avoid overwhelming the server
                await TestUtils.wait(200);
            }

            // Check member count on the last client (should see all members)
            await TestUtils.wait(1000); // Wait for presence updates to propagate
            
            const members = clients[clients.length - 1].getPresenceMembers(channelName);
            expect(members).to.exist;
            console.log(`Presence channel has ${members.count} members`);
            expect(members.count).to.equal(memberCount);
        });

        it('should track member count accurately', async function() {
            const channelName = 'presence-count-test';
            const clientCount = 3; // Reduced to avoid timeout issues

            // Add clients one by one and verify count
            for (let i = 0; i < clientCount; i++) {
                const client = new TestClient();
                await client.connect();
                await client.subscribe(channelName);
                clients.push(client);

                // Wait for presence update to propagate
                await TestUtils.wait(500);

                const members = client.getPresenceMembers(channelName);
                expect(members.count).to.equal(i + 1);
            }

            // Remove clients and verify count decreases
            while (clients.length > 0) {
                const client = clients.pop();
                await client.disconnect();
                
                // Wait for presence update
                await TestUtils.wait(500);

                if (clients.length > 0) {
                    const members = clients[0].getPresenceMembers(channelName);
                    expect(members.count).to.equal(clients.length);
                }
            }
        });
    });
});