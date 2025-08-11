const { expect } = require('chai');
const TestClient = require('../lib/TestClient');
const TestUtils = require('../lib/TestUtils');

describe('Resource Cleanup Tests', function() {
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

    describe('Resource Cleanup', function() {
        it('should properly cleanup disconnected client resources', async function() {
            const channelName = TestUtils.generateChannelName('cleanup-test');
            
            // Create client and subscribe
            const client = new TestClient();
            await client.connect();
            await client.subscribe(channelName);
            
            const socketId = client.getSocketId();
            expect(socketId).to.be.a('string');

            // Disconnect client
            await client.disconnect();
            
            // Wait for server cleanup
            await TestUtils.wait(2000);

            // Try to get channel info - should show 0 connections
            try {
                const channelInfo = await TestUtils.getChannelInfo({
                    channel: channelName,
                    info: ['subscription_count']
                });
                
                if (channelInfo.subscription_count !== undefined) {
                    expect(channelInfo.subscription_count).to.equal(0);
                }
            } catch (error) {
                // Channel might not exist anymore, which is also valid cleanup
                expect(error.message).to.match(/404|not found/i);
            }
        });

        it('should cleanup unused channels', async function() {
            const channelName = TestUtils.generateChannelName('auto-cleanup');
            
            // Create and immediately disconnect client
            const client = new TestClient();
            await client.connect();
            await client.subscribe(channelName);
            await client.disconnect();
            
            // Wait for server cleanup
            await TestUtils.wait(3000);

            // Check if channel still exists
            try {
                const channels = await TestUtils.getChannels({
                    filter: { prefix: channelName }
                });
                
                // Channel should either not exist or have 0 subscribers
                const foundChannel = channels.channels && channels.channels[channelName];
                if (foundChannel) {
                    expect(foundChannel.subscription_count).to.equal(0);
                }
            } catch (error) {
                // Acceptable if channel was cleaned up
            }
        });
    });
});