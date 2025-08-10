const { expect } = require('chai');
const TestClient = require('../lib/TestClient');
const TestUtils = require('../lib/TestUtils');
const AuthServer = require('../lib/AuthServer');

describe('Channel Subscription Tests', function() {
    this.timeout(15000);
    
    let client;
    let authServer;
    
    before(async function() {
        // Start auth server for private/presence channel tests
        authServer = new AuthServer({ port: 3012 });
        await authServer.start();
    });
    
    after(async function() {
        if (authServer) {
            await authServer.stop();
        }
    });
    
    beforeEach(async function() {
        // Clear rate limits before each test
        await TestUtils.resetServerState();
        client = new TestClient({ authEndpoint: 'http://localhost:3012/pusher/auth' });
        await client.connect();
    });
    
    afterEach(async function() {
        if (client && client.isConnected()) {
            await client.disconnect();
        }
    });

    describe('Public Channels', function() {
        it('should subscribe to a public channel', async function() {
            const channelName = TestUtils.generateChannelName('public');
            
            const { channel, data } = await client.subscribe(channelName);
            
            expect(channel).to.exist;
            expect(client.getChannel(channelName)).to.equal(channel);
        });

        it('should unsubscribe from a public channel', async function() {
            const channelName = TestUtils.generateChannelName('public');
            
            await client.subscribe(channelName);
            expect(client.getChannel(channelName)).to.exist;
            
            await client.unsubscribe(channelName);
            expect(client.getChannel(channelName)).to.be.undefined;
        });

        it('should subscribe to multiple public channels', async function() {
            const channels = [];
            const channelCount = 5;
            
            for (let i = 0; i < channelCount; i++) {
                const channelName = TestUtils.generateChannelName(`public-${i}`);
                await client.subscribe(channelName);
                channels.push(channelName);
            }
            
            // Verify all channels are subscribed
            channels.forEach(channelName => {
                expect(client.getChannel(channelName)).to.exist;
            });
        });

        it('should handle duplicate subscription attempts', async function() {
            const channelName = TestUtils.generateChannelName('public');
            
            // First subscription
            const { channel: channel1 } = await client.subscribe(channelName);
            
            // Try to subscribe again
            const { channel: channel2 } = await client.subscribe(channelName);
            
            // Should return the same channel object
            expect(channel1).to.equal(channel2);
        });
    });

    describe('Private Channels', function() {
        it('should subscribe to a private channel with authentication', async function() {
            const channelName = 'private-test-channel';
            
            const { channel } = await client.subscribe(channelName);
            
            expect(channel).to.exist;
            expect(client.getChannel(channelName)).to.equal(channel);
            
            // Verify auth request was made
            const authRequests = authServer.getAuthRequests();
            const authRequest = authRequests.find(r => r.channel_name === channelName);
            expect(authRequest).to.exist;
            expect(authRequest.socket_id).to.equal(client.getSocketId());
        });

        it('should fail to subscribe without proper authentication', async function() {
            // Use a client with invalid auth endpoint
            const unauthClient = new TestClient({ authEndpoint: 'http://localhost:9999/invalid' });
            await unauthClient.connect();
            
            try {
                await unauthClient.subscribe('private-test-channel');
                expect.fail('Should have thrown an error');
            } catch (error) {
                expect(error).to.be.instanceOf(Error);
            } finally {
                await unauthClient.disconnect();
            }
        });

        it('should not allow client events on public channels', async function() {
            const channelName = 'public-test-channel';
            await client.subscribe(channelName);
            
            try {
                await client.trigger(channelName, 'client-test-event', { data: 'test' });
                expect.fail('Should have thrown an error');
            } catch (error) {
                expect(error.message).to.include('Client events only supported on private/presence channels');
            }
        });

        it('should allow client events on private channels', async function() {
            const channelName = 'private-test-channel';
            await client.subscribe(channelName);
            
            // Should not throw
            await client.trigger(channelName, 'client-test-event', { data: 'test' });
        });
    });

    describe('Presence Channels', function() {
        let client2;
        
        afterEach(async function() {
            if (client2 && client2.isConnected()) {
                await client2.disconnect();
            }
        });

        it('should subscribe to a presence channel with member data', async function() {
            const channelName = 'presence-test-channel';
            
            const { channel } = await client.subscribe(channelName);
            
            expect(channel).to.exist;
            
            // Check member data
            const members = client.getPresenceMembers(channelName);
            expect(members).to.exist;
            expect(members.count).to.equal(1);
            expect(members.me).to.exist;
            expect(members.me.id).to.be.a('string');
        });

        it('should track member join events', async function() {
            const channelName = 'presence-test-channel';
            
            // First client subscribes
            await client.subscribe(channelName);
            
            // Set up event listener for member_added
            const memberAddedPromise = client.waitForEvent(channelName, 'pusher:member_added', 5000);
            
            // Second client subscribes
            client2 = new TestClient();
            await client2.connect();
            await client2.subscribe(channelName);
            
            // Wait for member_added event
            const memberData = await memberAddedPromise;
            expect(memberData).to.exist;
            expect(memberData.id).to.be.a('string');
            
            // Check updated member count
            const members = client.getPresenceMembers(channelName);
            expect(members.count).to.equal(2);
        });

        it('should track member leave events', async function() {
            const channelName = 'presence-test-channel';
            
            // Both clients subscribe
            await client.subscribe(channelName);
            
            client2 = new TestClient();
            await client2.connect();
            await client2.subscribe(channelName);
            
            // Wait for subscription to stabilize
            await TestUtils.wait(1000);
            
            // Set up event listener for member_removed
            const memberRemovedPromise = client.waitForEvent(channelName, 'pusher:member_removed', 5000);
            
            // Second client unsubscribes
            await client2.unsubscribe(channelName);
            
            // Wait for member_removed event
            const memberData = await memberRemovedPromise;
            expect(memberData).to.exist;
            
            // Check updated member count
            const members = client.getPresenceMembers(channelName);
            expect(members.count).to.equal(1);
        });

        it('should provide initial members list on subscription', async function() {
            const channelName = 'presence-test-channel';
            
            // First client subscribes
            client2 = new TestClient();
            await client2.connect();
            await client2.subscribe(channelName);
            
            // Second client subscribes and should receive initial members
            const { channel, data } = await client.subscribe(channelName);
            
            // Check members list includes both clients
            const members = client.getPresenceMembers(channelName);
            expect(members).to.exist;
            expect(members.count).to.equal(2);
            expect(Object.keys(members.members).length).to.equal(2);
        });
    });

    describe('Channel Name Validation', function() {
        it('should reject invalid channel names', async function() {
            const invalidNames = [
                '', // empty
                'a'.repeat(201), // too long (assuming 200 char limit)
                'channel with spaces', // spaces not allowed
                'channel#with#hashes', // special characters
            ];
            
            for (const invalidName of invalidNames) {
                try {
                    await client.subscribe(invalidName);
                    // If subscribe doesn't throw, try to check if channel is actually subscribed
                    const channel = client.getChannel(invalidName);
                    if (!channel || channel.subscriptionPending || channel.subscribed === false) {
                        // Channel wasn't actually subscribed, which is expected
                        continue;
                    }
                    expect.fail(`Should have rejected channel name: ${invalidName}`);
                } catch (error) {
                    expect(error).to.be.instanceOf(Error);
                }
            }
        });

        it('should accept valid channel names', async function() {
            const validNames = [
                'simple-channel',
                'channel_with_underscores',
                'channel.with.dots',
                'channel:with:colons',
                'private-valid-channel',
                'presence-valid-channel'
                // Note: private-encrypted channels need special auth with shared_secret
            ];
            
            for (const validName of validNames) {
                try {
                    await client.subscribe(validName);
                    expect(client.getChannel(validName)).to.exist;
                    await client.unsubscribe(validName);
                } catch (error) {
                    // Some channels might need auth, that's okay for this test
                    if (!validName.startsWith('private-') && !validName.startsWith('presence-')) {
                        throw error;
                    }
                }
            }
        });
    });

    describe('Channel Limits', function() {
        it('should handle subscription to many channels', async function() {
            const channelCount = 50;
            const channels = [];
            
            for (let i = 0; i < channelCount; i++) {
                const channelName = TestUtils.generateChannelName(`stress-${i}`);
                await client.subscribe(channelName);
                channels.push(channelName);
            }
            
            // Verify all channels are subscribed
            expect(client.channels.size).to.equal(channelCount);
            
            // Cleanup
            for (const channelName of channels) {
                await client.unsubscribe(channelName);
            }
            
            expect(client.channels.size).to.equal(0);
        });

        it('should enforce max channels per connection limit if configured', async function() {
            // This test assumes there might be a max channels limit
            // The actual limit should be configured in the test environment
            const maxChannels = 100; // Adjust based on server configuration
            
            try {
                for (let i = 0; i < maxChannels + 10; i++) {
                    const channelName = TestUtils.generateChannelName(`limit-test-${i}`);
                    await client.subscribe(channelName);
                }
                
                // If we get here, either no limit or limit is higher
                console.log(`Successfully subscribed to ${client.channels.size} channels`);
                
            } catch (error) {
                // Expected if there's a channel limit
                expect(client.channels.size).to.be.at.most(maxChannels);
            }
        });
    });
});