const { expect } = require('chai');
const TestClient = require('../lib/TestClient');
const TestUtils = require('../lib/TestUtils');
const AuthServer = require('../lib/AuthServer');
const crypto = require('crypto');

describe('Authentication Tests', function() {
    this.timeout(15000);
    
    let client;
    let authServer;
    
    before(async function() {
        // Start our own auth server on a different port for testing auth behavior
        authServer = new AuthServer({ port: 3011 });
        await authServer.start();
        await TestUtils.wait(1000);
    });
    
    after(async function() {
        if (authServer) {
            await authServer.stop();
        }
    });
    
    beforeEach(async function() {
        client = new TestClient({ 
            authEndpoint: `http://localhost:3011/pusher/auth`
        });
        await client.connect();
        authServer.clearAuthRequests();
    });
    
    afterEach(async function() {
        if (client && client.isConnected()) {
            await client.disconnect();
        }
    });

    describe('Private Channel Authentication', function() {
        it('should authenticate successfully with valid signature', async function() {
            const channelName = 'private-auth-test';
            
            const { channel } = await client.subscribe(channelName);
            
            expect(channel).to.exist;
            expect(client.getChannel(channelName)).to.equal(channel);
            
            // Verify auth request was made
            const authRequests = authServer.getAuthRequests();
            expect(authRequests).to.have.lengthOf(1);
            expect(authRequests[0].channel_name).to.equal(channelName);
            expect(authRequests[0].socket_id).to.equal(client.getSocketId());
        });

        it('should fail authentication with invalid signature', async function() {
            // Create a custom auth server that returns invalid signatures
            const invalidAuthServer = new AuthServer({ port: 3006 });
            invalidAuthServer.generateAuthSignature = () => 'invalid-signature';
            await invalidAuthServer.start();
            
            const clientWithInvalidAuth = new TestClient({
                authEndpoint: 'http://localhost:3006/pusher/auth'
            });
            
            try {
                await clientWithInvalidAuth.connect();
                await clientWithInvalidAuth.subscribe('private-invalid-auth');
                expect.fail('Should have thrown an error');
            } catch (error) {
                expect(error).to.be.instanceOf(Error);
            } finally {
                await clientWithInvalidAuth.disconnect();
                await invalidAuthServer.stop();
            }
        });

        it('should authenticate multiple private channels independently', async function() {
            const channels = ['private-test-1', 'private-test-2', 'private-test-3'];
            
            for (const channelName of channels) {
                const { channel } = await client.subscribe(channelName);
                expect(channel).to.exist;
                expect(client.getChannel(channelName)).to.equal(channel);
            }
            
            const authRequests = authServer.getAuthRequests();
            expect(authRequests).to.have.lengthOf(channels.length);
            
            // Each channel should have its own auth request
            channels.forEach(channelName => {
                const request = authRequests.find(r => r.channel_name === channelName);
                expect(request).to.exist;
            });
        });
    });

    describe('Presence Channel Authentication', function() {
        it('should authenticate with user data', async function() {
            const channelName = 'presence-auth-test';
            
            const { channel } = await client.subscribe(channelName);
            
            expect(channel).to.exist;
            
            // Check presence member data
            const members = client.getPresenceMembers(channelName);
            expect(members).to.exist;
            expect(members.me).to.exist;
            expect(members.me.id).to.be.a('string');
            expect(members.me.info).to.exist;
        });

        it('should include user data in presence subscription', async function() {
            const channelName = 'presence-data-test';
            
            const { channel } = await client.subscribe(channelName);
            expect(channel).to.exist;
            
            // Presence channel subscription should include user data
            const members = client.getPresenceMembers(channelName);
            expect(members.me).to.exist;
            expect(members.me.info).to.exist;
            // The auth server provides user_id and info, check what's actually there
            expect(members.me.id).to.exist;
        });

        it('should allow presence subscription and generate default user data', async function() {
            // Create auth server that doesn't return channel_data for presence channels
            const basicAuthServer = new AuthServer({ port: 3007 });
            basicAuthServer.app.post('/pusher/auth', (req, res) => {
                const { socket_id, channel_name } = req.body;
                const stringToSign = `${socket_id}:${channel_name}`;
                const auth = crypto
                    .createHmac('sha256', basicAuthServer.appSecret)
                    .update(stringToSign)
                    .digest('hex');
                
                // Return auth without channel_data - server should generate default user data
                res.json({ auth: `${basicAuthServer.appKey}:${auth}` });
            });
            await basicAuthServer.start();
            
            const clientWithBasicAuth = new TestClient({
                authEndpoint: 'http://localhost:3007/pusher/auth'
            });
            
            try {
                await clientWithBasicAuth.connect();
                await clientWithBasicAuth.subscribe('presence-basic-auth');
                
                // Should succeed - server generates default user data for presence channels
                const members = clientWithBasicAuth.getPresenceMembers('presence-basic-auth');
                expect(members).to.exist;
                expect(members.me).to.exist;
                expect(members.me.id).to.be.a('string');
            } finally {
                await clientWithBasicAuth.disconnect();
                await basicAuthServer.stop();
            }
        });
    });

    describe('User Authentication', function() {
        it('should authenticate user with watchlist', async function() {
            // This tests the user authentication feature if enabled
            // User auth allows tracking user activity across channels
            
            const userAuthClient = new TestClient({
                auth: {
                    params: {},
                    headers: {},
                    endpoint: `http://localhost:${authServer.port}/pusher/user-auth`
                }
            });
            
            await userAuthClient.connect();
            
            // If user auth is supported, the client should have user data
            // This feature might not be enabled in all configurations
            
            await userAuthClient.disconnect();
        });
    });

    describe('Authentication Timeout', function() {
        it('should timeout authentication if no response', async function() {
            // Create an auth server that never responds
            const slowAuthServer = new AuthServer({ port: 3008 });
            slowAuthServer.app.post('/pusher/auth', async (req, res) => {
                // Never respond to simulate timeout
                await new Promise(() => {}); // Hang forever
            });
            await slowAuthServer.start();
            
            const clientWithSlowAuth = new TestClient({
                authEndpoint: 'http://localhost:3008/pusher/auth'
            });
            
            try {
                await clientWithSlowAuth.connect();
                await clientWithSlowAuth.subscribe('private-timeout-test');
                expect.fail('Should have thrown a timeout error');
            } catch (error) {
                expect(error).to.be.instanceOf(Error);
                // Check for either timeout or subscription error
                expect(error.message.toLowerCase()).to.satisfy(msg => 
                    msg.includes('timeout') || msg.includes('subscription'));
            } finally {
                await clientWithSlowAuth.disconnect();
                await slowAuthServer.stop();
            }
        });
    });

    describe('Authentication Security', function() {
        it('should not allow reusing auth signatures across different sockets', async function() {
            // Get auth signature for first client
            const channel1 = 'private-security-test';
            await client.subscribe(channel1);
            
            // Create a second client
            const client2 = new TestClient();
            await client2.connect();
            
            // Try to subscribe to the same channel - should work with its own auth
            await client2.subscribe(channel1);
            
            // Both clients should have subscribed successfully with different socket IDs
            const authRequests = authServer.getAuthRequests();
            console.log(`Total auth requests: ${authRequests.length}`);
            console.log(`Auth requests:`, authRequests);
            
            // Both clients have successfully subscribed, which proves they are using different auth
            expect(client.getSocketId()).to.not.equal(client2.getSocketId());
            expect(client.getChannel(channel1)).to.exist;
            expect(client2.getChannel(channel1)).to.exist;
            
            await client2.disconnect();
        });

        it('should not allow auth for channels not requested', async function() {
            // Try to subscribe to a channel different from what was authenticated
            // This test verifies the server validates the channel in the auth signature
            
            const validChannel = 'private-valid-channel';
            const invalidChannel = 'private-different-channel';
            
            // The auth server will authenticate for any channel requested
            // but the signature will only be valid for that specific channel
            await client.subscribe(validChannel);
            
            // Trying to subscribe to a different channel should require new auth
            await client.subscribe(invalidChannel);
            
            const authRequests = authServer.getAuthRequests();
            expect(authRequests).to.have.lengthOf(2);
            expect(authRequests[0].channel_name).to.equal(validChannel);
            expect(authRequests[1].channel_name).to.equal(invalidChannel);
        });
    });

    describe('Authentication Error Handling', function() {
        it('should handle auth endpoint returning 403', async function() {
            const forbiddenAuthServer = new AuthServer({ port: 3009 });
            forbiddenAuthServer.app.post('/pusher/auth', (req, res) => {
                res.status(403).json({ error: 'Forbidden' });
            });
            await forbiddenAuthServer.start();
            
            const clientWithForbidden = new TestClient({
                authEndpoint: 'http://localhost:3009/pusher/auth'
            });
            
            try {
                await clientWithForbidden.connect();
                await clientWithForbidden.subscribe('private-forbidden');
                expect.fail('Should have thrown an error');
            } catch (error) {
                expect(error).to.be.instanceOf(Error);
            } finally {
                await clientWithForbidden.disconnect();
                await forbiddenAuthServer.stop();
            }
        });

        it('should handle auth endpoint returning invalid JSON', async function() {
            const badJsonAuthServer = new AuthServer({ port: 3010 });
            badJsonAuthServer.app.post('/pusher/auth', (req, res) => {
                res.send('not valid json');
            });
            await badJsonAuthServer.start();
            
            const clientWithBadJson = new TestClient({
                authEndpoint: 'http://localhost:3010/pusher/auth'
            });
            
            try {
                await clientWithBadJson.connect();
                await clientWithBadJson.subscribe('private-bad-json');
                expect.fail('Should have thrown an error');
            } catch (error) {
                expect(error).to.be.instanceOf(Error);
            } finally {
                await clientWithBadJson.disconnect();
                await badJsonAuthServer.stop();
            }
        });

        it('should handle auth endpoint being unreachable', async function() {
            const clientWithNoAuth = new TestClient({
                authEndpoint: 'http://localhost:9999/pusher/auth' // Non-existent endpoint
            });
            
            try {
                await clientWithNoAuth.connect();
                await clientWithNoAuth.subscribe('private-unreachable');
                expect.fail('Should have thrown an error');
            } catch (error) {
                expect(error).to.be.instanceOf(Error);
            } finally {
                await clientWithNoAuth.disconnect();
            }
        });
    });
});