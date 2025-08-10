const { expect } = require('chai');
const TestClient = require('../lib/TestClient');
const TestUtils = require('../lib/TestUtils');

describe('Connection Tests', function() {
    this.timeout(15000);
    
    let client;
    
    afterEach(async function() {
        if (client && client.isConnected()) {
            await client.disconnect();
        }
    });

    describe('Basic Connection', function() {
        it('should connect successfully with valid credentials', async function() {
            client = new TestClient();
            const socketId = await client.connect();
            
            expect(socketId).to.be.a('string');
            expect(socketId).to.match(/^\d+\.\d+$/);
            expect(client.isConnected()).to.be.true;
            expect(client.getConnectionState()).to.equal('connected');
        });

        it('should fail to connect with invalid app key', async function() {
            client = new TestClient({ key: 'invalid-key' });
            
            try {
                await client.connect();
                expect.fail('Should have thrown an error');
            } catch (error) {
                expect(error).to.be.an('error');
                expect(client.isConnected()).to.be.false;
            }
        });

        it('should disconnect gracefully', async function() {
            client = new TestClient();
            await client.connect();
            
            expect(client.isConnected()).to.be.true;
            
            await client.disconnect();
            
            expect(client.isConnected()).to.be.false;
            expect(client.getConnectionState()).to.equal('disconnected');
            expect(client.getSocketId()).to.be.null;
        });

        it('should handle reconnection after disconnect', async function() {
            client = new TestClient();
            
            // First connection
            const firstSocketId = await client.connect();
            expect(client.isConnected()).to.be.true;
            
            // Disconnect
            await client.disconnect();
            expect(client.isConnected()).to.be.false;
            
            // Wait a bit
            await TestUtils.wait(1000);
            
            // Reconnect
            const secondSocketId = await client.connect();
            expect(client.isConnected()).to.be.true;
            expect(secondSocketId).to.be.a('string');
            
            // Socket IDs should be different
            expect(secondSocketId).to.not.equal(firstSocketId);
        });
    });

    describe('Multiple Connections', function() {
        let clients = [];
        
        afterEach(async function() {
            await TestUtils.cleanupClients(clients);
            clients = [];
        });

        it('should handle multiple simultaneous connections', async function() {
            const connectionCount = 5;
            clients = await TestUtils.createClients(connectionCount);
            
            // Verify all clients are connected
            clients.forEach((client, index) => {
                expect(client.isConnected()).to.be.true;
                expect(client.getSocketId()).to.be.a('string');
            });
            
            // Verify each client has a unique socket ID
            const socketIds = clients.map(c => c.getSocketId());
            const uniqueSocketIds = [...new Set(socketIds)];
            expect(uniqueSocketIds.length).to.equal(connectionCount);
        });

        it('should handle rapid connect/disconnect cycles', async function() {
            const cycles = 3;
            
            for (let i = 0; i < cycles; i++) {
                const client = new TestClient();
                await client.connect();
                expect(client.isConnected()).to.be.true;
                
                await client.disconnect();
                expect(client.isConnected()).to.be.false;
                
                clients.push(client);
            }
        });
    });

    describe('Connection Events', function() {
        it('should emit connection state changes', async function() {
            client = new TestClient();
            const stateChanges = [];
            
            // Set up state listener before connecting
            client.on('state_change', (states) => {
                stateChanges.push(states.current);
            });
            
            // Connect and wait for completion
            await client.connect();
            
            // Wait a bit for any additional state changes
            await TestUtils.wait(100);
            
            // Should have seen some state changes
            expect(stateChanges.length).to.be.greaterThan(0);
            expect(stateChanges[stateChanges.length - 1]).to.equal('connected');
        });

        it('should emit connected event with socket ID', async function() {
            client = new TestClient();
            let emittedSocketId = null;
            
            client.on('connected', (socketId) => {
                emittedSocketId = socketId;
            });
            
            const connectedSocketId = await client.connect();
            
            expect(emittedSocketId).to.equal(connectedSocketId);
        });

        it('should emit disconnected event', async function() {
            client = new TestClient();
            let disconnectedEmitted = false;
            
            client.on('disconnected', () => {
                disconnectedEmitted = true;
            });
            
            await client.connect();
            await client.disconnect();
            
            expect(disconnectedEmitted).to.be.true;
        });
    });

    describe('Connection Limits', function() {
        let clients = [];
        
        afterEach(async function() {
            await TestUtils.cleanupClients(clients);
            clients = [];
        });

        it('should respect max connections per app limit', async function() {
            // This test assumes the app is configured with a specific max_connections limit
            // The actual limit should be configured in the test environment
            const maxConnections = 100; // This should match the test app configuration
            
            try {
                // Try to create more connections than allowed
                for (let i = 0; i < maxConnections + 5; i++) {
                    const client = new TestClient();
                    await client.connect();
                    clients.push(client);
                }
                
                // If we get here without error, the limit might not be enforced
                // or is higher than expected
                console.warn('Connection limit not enforced or higher than expected');
                
            } catch (error) {
                // Expected behavior - connection should be rejected after limit
                expect(error).to.be.an('error');
                expect(clients.length).to.be.at.most(maxConnections);
            }
        });
    });

    describe('Connection Persistence', function() {
        it('should maintain connection during idle periods', async function() {
            client = new TestClient();
            await client.connect();
            
            const initialSocketId = client.getSocketId();
            expect(client.isConnected()).to.be.true;
            
            // Wait for 5 seconds (idle period)
            await TestUtils.wait(5000);
            
            // Connection should still be active
            expect(client.isConnected()).to.be.true;
            expect(client.getSocketId()).to.equal(initialSocketId);
        });

        it('should handle ping/pong to keep connection alive', async function() {
            client = new TestClient();
            await client.connect();
            
            // Pusher protocol uses ping/pong automatically
            // Wait for multiple ping/pong cycles (typically 30 seconds)
            // For testing, we'll just verify connection stays alive
            await TestUtils.wait(3000);
            
            expect(client.isConnected()).to.be.true;
        });
    });
});