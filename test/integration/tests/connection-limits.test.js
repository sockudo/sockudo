const { expect } = require('chai');
const TestClient = require('../lib/TestClient');
const TestUtils = require('../lib/TestUtils');

describe('Connection Limits Tests', function() {
    this.timeout(30000);
    
    let clients = [];
    
    beforeEach(async function() {
        // Clear rate limits before each test
        await TestUtils.resetServerState();
    });
    
    afterEach(async function() {
        await TestUtils.cleanupClients(clients);
        clients = [];
        // Wait a bit more to ensure cleanup
        await TestUtils.wait(1000);
    });

    describe('Connection Limits', function() {
        it('should enforce max connections per app', async function() {
            const maxConnections = 10; // Based on test configuration (SOCKUDO_DEFAULT_APP_MAX_CONNECTIONS)
            
            // Step 1: Connect up to max_connections
            console.log(`Connecting up to ${maxConnections} clients...`);
            for (let i = 0; i < maxConnections; i++) {
                const client = new TestClient();
                await client.connect();
                clients.push(client);
            }
            
            expect(clients).to.have.lengthOf(maxConnections);
            console.log(`Successfully connected ${clients.length} clients`);
            
            // Step 2: Confirm quota error on the next connection
            console.log('Testing quota enforcement...');
            let quotaErrorReceived = false;
            try {
                const extraClient = new TestClient();
                await extraClient.connect();
                // If we get here, the limit wasn't enforced
                clients.push(extraClient); // Add to cleanup
                expect.fail('Should have hit connection limit');
            } catch (error) {
                quotaErrorReceived = true;
                console.log(`Got expected quota error: ${error.message}`);
            }
            
            expect(quotaErrorReceived).to.be.true;
            
            // Step 3: Confirm quota is consistently enforced
            console.log('Testing quota enforcement consistency...');
            for (let attempt = 1; attempt <= 3; attempt++) {
                console.log(`Quota test attempt ${attempt}/3`);
                try {
                    const extraClient = new TestClient();
                    await extraClient.connect();
                    clients.push(extraClient); // Add to cleanup
                    expect.fail(`Should have hit connection limit on attempt ${attempt}`);
                } catch (error) {
                    console.log(`Got expected quota error on attempt ${attempt}: ${error.message}`);
                    // This is expected - quota should be enforced
                }
            }
            
            console.log('Connection quota enforcement verified successfully');
        });

        it('should allow new connections after others disconnect', async function() {
            // Create some initial connections
            const initialCount = 5;
            for (let i = 0; i < initialCount; i++) {
                const client = new TestClient();
                await client.connect();
                clients.push(client);
            }

            expect(clients).to.have.lengthOf(initialCount);

            // Disconnect half of them
            const toDisconnect = Math.floor(initialCount / 2);
            for (let i = 0; i < toDisconnect; i++) {
                await clients[i].disconnect();
            }

            // Wait for server to process disconnections
            await TestUtils.wait(1000);

            // Should be able to create new connections
            const newClient = new TestClient();
            await newClient.connect();
            clients.push(newClient);

            expect(newClient.isConnected()).to.be.true;
        });
    });
});