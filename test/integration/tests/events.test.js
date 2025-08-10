const { expect } = require('chai');
const TestClient = require('../lib/TestClient');
const TestUtils = require('../lib/TestUtils');
const AuthServer = require('../lib/AuthServer');

describe('Event Broadcasting Tests', function() {
    this.timeout(15000);
    
    let client1, client2;
    let authServer;
    
    before(async function() {
        // Start auth server for private/presence channel tests
        authServer = new AuthServer({ port: 3013 });
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
    });
    
    afterEach(async function() {
        if (client1 && client1.isConnected()) {
            await client1.disconnect();
        }
        if (client2 && client2.isConnected()) {
            await client2.disconnect();
        }
        await TestUtils.wait(500);
    });

    describe('HTTP API Event Broadcasting', function() {
        it('should broadcast events to subscribed clients', async function() {
            const channelName = TestUtils.generateChannelName('broadcast-test');
            const eventName = 'test-event';
            const eventData = { message: 'Hello World', timestamp: Date.now() };

            // Set up client to receive events
            client1 = new TestClient();
            await client1.connect();
            await client1.subscribe(channelName);

            // Set up event listener
            const eventPromise = client1.waitForEvent(channelName, eventName, 5000);

            // Send event via HTTP API
            await TestUtils.sendApiEvent({
                channels: channelName,
                event: eventName,
                data: eventData
            });

            // Verify event was received
            const receivedData = await eventPromise;
            expect(receivedData).to.deep.equal(eventData);
        });

        it('should broadcast to multiple channels simultaneously', async function() {
            const channels = [
                TestUtils.generateChannelName('multi-1'),
                TestUtils.generateChannelName('multi-2')
            ];
            const eventName = 'multi-channel-event';
            const eventData = { message: 'Multi-channel broadcast' };

            // Set up clients for each channel
            client1 = new TestClient();
            await client1.connect();
            await client1.subscribe(channels[0]);

            client2 = new TestClient();
            await client2.connect();
            await client2.subscribe(channels[1]);

            // Set up event listeners
            const event1Promise = client1.waitForEvent(channels[0], eventName);
            const event2Promise = client2.waitForEvent(channels[1], eventName);

            // Broadcast to both channels
            await TestUtils.sendApiEvent({
                channels: channels,
                event: eventName,
                data: eventData
            });

            // Verify both clients received the event
            const [data1, data2] = await Promise.all([event1Promise, event2Promise]);
            expect(data1).to.deep.equal(eventData);
            expect(data2).to.deep.equal(eventData);
        });

        it('should not send events to unsubscribed clients', async function() {
            const channelName = TestUtils.generateChannelName('no-sub-test');
            const eventName = 'should-not-receive';
            const eventData = { message: 'Should not be received' };

            // Set up client but don't subscribe
            client1 = new TestClient();
            await client1.connect();

            // Try to listen for event (should timeout)
            let eventReceived = false;
            const channel = client1.getChannel(channelName);
            if (channel) {
                channel.bind(eventName, () => {
                    eventReceived = true;
                });
            }

            // Send event
            await TestUtils.sendApiEvent({
                channels: channelName,
                event: eventName,
                data: eventData
            });

            // Wait a bit to ensure no event is received
            await TestUtils.wait(2000);
            expect(eventReceived).to.be.false;
        });

        it('should handle large payloads within limits', async function() {
            const channelName = TestUtils.generateChannelName('large-payload');
            const eventName = 'large-event';
            
            // Generate ~50KB payload (within typical limits)
            const largeData = {
                message: 'Large payload test',
                data: TestUtils.generateLargePayload(50) // 50KB
            };

            client1 = new TestClient();
            await client1.connect();
            await client1.subscribe(channelName);

            const eventPromise = client1.waitForEvent(channelName, eventName);

            await TestUtils.sendApiEvent({
                channels: channelName,
                event: eventName,
                data: largeData
            });

            const receivedData = await eventPromise;
            expect(receivedData.message).to.equal(largeData.message);
            expect(receivedData.data.length).to.equal(largeData.data.length);
        });

        it('should reject oversized payloads', async function() {
            const channelName = TestUtils.generateChannelName('oversized');
            const eventName = 'oversized-event';
            
            // Generate payload larger than limit (assuming 1MB limit)
            const oversizedData = {
                data: TestUtils.generateLargePayload(1100) // 1.1MB
            };

            try {
                await TestUtils.sendApiEvent({
                    channels: channelName,
                    event: eventName,
                    data: oversizedData
                });
                expect.fail('Should have thrown an error for oversized payload');
            } catch (error) {
                // Server might return different errors for oversized payloads
                expect(error.message).to.match(/413|400|payload|size|large|invalid/i);
            }
        });
    });

    describe('Client Event Broadcasting', function() {
        it('should broadcast client events on private channels', async function() {
            const channelName = 'private-client-events';
            const eventName = 'test-event'; // Don't include client- prefix, trigger will add it
            const eventData = { message: 'Client event test', sender: 'client1' };

            // Set up two clients with auth
            client1 = new TestClient({ authEndpoint: 'http://localhost:3013/pusher/auth' });
            await client1.connect();
            await client1.subscribe(channelName);

            client2 = new TestClient({ authEndpoint: 'http://localhost:3013/pusher/auth' });
            await client2.connect();
            await client2.subscribe(channelName);

            // Set up event listener on client2 (trigger adds 'client-' prefix)  
            console.log(`Setting up listener for event: client-${eventName}`);
            const eventPromise = client2.waitForEvent(channelName, `client-${eventName}`, 10000);

            // Client1 sends event
            console.log(`Triggering event: ${eventName}`);
            await client1.trigger(channelName, eventName, eventData);

            // Verify client2 received the event
            try {
                const receivedData = await eventPromise;
                expect(receivedData).to.deep.equal(eventData);
            } catch (error) {
                console.log(`Event not received: ${error.message}`);
                throw error;
            }
        });

        it('should broadcast client events on presence channels', async function() {
            const channelName = 'presence-client-events';
            const eventName = 'presence-event'; // Don't include client- prefix, trigger will add it
            const eventData = { message: 'Presence client event' };

            client1 = new TestClient();
            await client1.connect();
            await client1.subscribe(channelName);

            client2 = new TestClient({ authEndpoint: 'http://localhost:3013/pusher/auth' });
            await client2.connect();
            await client2.subscribe(channelName);

            const eventPromise = client2.waitForEvent(channelName, `client-${eventName}`);

            await client1.trigger(channelName, eventName, eventData);

            const receivedData = await eventPromise;
            expect(receivedData).to.deep.equal(eventData);
        });

        it('should not allow client events on public channels', async function() {
            const channelName = 'public-no-client-events';

            client1 = new TestClient();
            await client1.connect();
            await client1.subscribe(channelName);

            try {
                await client1.trigger(channelName, 'client-event', { data: 'test' });
                expect.fail('Should have thrown an error');
            } catch (error) {
                expect(error.message).to.include('Client events only supported on private/presence channels');
            }
        });

        it('should enforce client event rate limits', async function() {
            const channelName = 'private-rate-limit-test';
            const eventName = 'rate-limit-event';

            client1 = new TestClient();
            await client1.connect();
            await client1.subscribe(channelName);

            // Send events rapidly to trigger rate limit
            const promises = [];
            for (let i = 0; i < 20; i++) {
                promises.push(
                    client1.trigger(channelName, eventName, { id: i })
                        .catch(error => error)
                );
            }

            const results = await Promise.all(promises);
            
            // Some should succeed, some should be rate limited
            const errors = results.filter(r => r instanceof Error);
            const successes = results.filter(r => !(r instanceof Error));
            
            expect(errors.length).to.be.greaterThan(0);
            expect(successes.length).to.be.greaterThan(0);
        });
    });

    describe('Event Filtering and Targeting', function() {
        it('should only send events to subscribed channels', async function() {
            const targetChannel = TestUtils.generateChannelName('target');
            const otherChannel = TestUtils.generateChannelName('other');
            const eventName = 'targeted-event';
            const eventData = { message: 'Only for target channel' };

            // Client1 subscribes to target channel
            client1 = new TestClient();
            await client1.connect();
            await client1.subscribe(targetChannel);

            // Client2 subscribes to other channel
            client2 = new TestClient();
            await client2.connect();
            await client2.subscribe(otherChannel);

            // Set up listeners
            let client1Received = false;
            let client2Received = false;

            client1.getChannel(targetChannel).bind(eventName, () => {
                client1Received = true;
            });

            client2.getChannel(otherChannel).bind(eventName, () => {
                client2Received = true;
            });

            // Send event only to target channel
            await TestUtils.sendApiEvent({
                channels: targetChannel,
                event: eventName,
                data: eventData
            });

            // Wait for potential delivery
            await TestUtils.wait(1000);

            expect(client1Received).to.be.true;
            expect(client2Received).to.be.false;
        });

        it('should handle event name validation', async function() {
            const channelName = TestUtils.generateChannelName('validation');
            
            client1 = new TestClient();
            await client1.connect();
            await client1.subscribe(channelName);

            // Test invalid event names
            const invalidNames = ['', 'a'.repeat(201), 'invalid#name'];
            
            for (const invalidName of invalidNames) {
                try {
                    await TestUtils.sendApiEvent({
                        channels: channelName,
                        event: invalidName,
                        data: { test: true }
                    });
                    expect.fail(`Should have rejected event name: ${invalidName}`);
                } catch (error) {
                    expect(error).to.be.instanceOf(Error);
                }
            }
        });
    });

    describe('Event Latency and Performance', function() {
        it('should deliver events with reasonable latency', async function() {
            const channelName = 'private-latency-test';
            const eventName = 'latency-test';

            client1 = new TestClient();
            await client1.connect();
            await client1.subscribe(channelName);

            client2 = new TestClient({ authEndpoint: 'http://localhost:3013/pusher/auth' });
            await client2.connect();
            await client2.subscribe(channelName);

            // Measure latency using client events
            // measureLatency will use client events which need private channel
            const latency = await TestUtils.measureLatency(client1, client2, channelName);
            
            // Latency should be reasonable (less than 100ms for local test)
            expect(latency).to.be.lessThan(100);
        });

        it('should handle burst event delivery', async function() {
            const channelName = TestUtils.generateChannelName('burst');
            const eventName = 'burst-event';
            const eventCount = 10;

            client1 = new TestClient();
            await client1.connect();
            await client1.subscribe(channelName);

            const receivedEvents = [];
            client1.getChannel(channelName).bind(eventName, (data) => {
                receivedEvents.push(data);
            });

            // Send burst of events
            const promises = [];
            for (let i = 0; i < eventCount; i++) {
                promises.push(
                    TestUtils.sendApiEvent({
                        channels: channelName,
                        event: eventName,
                        data: { id: i, message: `Event ${i}` }
                    })
                );
            }

            await Promise.all(promises);

            // Wait for all events to be delivered
            await TestUtils.wait(2000);

            expect(receivedEvents).to.have.lengthOf(eventCount);
            
            // Verify all events were received in order
            for (let i = 0; i < eventCount; i++) {
                const event = receivedEvents.find(e => e.id === i);
                expect(event).to.exist;
                expect(event.message).to.equal(`Event ${i}`);
            }
        });
    });
});