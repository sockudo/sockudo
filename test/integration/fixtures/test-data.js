// Test data fixtures for integration tests

module.exports = {
    // Test app configurations
    testApps: {
        default: {
            id: 'test-app',
            key: 'test-key',
            secret: 'test-secret',
            maxConnections: 100,
            maxClientEventsPerSecond: 10,
            enableClientMessages: true,
            enabled: true
        },
        
        limited: {
            id: 'limited-app',
            key: 'limited-key', 
            secret: 'limited-secret',
            maxConnections: 5,
            maxClientEventsPerSecond: 2,
            enableClientMessages: true,
            enabled: true
        }
    },

    // Sample user data for presence channels
    users: [
        {
            user_id: 'user-1',
            user_info: {
                name: 'Alice Johnson',
                email: 'alice@test.com',
                role: 'admin',
                avatar: 'https://example.com/avatars/alice.png'
            }
        },
        {
            user_id: 'user-2', 
            user_info: {
                name: 'Bob Smith',
                email: 'bob@test.com',
                role: 'user',
                avatar: 'https://example.com/avatars/bob.png'
            }
        },
        {
            user_id: 'user-3',
            user_info: {
                name: 'Carol Davis',
                email: 'carol@test.com',
                role: 'moderator'
            }
        }
    ],

    // Sample event data
    events: {
        simple: {
            message: 'Hello, World!',
            timestamp: Date.now()
        },
        
        complex: {
            type: 'notification',
            title: 'New Message',
            body: 'You have received a new message from Alice',
            data: {
                messageId: 'msg_123',
                senderId: 'user-1',
                threadId: 'thread_456'
            },
            actions: [
                { type: 'view', label: 'View Message' },
                { type: 'reply', label: 'Reply' }
            ]
        },
        
        largePayload: {
            type: 'bulk_data',
            items: Array.from({ length: 100 }, (_, i) => ({
                id: i,
                name: `Item ${i}`,
                description: `This is test item number ${i} with some additional content to make the payload larger`,
                metadata: {
                    created: new Date().toISOString(),
                    tags: ['test', 'data', `item-${i}`],
                    properties: {
                        color: ['red', 'green', 'blue'][i % 3],
                        size: ['small', 'medium', 'large'][i % 3],
                        priority: Math.floor(Math.random() * 10)
                    }
                }
            }))
        }
    },

    // Channel names for different test scenarios
    channels: {
        public: [
            'test-public-1',
            'test-public-2', 
            'notifications',
            'chat-general',
            'updates'
        ],
        
        private: [
            'private-test-1',
            'private-test-2',
            'private-user-notifications',
            'private-admin-panel',
            'private-secure-data'
        ],
        
        presence: [
            'presence-test-room',
            'presence-chat-room',
            'presence-game-lobby',
            'presence-workspace',
            'presence-meeting-room'
        ]
    },

    // Error test cases
    errors: {
        invalidChannelNames: [
            '', // empty
            'a'.repeat(201), // too long
            'channel with spaces',
            'channel#with#hashes',
            'channel@with@symbols',
            'channel\nwith\nnewlines'
        ],
        
        invalidEventNames: [
            '', // empty
            'a'.repeat(201), // too long
            'event with spaces',
            'event#with#hashes',
            'pusher:invalid', // reserved prefix
            'pusher_internal:invalid' // reserved prefix
        ]
    },

    // Rate limiting test data
    rateLimits: {
        // Burst of events to trigger rate limiting
        burstEvents: Array.from({ length: 25 }, (_, i) => ({
            name: `burst-event-${i}`,
            data: { 
                id: i, 
                message: `Burst event ${i}`,
                timestamp: Date.now()
            }
        })),
        
        // Gradual events within limits
        gradualEvents: Array.from({ length: 8 }, (_, i) => ({
            name: `gradual-event-${i}`,
            data: {
                id: i,
                message: `Gradual event ${i}`,
                delay: 200 // 200ms delay between events = 5 events/second
            }
        }))
    }
};