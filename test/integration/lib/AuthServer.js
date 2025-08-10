const express = require('express');
const bodyParser = require('body-parser');
const crypto = require('crypto');

class AuthServer {
    constructor(config = {}) {
        this.app = express();
        this.port = config.port || process.env.AUTH_SERVER_PORT || 3005;
        this.host = config.host || process.env.AUTH_SERVER_HOST || 'localhost';
        this.appKey = config.appKey || process.env.TEST_APP_KEY || 'test-key';
        this.appSecret = config.appSecret || process.env.TEST_APP_SECRET || 'test-secret';
        this.server = null;
        
        // Track auth requests for testing
        this.authRequests = [];
        
        this.setupRoutes();
    }

    setupRoutes() {
        this.app.use(bodyParser.json());
        this.app.use(bodyParser.urlencoded({ extended: true }));

        // CORS middleware
        this.app.use((req, res, next) => {
            res.header('Access-Control-Allow-Origin', '*');
            res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
            res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
            if (req.method === 'OPTIONS') {
                return res.sendStatus(200);
            }
            next();
        });

        // Authentication endpoint
        this.app.post('/pusher/auth', (req, res) => {
            const { socket_id, channel_name } = req.body;
            
            // Track request
            this.authRequests.push({
                socket_id,
                channel_name,
                timestamp: new Date()
            });

            // Validate required parameters
            if (!socket_id || !channel_name) {
                return res.status(400).json({
                    error: 'Missing required parameters'
                });
            }

            // Check if channel requires auth
            if (!channel_name.startsWith('private-') && !channel_name.startsWith('presence-')) {
                return res.status(403).json({
                    error: 'Channel does not require authentication'
                });
            }

            // Generate auth signature
            const stringToSign = `${socket_id}:${channel_name}`;
            const auth = this.generateAuthSignature(stringToSign);

            const response = {
                auth: `${this.appKey}:${auth}`
            };

            // For presence channels, add user data
            if (channel_name.startsWith('presence-')) {
                const userId = socket_id.split('.')[0]; // Use first part of socket_id as user_id
                const userInfo = {
                    user_id: userId,
                    user_info: {
                        name: `User ${userId}`,
                        email: `user${userId}@test.com`,
                        role: 'tester'
                    }
                };
                
                const presenceData = JSON.stringify(userInfo);
                const presenceStringToSign = `${socket_id}:${channel_name}:${presenceData}`;
                const presenceAuth = this.generateAuthSignature(presenceStringToSign);
                
                response.auth = `${this.appKey}:${presenceAuth}`;
                response.channel_data = presenceData;
            }

            res.json(response);
        });

        // User authentication endpoint (for user authentication feature)
        this.app.post('/pusher/user-auth', (req, res) => {
            const { socket_id } = req.body;
            
            if (!socket_id) {
                return res.status(400).json({
                    error: 'Missing socket_id'
                });
            }

            const userId = socket_id.split('.')[0];
            const userData = {
                id: userId,
                watchlist: ['user-1', 'user-2'],
                info: {
                    name: `User ${userId}`,
                    role: 'tester'
                }
            };

            const userDataJson = JSON.stringify(userData);
            const auth = this.generateAuthSignature(`${socket_id}::user::${userDataJson}`);

            res.json({
                auth: `${this.appKey}:${auth}`,
                user_data: userDataJson
            });
        });

        // Health check endpoint
        this.app.get('/health', (req, res) => {
            res.json({ status: 'ok', timestamp: new Date() });
        });

        // Get auth request history (for testing)
        this.app.get('/auth-requests', (req, res) => {
            res.json(this.authRequests);
        });

        // Clear auth request history
        this.app.delete('/auth-requests', (req, res) => {
            this.authRequests = [];
            res.json({ message: 'Auth requests cleared' });
        });
    }

    generateAuthSignature(stringToSign) {
        return crypto
            .createHmac('sha256', this.appSecret)
            .update(stringToSign)
            .digest('hex');
    }

    async start() {
        return new Promise((resolve) => {
            this.server = this.app.listen(this.port, this.host, () => {
                console.log(`Auth server running on http://${this.host}:${this.port}`);
                resolve();
            });
        });
    }

    async stop() {
        return new Promise((resolve) => {
            if (this.server) {
                this.server.close(() => {
                    console.log('Auth server stopped');
                    resolve();
                });
            } else {
                resolve();
            }
        });
    }

    getAuthRequests() {
        return this.authRequests;
    }

    clearAuthRequests() {
        this.authRequests = [];
    }
}

// If run directly, start the server
if (require.main === module) {
    const server = new AuthServer();
    server.start().catch(console.error);
    
    // Handle graceful shutdown
    process.on('SIGINT', async () => {
        await server.stop();
        process.exit(0);
    });
}

module.exports = AuthServer;