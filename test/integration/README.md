# Sockudo Integration Tests

Comprehensive integration test suite for the Sockudo WebSocket server that tests real-world functionality end-to-end.

## Overview

These integration tests verify that Sockudo functions correctly as a complete system by:
- Testing actual WebSocket connections using Pusher JS SDK
- Verifying all channel types (public, private, presence, private-encrypted)
- Testing authentication flows with real HMAC signatures
- Validating quota enforcement and rate limiting
- Testing event broadcasting and delivery
- Verifying horizontal scaling capabilities

## Test Coverage

### 🔌 Connection Tests
- Basic WebSocket connection/disconnection
- Invalid credentials handling
- Reconnection after network issues
- Multiple simultaneous connections
- Connection limits enforcement
- Connection persistence and ping/pong

### 📺 Channel Subscription Tests
- Public channel subscription/unsubscription
- Private channel authentication
- Presence channel member tracking
- Channel name validation
- Channel limits enforcement
- Duplicate subscription handling

### 🔐 Authentication Tests
- Private channel HMAC authentication
- Presence channel user data handling
- Invalid signature rejection
- Authentication timeout handling
- Auth endpoint error scenarios
- Security validation

### 📡 Event Broadcasting Tests
- HTTP API event delivery
- Client event broadcasting (private/presence only)
- Multi-channel broadcasting
- Event filtering and targeting
- Large payload handling
- Event latency measurement

### ⚡ Quota and Limits Tests
- Max connections per app enforcement
- Client event rate limiting
- API request rate limiting
- Channel limits per connection
- Presence member limits
- Message size limits
- Resource cleanup verification

## Architecture

```
test/integration/
├── lib/                    # Test helper library
│   ├── TestClient.js      # Pusher JS wrapper for testing
│   ├── AuthServer.js      # Mock auth server for private/presence
│   └── TestUtils.js       # Common utilities and helpers
├── tests/                  # Test suites
│   ├── connection.test.js # Connection lifecycle tests
│   ├── channels.test.js   # Channel subscription tests
│   ├── auth.test.js       # Authentication flow tests
│   ├── events.test.js     # Event broadcasting tests
│   └── quotas.test.js     # Limits and quota tests
├── fixtures/               # Test data and configurations
│   └── test-data.js       # Sample data for tests
├── scripts/               # Test execution scripts
│   └── run-tests.sh       # Main test runner script
└── package.json           # Test dependencies and scripts
```

## Running Tests

### Prerequisites
- Docker and Docker Compose
- Node.js 18+ (for local development)
- Make (for convenience commands)

### Quick Start
```bash
# Run all integration tests with Docker
make test-integration

# Run tests locally (requires Sockudo server running)
make test-integration-local

# Run specific test suite
cd test/integration
npm test -- --grep "Connection Tests"

# Run tests in watch mode
make test-integration-watch

# Generate coverage report
make test-integration-coverage
```

### Test Environments

**Docker Environment (Recommended)**
- Uses isolated ports (6005, 3005, 6385)
- Full test environment in containers
- Consistent across different systems
- Used by CI/CD pipeline

**Local Environment**
- Requires local Sockudo server
- Faster test execution
- Better for development/debugging
- Uses default ports (6001, 3001, 6379)

## Configuration

### Environment Variables
```bash
# Sockudo server configuration
SOCKUDO_HOST=localhost
SOCKUDO_PORT=6005
SOCKUDO_SSL=false

# Test app credentials
TEST_APP_ID=test-app
TEST_APP_KEY=test-key
TEST_APP_SECRET=test-secret

# Auth server configuration
AUTH_SERVER_PORT=3005
AUTH_SERVER_HOST=localhost

# Test behavior
TEST_TIMEOUT=10000
TEST_VERBOSE=false
```

### Docker Configuration
The test environment uses `docker-compose.test.yml` which includes:
- Sockudo server with test configuration
- Redis for message brokering and caching
- Auth server for private/presence channel testing
- Isolated network and non-conflicting ports

## Test Helpers

### TestClient
Wrapper around Pusher JS client with testing conveniences:
```javascript
const client = new TestClient();
await client.connect();
await client.subscribe('test-channel');
await client.trigger('private-channel', 'client-event', data);
const members = client.getPresenceMembers('presence-channel');
```

### AuthServer
Mock authentication server for testing private/presence channels:
```javascript
const authServer = new AuthServer({ port: 3011 });
await authServer.start();
const requests = authServer.getAuthRequests();
```

### TestUtils
Common utilities for testing:
```javascript
await TestUtils.sendApiEvent({ channel: 'test', event: 'api-event', data });
const latency = await TestUtils.measureLatency(client1, client2, 'channel');
const clients = await TestUtils.createClients(5);
```

## CI Integration

Integration tests are automatically run on:
- Pull requests to main branches
- Pushes to main branches
- Manual workflow dispatch

### GitHub Actions Features
- Parallel test execution
- Test result reporting in PR comments
- Performance benchmark tracking
- Artifact collection (coverage, logs)
- Failure notifications

### Status Checks
- All tests must pass for PR approval
- Test results posted as PR comments
- Performance benchmarks on PRs
- Coverage reports generated

## Development

### Adding New Tests
1. Create test file in `tests/` directory
2. Use existing helpers (TestClient, TestUtils)
3. Follow naming convention: `feature.test.js`
4. Add appropriate timeouts for async operations
5. Clean up resources in `afterEach`/`after` hooks

### Debugging Tests
```bash
# Run single test with verbose output
npm test -- --grep "specific test name"

# Check Docker logs
docker logs sockudo-test-server
docker logs sockudo-auth-test

# Run auth server locally for debugging
node lib/AuthServer.js
```

### Test Data
Use fixtures for consistent test data:
```javascript
const testData = require('../fixtures/test-data');
const eventData = testData.events.simple;
const channelName = testData.channels.public[0];
```

## Troubleshooting

### Common Issues

**Port Conflicts**
- Tests use ports 6005, 3005, 6385 to avoid conflicts
- Check for processes using these ports: `lsof -i :6005`

**Docker Issues**
- Clear Docker cache: `docker system prune`
- Rebuild images: `make test-integration` (rebuilds automatically)

**Test Timeouts**
- Increase timeout in test file: `this.timeout(20000)`
- Check server logs for connection issues
- Verify network connectivity in Docker

**Authentication Failures**
- Verify HMAC signature generation in AuthServer
- Check app credentials match between client and server
- Ensure auth endpoint is accessible

### Performance
- Tests run in ~30-45 seconds
- Docker build cached for faster subsequent runs
- Parallel execution where possible
- Resource cleanup to prevent memory leaks

## Metrics

The integration tests validate these key metrics:
- **Connection Speed**: < 1 second for WebSocket handshake
- **Event Latency**: < 100ms for local delivery
- **Concurrent Connections**: Up to configured limits
- **Message Throughput**: Handles burst traffic
- **Resource Usage**: Proper cleanup after tests

This comprehensive test suite provides confidence that Sockudo works correctly in real-world scenarios and helps prevent regressions during development.