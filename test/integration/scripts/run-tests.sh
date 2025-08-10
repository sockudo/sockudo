#!/bin/bash

# Exit on error
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting Sockudo Integration Tests${NC}"

# Parse arguments
TEST_ENV="${1:-local}"
TEST_FILTER="${2:-}"

# Function to cleanup on exit
cleanup() {
    echo -e "${YELLOW}Cleaning up test environment...${NC}"
    if [ "$TEST_ENV" = "docker" ]; then
        docker-compose -f ../../docker-compose.test.yml down -v
    fi
}

# Set trap for cleanup
trap cleanup EXIT

# Change to integration test directory
cd "$(dirname "$0")/.."

# Install dependencies if needed
if [ ! -d "node_modules" ]; then
    echo -e "${YELLOW}Installing test dependencies...${NC}"
    npm install
fi

# Copy .env.example to .env if it doesn't exist
if [ ! -f ".env" ]; then
    cp .env.example .env
fi

if [ "$TEST_ENV" = "docker" ]; then
    echo -e "${GREEN}Starting Docker test environment...${NC}"
    
    # Start the test environment
    docker-compose -f ../../docker-compose.test.yml up -d --build
    
    # Wait for services to be ready
    echo -e "${YELLOW}Waiting for services to be ready...${NC}"
    
    # Wait for Sockudo
    max_attempts=30
    attempt=0
    while [ $attempt -lt $max_attempts ]; do
        if curl -f http://localhost:6005/up/test-app > /dev/null 2>&1; then
            echo -e "${GREEN}Sockudo is ready${NC}"
            break
        fi
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    if [ $attempt -eq $max_attempts ]; then
        echo -e "${RED}Sockudo failed to start${NC}"
        exit 1
    fi
    
    # Wait for Auth Server
    attempt=0
    while [ $attempt -lt $max_attempts ]; do
        if curl -f http://localhost:3005/health > /dev/null 2>&1; then
            echo -e "${GREEN}Auth server is ready${NC}"
            break
        fi
        echo -n "."
        sleep 1
        attempt=$((attempt + 1))
    done
    
    if [ $attempt -eq $max_attempts ]; then
        echo -e "${RED}Auth server failed to start${NC}"
        exit 1
    fi
fi

# Run the tests
echo -e "${GREEN}Running tests...${NC}"

if [ "$TEST_ENV" = "docker" ]; then
    # For CI, generate JSON output
    if [ -n "$TEST_FILTER" ]; then
        npm run test:ci -- --grep "$TEST_FILTER"
    else
        npm run test:ci
    fi
else
    # For local development - same as normal mode
    if [ -n "$TEST_FILTER" ]; then
        npm test -- --grep "$TEST_FILTER"
    else
        npm test
    fi
fi

TEST_EXIT_CODE=$?

if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}All tests passed!${NC}"
else
    echo -e "${RED}Some tests failed${NC}"
fi

exit $TEST_EXIT_CODE