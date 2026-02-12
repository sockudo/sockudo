#!/bin/bash

# Subscription Profiling Script with Flamegraph
# Profiles Sockudo during k6-burst.js benchmark to identify bottlenecks

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Sockudo Subscription Profiling ===${NC}"
echo ""

# Check dependencies
echo -e "${YELLOW}Checking dependencies...${NC}"

if ! command -v cargo &> /dev/null; then
    echo -e "${RED}Error: cargo not found${NC}"
    exit 1
fi

if ! command -v k6 &> /dev/null; then
    echo -e "${RED}Error: k6 not found. Install from https://k6.io/docs/get-started/installation/${NC}"
    exit 1
fi

if ! command -v cargo-flamegraph &> /dev/null; then
    echo -e "${YELLOW}Installing cargo-flamegraph...${NC}"
    cargo install flamegraph
fi

# Build with release + debug symbols
echo -e "${YELLOW}Building Sockudo with profiling symbols...${NC}"
cargo build --release --features local

# Create profiling output directory
PROFILE_DIR="profile-results-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$PROFILE_DIR"

echo -e "${GREEN}Profile results will be saved to: $PROFILE_DIR${NC}"
echo ""

# Start Sockudo with flamegraph
echo -e "${YELLOW}Starting Sockudo with flamegraph profiling...${NC}"
echo "Press Ctrl+C after k6 test completes to generate flamegraph"
echo ""

# Run Sockudo in background with flamegraph
cargo flamegraph --release --features local -o "$PROFILE_DIR/subscription-flamegraph.svg" -- --config config/config.json &
SOCKUDO_PID=$!

# Wait for Sockudo to start
echo -e "${YELLOW}Waiting for Sockudo to start...${NC}"
sleep 5

# Check if Sockudo is running
if ! kill -0 $SOCKUDO_PID 2>/dev/null; then
    echo -e "${RED}Error: Sockudo failed to start${NC}"
    exit 1
fi

echo -e "${GREEN}Sockudo started (PID: $SOCKUDO_PID)${NC}"
echo ""

# Run k6 benchmark
echo -e "${YELLOW}Running k6 burst test (10,000 concurrent subscriptions)...${NC}"
cd benchmark
k6 run k6-burst.js --out json="../$PROFILE_DIR/k6-results.json" 2>&1 | tee "../$PROFILE_DIR/k6-output.txt"
cd ..

echo ""
echo -e "${GREEN}k6 test completed!${NC}"
echo -e "${YELLOW}Waiting 5 seconds for remaining operations...${NC}"
sleep 5

# Stop Sockudo to generate flamegraph
echo -e "${YELLOW}Stopping Sockudo and generating flamegraph...${NC}"
kill -SIGINT $SOCKUDO_PID
wait $SOCKUDO_PID 2>/dev/null || true

echo ""
echo -e "${GREEN}=== Profiling Complete ===${NC}"
echo ""
echo -e "${GREEN}Results saved to: $PROFILE_DIR/${NC}"
echo -e "  - subscription-flamegraph.svg (CPU flamegraph)"
echo -e "  - k6-results.json (detailed k6 metrics)"
echo -e "  - k6-output.txt (k6 console output)"
echo ""
echo -e "${YELLOW}Open the flamegraph:${NC}"
echo -e "  xdg-open $PROFILE_DIR/subscription-flamegraph.svg  # Linux"
echo -e "  open $PROFILE_DIR/subscription-flamegraph.svg      # macOS"
echo -e "  start $PROFILE_DIR/subscription-flamegraph.svg     # Windows"
echo ""
echo -e "${YELLOW}Analyze the flamegraph to find hot paths in:${NC}"
echo -e "  - handle_subscribe_request"
echo -e "  - execute_subscription"
echo -e "  - ChannelManager::subscribe"
echo -e "  - ConnectionManager operations"
echo ""
