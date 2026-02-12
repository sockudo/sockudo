#!/bin/bash
# Run only the Sockudo integration tests (not sockudo-js internal tests)

echo "🧪 Running Sockudo Integration Tests"
echo "===================================="
echo ""

# Run only our test file, not the entire sockudo-js spec directory
bun test test-all.test.js "$@"
