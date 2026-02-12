#!/bin/bash

# Helper script to rebuild the Pusher client library with delta compression fixes
# and copy it to the public directory for testing

set -e

cd "$(dirname "$0")"

echo "🔨 Building Pusher client library with Vite..."
cd sockudo-js

# Build with Vite
npm run build

echo ""
echo "✅ Build complete!"
echo ""

# Copy to public directory
echo "📦 Copying build to public directory..."
cp dist/web/pusher.mjs ../public/pusher-local.js

echo "✅ Copied to ../public/pusher-local.js"
echo ""
echo "🎉 Done! You can now test at http://localhost:3000/delta-test-local.html"
echo ""
echo "Next steps:"
echo "  1. Start Sockudo server (in main project directory): cargo run"
echo "  2. Start test server (in test/interactive): npm start"
echo "  3. Open browser: http://localhost:3000/delta-test-local.html"
