#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PROJECT="$SCRIPT_DIR/SockudoAPNSProbe.xcodeproj"
DERIVED_DATA="$REPO_ROOT/.apns-e2e/DerivedData"

BUNDLE_ID="${BUNDLE_ID:-com.sockudo.apnsprobe}"
DEVELOPMENT_TEAM="${DEVELOPMENT_TEAM:-69QGT49729}"
DEVICE_ID="${DEVICE_ID:-}"
BRIDGE_URL="${BRIDGE_URL:-}"

if [[ -z "$DEVICE_ID" ]]; then
  DEVICE_ID="$(xcrun xctrace list devices 2>/dev/null | awk '/iPhone - Radu/ {print $NF; exit}' | tr -d '()')"
fi
if [[ -z "$DEVICE_ID" ]]; then
  DEVICE_ID="$(xcrun xctrace list devices 2>/dev/null | awk '/iPhone .*\\([0-9A-F-]+\\)$/ {print $NF; exit}' | tr -d '()')"
fi
if [[ -z "$DEVICE_ID" ]]; then
  echo "No physical iPhone found. Connect and trust an iPhone, or set DEVICE_ID." >&2
  exit 2
fi

if [[ -z "$BRIDGE_URL" ]]; then
  HOST_IP="$(ipconfig getifaddr en0 2>/dev/null || ipconfig getifaddr en1 2>/dev/null || true)"
  if [[ -z "$HOST_IP" ]]; then
    echo "Could not determine Mac LAN IP. Set BRIDGE_URL=http://<mac-ip>:8765." >&2
    exit 2
  fi
  BRIDGE_URL="http://${HOST_IP}:8765"
fi

echo "Bundle ID / APNs topic: $BUNDLE_ID"
echo "Development team: $DEVELOPMENT_TEAM"
echo "Device: $DEVICE_ID"
echo "Bridge URL: $BRIDGE_URL"

xcodebuild \
  -project "$PROJECT" \
  -scheme SockudoAPNSProbe \
  -configuration Debug \
  -destination "id=$DEVICE_ID" \
  -derivedDataPath "$DERIVED_DATA" \
  -allowProvisioningUpdates \
  DEVELOPMENT_TEAM="$DEVELOPMENT_TEAM" \
  PRODUCT_BUNDLE_IDENTIFIER="$BUNDLE_ID" \
  SOCKUDO_APNS_BRIDGE_URL="$BRIDGE_URL" \
  build

APP_PATH="$DERIVED_DATA/Build/Products/Debug-iphoneos/SockudoAPNSProbe.app"
xcrun devicectl device install app --device "$DEVICE_ID" "$APP_PATH"
xcrun devicectl device process launch --device "$DEVICE_ID" --terminate-existing "$BUNDLE_ID"
