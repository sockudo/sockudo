#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PROJECT="$ROOT_DIR/tools/macos-push-probe/SockudoMacPushProbe.xcodeproj"
SCHEME="SockudoMacPushProbe"
DERIVED_DATA="$ROOT_DIR/.macos-push-probe/DerivedData"

BUNDLE_ID="${BUNDLE_ID:-com.sockudo.macosprobe}"
SIGNING_TEAM="${SIGNING_TEAM:-}"

args=(
  -allowProvisioningUpdates
  -allowProvisioningDeviceRegistration
  -project "$PROJECT"
  -scheme "$SCHEME"
  -configuration Debug
  -derivedDataPath "$DERIVED_DATA"
  -destination "platform=macOS"
  PRODUCT_BUNDLE_IDENTIFIER="$BUNDLE_ID"
)

if [[ -n "$SIGNING_TEAM" ]]; then
  args+=(DEVELOPMENT_TEAM="$SIGNING_TEAM")
fi

xcodebuild "${args[@]}" build
open "$DERIVED_DATA/Build/Products/Debug/SockudoMacPushProbe.app"
