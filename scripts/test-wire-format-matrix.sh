#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KOTLIN_DIR="$ROOT_DIR/client-sdks/sockudo-kotlin"
JDK21_HOME="/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home"

if [[ "${SOCKUDO_LIVE_TESTS:-}" != "1" ]]; then
  echo "Set SOCKUDO_LIVE_TESTS=1 to run live interoperability tests."
  exit 1
fi

echo "Assuming Sockudo is already running on http://127.0.0.1:6001 using config/config.toml."

for format in json messagepack protobuf; do
  echo ""
  echo "== Wire format: $format =="
  (
    cd "$ROOT_DIR/client-sdks/sockudo-js"
    SOCKUDO_WIRE_FORMAT="$format" bun run test
  )
  (
    cd "$ROOT_DIR/client-sdks/sockudo-flutter"
    SOCKUDO_WIRE_FORMAT="$format" flutter test
  )
  (
    cd "$KOTLIN_DIR"
    if [[ -d "$JDK21_HOME" ]]; then
      JAVA_HOME="$JDK21_HOME" PATH="$JDK21_HOME/bin:$PATH" SOCKUDO_WIRE_FORMAT="$format" ./gradlew test
    else
      SOCKUDO_WIRE_FORMAT="$format" ./gradlew test
    fi
  )
  (
    cd "$ROOT_DIR/client-sdks/sockudo-swift"
    SOCKUDO_WIRE_FORMAT="$format" swift test
  )
done
