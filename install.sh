#!/bin/sh

set -eu

RELEASES_URL="https://github.com/sockudo/sockudo/releases"
VERSION=${SOCKUDO_VERSION:-}
BIN_DIR=${SOCKUDO_INSTALL_DIR:-}

say() {
  printf '%s\n' "$*"
}

fail() {
  say "sockudo installer: $*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Install a released Sockudo binary on Linux.

Usage:
  install.sh [--version <version>] [--bin-dir <directory>]

Options:
  --version <version>    Install a specific release (for example, 5.2.0).
  --bin-dir <directory>  Install into this directory.
  -h, --help             Show this help.

Environment:
  SOCKUDO_VERSION        Same as --version.
  SOCKUDO_INSTALL_DIR    Same as --bin-dir.

The default installation directory is $HOME/.local/bin.
EOF
}

need_value() {
  if [ "$#" -lt 2 ]; then
    fail "$1 requires a value"
  fi
  if [ -z "$2" ]; then
    fail "$1 requires a value"
  fi
  case "$2" in
    -*) fail "$1 requires a value" ;;
  esac
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --version)
      need_value "$@"
      VERSION=$2
      shift 2
      ;;
    --version=*)
      VERSION=${1#*=}
      shift
      ;;
    --bin-dir)
      need_value "$@"
      BIN_DIR=$2
      shift 2
      ;;
    --bin-dir=*)
      BIN_DIR=${1#*=}
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      fail "unknown option: $1"
      ;;
  esac
done

command -v curl >/dev/null 2>&1 || fail "curl is required"
command -v tar >/dev/null 2>&1 || fail "tar is required"
command -v install >/dev/null 2>&1 || fail "install is required"

[ "$(uname -s)" = "Linux" ] || fail "prebuilt binaries are available only for Linux; use 'cargo install sockudo' on other platforms"

case "$(uname -m)" in
  x86_64 | amd64)
    ARCH=x86_64
    ;;
  aarch64 | arm64)
    ARCH=aarch64
    ;;
  *)
    fail "unsupported Linux architecture: $(uname -m)"
    ;;
esac

LIBC=gnu
if [ -f /etc/alpine-release ] || (command -v ldd >/dev/null 2>&1 && ldd --version 2>&1 | grep -qi musl); then
  LIBC=musl
fi
TARGET="${ARCH}-unknown-linux-${LIBC}"

if [ -z "$VERSION" ] || [ "$VERSION" = "latest" ]; then
  LATEST_URL=$(curl --proto '=https' --tlsv1.2 --fail --location --silent --show-error \
    --output /dev/null --write-out '%{url_effective}' "${RELEASES_URL}/latest")
  case "$LATEST_URL" in
    */tag/v*) VERSION=${LATEST_URL##*/v} ;;
    *) fail "could not determine the latest release version" ;;
  esac
fi
VERSION=${VERSION#v}

case "$VERSION" in
  '' | [!0-9]* | *[!0-9A-Za-z.+-]*)
    fail "invalid release version: $VERSION"
    ;;
esac

if [ -z "$BIN_DIR" ]; then
  [ -n "${HOME:-}" ] || fail "HOME is not set; pass --bin-dir"
  BIN_DIR="$HOME/.local/bin"
fi

ARCHIVE="sockudo-v${VERSION}-${TARGET}.tgz"
DOWNLOAD_URL="${RELEASES_URL}/download/v${VERSION}/${ARCHIVE}"

TMP_DIR=$(mktemp -d 2>/dev/null || mktemp -d -t sockudo-install) || fail "could not create a temporary directory"
trap 'rm -rf "$TMP_DIR"' EXIT HUP INT TERM

say "Downloading Sockudo ${VERSION} for ${TARGET}..."
curl --proto '=https' --tlsv1.2 --fail --location --silent --show-error \
  --output "$TMP_DIR/$ARCHIVE" "$DOWNLOAD_URL"
curl --proto '=https' --tlsv1.2 --fail --location --silent --show-error \
  --output "$TMP_DIR/${ARCHIVE}.sha256" "${DOWNLOAD_URL}.sha256"

if command -v sha256sum >/dev/null 2>&1; then
  (cd "$TMP_DIR" && sha256sum -c "${ARCHIVE}.sha256")
elif command -v shasum >/dev/null 2>&1; then
  (cd "$TMP_DIR" && shasum -a 256 -c "${ARCHIVE}.sha256")
else
  fail "sha256sum or shasum is required to verify the download"
fi

tar -xzf "$TMP_DIR/$ARCHIVE" -C "$TMP_DIR"
[ -f "$TMP_DIR/sockudo" ] || fail "release archive does not contain the sockudo binary"

mkdir -p "$BIN_DIR" || fail "could not create $BIN_DIR"
install -m 0755 "$TMP_DIR/sockudo" "$BIN_DIR/sockudo" || fail "could not install to $BIN_DIR; choose a writable --bin-dir"

say "Installed Sockudo ${VERSION} to $BIN_DIR/sockudo"
case ":${PATH:-}:" in
  *:"$BIN_DIR":*) ;;
  *) say "Add $BIN_DIR to PATH to run 'sockudo' from any directory." ;;
esac
