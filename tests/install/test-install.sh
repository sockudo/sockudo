#!/bin/sh

# tests for install.sh.
# mocked curl/platform commands. No network
# requests or changes to the host's ~/.local/bin are made.

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
INSTALLER=${1:-"$REPO_ROOT/install.sh"}

if [ ! -f "$INSTALLER" ]; then
  printf 'test error: installer not found: %s\n' "$INSTALLER" >&2
  exit 2
fi

REAL_SH=$(command -v sh)
REAL_MKTEMP=$(command -v mktemp)
REAL_MKDIR=$(command -v mkdir)
REAL_RM=$(command -v rm)
REAL_CP=$(command -v cp)
REAL_TAR=$(command -v tar)
REAL_GZIP=$(command -v gzip)
REAL_GREP=$(command -v grep)
REAL_INSTALL=$(command -v install)
REAL_MV=$(command -v mv)
REAL_SHA256SUM=$(command -v sha256sum)
REAL_AWK=$(command -v awk)
REAL_CAT=$(command -v cat)
REAL_CHMOD=$(command -v chmod)
REAL_CMP=$(command -v cmp)
REAL_SLEEP=$(command -v sleep)

WORK=$($REAL_MKTEMP -d "${TMPDIR:-/tmp}/sockudo-installer-tests.XXXXXX")
MOCK_BIN="$WORK/mock-bin"
"$REAL_MKDIR" -p "$MOCK_BIN"
FIXTURE_DIR="$WORK/fixtures"
GOOD_DIR="$FIXTURE_DIR/good"
MISSING_DIR="$FIXTURE_DIR/missing"
NONEXEC_DIR="$FIXTURE_DIR/nonexec"
GOOD_ARCHIVE="$FIXTURE_DIR/good.tgz"
MISSING_ARCHIVE="$FIXTURE_DIR/missing.tgz"
NONEXEC_ARCHIVE="$FIXTURE_DIR/nonexec.tgz"
CORRUPT_ARCHIVE="$FIXTURE_DIR/corrupt.tgz"

TEST_COUNT=0
PASS_COUNT=0
FAIL_COUNT=0
CASE_NUMBER=0

cleanup_tests() {
  "$REAL_RM" -rf "$WORK"
}
trap cleanup_tests 0
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

pass() {
  TEST_COUNT=$((TEST_COUNT + 1))
  PASS_COUNT=$((PASS_COUNT + 1))

  printf 'ok %s - %s\n' "$TEST_COUNT" "$1"
}

fail_test() {
  TEST_COUNT=$((TEST_COUNT + 1))
  FAIL_COUNT=$((FAIL_COUNT + 1))

  printf 'not ok %s - %s\n' "$TEST_COUNT" "$1"

  if [ -n "${OUT:-}" ] && [ -s "$OUT" ]; then
    printf '# stdout:\n'

    while IFS= read -r line || [ -n "$line" ]; do
      printf '#   %s\n' "$line"
    done < "$OUT"
  fi

  if [ -n "${ERR:-}" ] && [ -s "$ERR" ]; then
    printf '# stderr:\n'

    while IFS= read -r line || [ -n "$line" ]; do
      printf '#   %s\n' "$line"
    done < "$ERR"
  fi
}

section() {
  printf '\n# %s\n' "$1"
}

assert_contains() {
  expected=$1
  file=$2

  "$REAL_GREP" -F -e "$expected" "$file" >/dev/null 2>&1
}

assert_not_contains() {
  unexpected=$1
  file=$2

  if "$REAL_GREP" -F -e "$unexpected" "$file" >/dev/null 2>&1; then
    return 1
  fi

  return 0
}

new_case() {
  CASE_NUMBER=$((CASE_NUMBER + 1))
  CASE_DIR="$WORK/case-$CASE_NUMBER"
  CASE_HOME="$CASE_DIR/home"
  CASE_BIN="$CASE_DIR/bin"
  OUT="$CASE_DIR/stdout"
  ERR="$CASE_DIR/stderr"
  CURL_LOG="$CASE_DIR/curl.log"
  DOWNLOAD_DIR_FILE="$CASE_DIR/download-dir"
  CURL_PID_FILE="$CASE_DIR/curl.pid"

  "$REAL_MKDIR" -p "$CASE_HOME" "$CASE_BIN"
  : > "$OUT"
  : > "$ERR"
  : > "$CURL_LOG"
  : > "$DOWNLOAD_DIR_FILE"
  : > "$CURL_PID_FILE"

  TEST_PATH="$FULL_PATH"
  TEST_HOME=$CASE_HOME
  TEST_VERSION=
  TEST_ENV_BIN_DIR=
  MOCK_UNAME_S=Linux
  MOCK_UNAME_M=x86_64
  MOCK_LDD_OUTPUT='GNU libc'
  MOCK_LATEST_MODE=valid
  MOCK_LATEST_VERSION=5.2.0
  MOCK_ARCHIVE_MODE=good
  MOCK_CURL_FAIL=0
  MOCK_CURL_FAIL_DOWNLOAD=0
  MOCK_CURL_FAIL_CHECKSUM=0
  MOCK_CURL_BLOCK=0
  MOCK_INSTALL_FAIL=0
  MOCK_MV_FAIL=0
  MOCK_BAD_CHECKSUM=0
}

run_installer() {
  : > "$OUT"
  : > "$ERR"
  : > "$CURL_LOG"
  : > "$DOWNLOAD_DIR_FILE"
  : > "$CURL_PID_FILE"

  PATH="$TEST_PATH" \
  HOME="$TEST_HOME" \
  SOCKUDO_VERSION="$TEST_VERSION" \
  SOCKUDO_INSTALL_DIR="$TEST_ENV_BIN_DIR" \
  MOCK_CURL_LOG="$CURL_LOG" \
  MOCK_DOWNLOAD_DIR_FILE="$DOWNLOAD_DIR_FILE" \
  MOCK_CURL_PID_FILE="$CURL_PID_FILE" \
  MOCK_ARCHIVE_GOOD="$GOOD_ARCHIVE" \
  MOCK_ARCHIVE_MISSING="$MISSING_ARCHIVE" \
  MOCK_ARCHIVE_NONEXEC="$NONEXEC_ARCHIVE" \
  MOCK_ARCHIVE_CORRUPT="$CORRUPT_ARCHIVE" \
  MOCK_ARCHIVE_MODE="$MOCK_ARCHIVE_MODE" \
  MOCK_CURL_FAIL="$MOCK_CURL_FAIL" \
  MOCK_CURL_FAIL_DOWNLOAD="$MOCK_CURL_FAIL_DOWNLOAD" \
  MOCK_CURL_FAIL_CHECKSUM="$MOCK_CURL_FAIL_CHECKSUM" \
  MOCK_CURL_BLOCK="$MOCK_CURL_BLOCK" \
  MOCK_LATEST_MODE="$MOCK_LATEST_MODE" \
  MOCK_LATEST_VERSION="$MOCK_LATEST_VERSION" \
  MOCK_UNAME_S="$MOCK_UNAME_S" \
  MOCK_UNAME_M="$MOCK_UNAME_M" \
  MOCK_LDD_OUTPUT="$MOCK_LDD_OUTPUT" \
  MOCK_INSTALL_FAIL="$MOCK_INSTALL_FAIL" \
  MOCK_MV_FAIL="$MOCK_MV_FAIL" \
  MOCK_BAD_CHECKSUM="$MOCK_BAD_CHECKSUM" \
  REAL_CP="$REAL_CP" \
  REAL_SHA256SUM="$REAL_SHA256SUM" \
  REAL_AWK="$REAL_AWK" \
  REAL_INSTALL="$REAL_INSTALL" \
  REAL_MV="$REAL_MV" \
  REAL_SLEEP="$REAL_SLEEP" \
    "$REAL_SH" "$INSTALLER" "$@" > "$OUT" 2> "$ERR"
}

expect_success() {
  label=$1
  shift

  if run_installer "$@"; then
    pass "$label"
  else
    status=$?
    fail_test "$label (expected success, got exit status $status)"
  fi
}

expect_failure() {
  label=$1
  expected=$2
  shift 2

  if run_installer "$@"; then
    fail_test "$label (expected a nonzero exit status)"
    return
  else
    status=$?
  fi

  if [ -n "$expected" ] &&
    ! assert_contains "$expected" "$ERR" &&
    ! assert_contains "$expected" "$OUT"; then
    fail_test "$label (expected output containing: $expected, got exit status $status)"
    return
  fi

  pass "$label (exit status $status)"
}

assert_no_temp_files() {
  dir=$1
  for candidate in "$dir"/.sockudo.*; do
    [ -e "$candidate" ] && return 1
  done
  return 0
}

assert_download_dir_cleaned() {
  [ -s "$DOWNLOAD_DIR_FILE" ] || return 1
  download_dir=$($REAL_CAT "$DOWNLOAD_DIR_FILE")
  [ -n "$download_dir" ] && [ ! -e "$download_dir" ]
}

make_path() {
  path_dir=$1
  missing_command=${2:-}
  "$REAL_MKDIR" -p "$path_dir"

  # Mocked platform and network commands.
  for command_name in curl uname ldd; do
    [ "$command_name" = "$missing_command" ] && continue
    ln -sf "$MOCK_BIN/$command_name" "$path_dir/$command_name"
  done

  if [ "$missing_command" != install ]; then
    ln -sf "$MOCK_BIN/install" "$path_dir/install"
  fi
  if [ "$missing_command" != mv ]; then
    ln -sf "$MOCK_BIN/mv" "$path_dir/mv"
  fi

  for command_name in tar gzip grep mktemp mkdir rm sha256sum cat; do
    [ "$command_name" = "$missing_command" ] && continue
    [ "$missing_command" = checksum-tools ] && [ "$command_name" = sha256sum ] && continue
    case "$command_name" in
      tar) real_command=$REAL_TAR ;;
      gzip) real_command=$REAL_GZIP ;;
      grep) real_command=$REAL_GREP ;;
      mktemp) real_command=$REAL_MKTEMP ;;
      mkdir) real_command=$REAL_MKDIR ;;
      rm) real_command=$REAL_RM ;;
      sha256sum) real_command=$REAL_SHA256SUM ;;
      cat) real_command=$REAL_CAT ;;
    esac
    ln -sf "$real_command" "$path_dir/$command_name"
  done

  if [ "$missing_command" != shasum ] && [ "$missing_command" != checksum-tools ]; then
    ln -sf "$MOCK_BIN/shasum" "$path_dir/shasum"
  fi
}

# Mock curl understands only the options used by install.sh. It copies local
# fixture archives and emits a matching checksum, so tests never access the net.
cat > "$MOCK_BIN/curl" <<'MOCK_CURL'
#!/bin/sh
set -eu

OUTPUT=
WRITE_OUT=
URL=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --output)
      OUTPUT=$2
      shift 2
      ;;
    --write-out)
      WRITE_OUT=$2
      shift 2
      ;;
    --proto)
      shift 2
      ;;
    --tlsv1.2 | --fail | --location | --silent | --show-error)
      shift
      ;;
    *)
      URL=$1
      shift
      ;;
  esac
done

printf '%s\n' "$URL" >> "$MOCK_CURL_LOG"

if [ "$MOCK_CURL_FAIL" = 1 ]; then
  printf '%s\n' 'mock curl: simulated request failure' >&2
  exit 22
fi

if [ -n "$WRITE_OUT" ]; then
  if [ "$MOCK_LATEST_MODE" = invalid ]; then
    printf '%s' 'https://github.com/sockudo/sockudo/releases'
  else
    printf 'https://github.com/sockudo/sockudo/releases/tag/v%s' "$MOCK_LATEST_VERSION"
  fi
  exit 0
fi

[ -n "$OUTPUT" ] || {
  printf '%s\n' 'mock curl: missing --output' >&2
  exit 2
}
printf '%s\n' "${OUTPUT%/*}" > "$MOCK_DOWNLOAD_DIR_FILE"

case "$URL" in
  *.sha256)
    [ "$MOCK_CURL_FAIL_DOWNLOAD" != 1 ] || {
      printf '%s\n' 'mock curl: simulated download failure' >&2
      exit 22
    }
    [ "$MOCK_CURL_FAIL_CHECKSUM" != 1 ] || {
      printf '%s\n' 'mock curl: simulated checksum download failure' >&2
      exit 22
    }

    archive_path=${OUTPUT%.sha256}
    archive_name=${URL##*/}
    archive_name=${archive_name%.sha256}
    checksum=$("$REAL_SHA256SUM" "$archive_path" | "$REAL_AWK" '{ print $1 }')
    if [ "${MOCK_BAD_CHECKSUM:-0}" = 1 ]; then
      checksum=0000000000000000000000000000000000000000000000000000000000000000
    fi
    printf '%s  %s\n' "$checksum" "$archive_name" > "$OUTPUT"
    ;;
  *.tgz)
    [ "$MOCK_CURL_FAIL_DOWNLOAD" != 1 ] || {
      printf '%s\n' 'mock curl: simulated download failure' >&2
      exit 22
    }
    if [ "$MOCK_CURL_BLOCK" = 1 ]; then
      printf '%s\n' "$$" > "$MOCK_CURL_PID_FILE"
      exec "$REAL_SLEEP" 30
    fi
    case "$MOCK_ARCHIVE_MODE" in
      good) source_archive=$MOCK_ARCHIVE_GOOD ;;
      missing) source_archive=$MOCK_ARCHIVE_MISSING ;;
      nonexec) source_archive=$MOCK_ARCHIVE_NONEXEC ;;
      corrupt) source_archive=$MOCK_ARCHIVE_CORRUPT ;;
      *) printf 'unknown archive mode: %s\n' "$MOCK_ARCHIVE_MODE" >&2; exit 2 ;;
    esac
    "$REAL_CP" "$source_archive" "$OUTPUT"
    ;;
  *)
    printf 'mock curl: unexpected URL: %s\n' "$URL" >&2
    exit 2
    ;;
esac
MOCK_CURL

cat > "$MOCK_BIN/uname" <<'MOCK_UNAME'
#!/bin/sh
case "$1" in
  -s) printf '%s\n' "$MOCK_UNAME_S" ;;
  -m) printf '%s\n' "$MOCK_UNAME_M" ;;
  *) exit 1 ;;
esac
MOCK_UNAME

cat > "$MOCK_BIN/ldd" <<'MOCK_LDD'
#!/bin/sh
printf '%s\n' "$MOCK_LDD_OUTPUT"
MOCK_LDD

cat > "$MOCK_BIN/install" <<'MOCK_INSTALL'
#!/bin/sh
if [ "$MOCK_INSTALL_FAIL" = 1 ]; then
  printf '%s\n' 'mock install: simulated write failure' >&2
  exit 1
fi
exec "$REAL_INSTALL" "$@"
MOCK_INSTALL

cat > "$MOCK_BIN/mv" <<'MOCK_MV'
#!/bin/sh
if [ "$MOCK_MV_FAIL" = 1 ]; then
  printf '%s\n' 'mock mv: simulated rename failure' >&2
  exit 1
fi
exec "$REAL_MV" "$@"
MOCK_MV

cat > "$MOCK_BIN/shasum" <<'MOCK_SHASUM'
#!/bin/sh
if [ "${1:-}" = -a ]; then
  shift 2
fi
exec "$REAL_SHA256SUM" "$@"
MOCK_SHASUM

"$REAL_CHMOD" +x "$MOCK_BIN/curl" "$MOCK_BIN/uname" "$MOCK_BIN/ldd" "$MOCK_BIN/install" "$MOCK_BIN/mv" "$MOCK_BIN/shasum" "$MOCK_BIN/uname" "$MOCK_BIN/ldd" "$MOCK_BIN/install" "$MOCK_BIN/mv"

# Build real .tgz fixtures for extraction and executable-bit tests.
"$REAL_MKDIR" -p "$FIXTURE_DIR" "$GOOD_DIR" "$MISSING_DIR" "$NONEXEC_DIR"
printf '#!/bin/sh\nprintf "sockudo test binary\\n"\n' > "$GOOD_DIR/sockudo"
"$REAL_CHMOD" 0755 "$GOOD_DIR/sockudo"
printf 'fixture without the expected binary\n' > "$MISSING_DIR/README"
printf '#!/bin/sh\nprintf "non-executable fixture\\n"\n' > "$NONEXEC_DIR/sockudo"
"$REAL_CHMOD" 0644 "$NONEXEC_DIR/sockudo"
"$REAL_TAR" -czf "$GOOD_ARCHIVE" -C "$GOOD_DIR" sockudo
"$REAL_TAR" -czf "$MISSING_ARCHIVE" -C "$MISSING_DIR" README
"$REAL_TAR" -czf "$NONEXEC_ARCHIVE" -C "$NONEXEC_DIR" sockudo
printf '%s\n' 'this is not a gzip archive' > "$CORRUPT_ARCHIVE"

FULL_PATH="$WORK/path-full"
make_path "$FULL_PATH"

printf '# Sockudo installer tests\n'
printf '# Installer: %s\n' "$INSTALLER"

# Help and argument parsing.
section 'Help and argument parsing.'

new_case
if run_installer --help && assert_contains 'Usage:' "$OUT" && [ ! -s "$CURL_LOG" ]; then
  pass 'help exits successfully without making requests'
else
  fail_test 'help exits successfully without making requests'
fi

new_case
expect_failure 'unknown options are rejected' 'unknown option: --bogus' --bogus
new_case
expect_failure 'empty --version= is rejected' '--version requires a value' --version=
new_case
expect_failure 'empty --bin-dir= is rejected' '--bin-dir requires a value' --bin-dir=
new_case
expect_failure 'missing --version value is rejected' '--version requires a value' --version
new_case
expect_failure 'another option cannot be used as --version value' '--version requires a value' --version --bin-dir "$CASE_BIN"
new_case
expect_failure 'missing --bin-dir value is rejected' '--bin-dir requires a value' --bin-dir

new_case
TEST_HOME=
expect_failure 'missing HOME is reported when no install directory is supplied' 'HOME is not set; pass --bin-dir' --version 5.2.0

new_case
TEST_HOME=
expect_success 'explicit --bin-dir works without HOME' --version 5.2.0 --bin-dir "$CASE_BIN"

# Semantic Versioning validation.
section 'Semantic Versioning validation.'

for invalid_version in '1' '1.2' '1..2' '1.2.3-' '1.2.3+' '01.2.3' '1.02.3' '1.2.03' '1.2.3-01' '1.2.3/foo' 'v'; do
  new_case
  TEST_VERSION=$invalid_version
  expect_failure "invalid SemVer is rejected: $invalid_version" 'invalid release version:'
  if [ -s "$CURL_LOG" ]; then
    fail_test "invalid SemVer makes no network requests: $invalid_version"
  else
    pass "invalid SemVer makes no network requests: $invalid_version"
  fi
done

new_case
TEST_VERSION=5.2.0
expect_success 'plain SemVer version installs' --version 5.2.0 --bin-dir "$CASE_BIN"
if assert_contains '/download/v5.2.0/sockudo-v5.2.0-x86_64-unknown-linux-gnu.tgz' "$CURL_LOG"; then
  pass 'release URL uses the requested version and GNU x86_64 target'
else
  fail_test 'release URL uses the requested version and GNU x86_64 target'
fi

new_case
expect_success 'v-prefixed version is normalized' --version=v5.2.0 --bin-dir "$CASE_BIN"
if assert_contains '/download/v5.2.0/sockudo-v5.2.0-x86_64-unknown-linux-gnu.tgz' "$CURL_LOG"; then
  pass 'v-prefixed version does not produce a double-v release URL'
else
  fail_test 'v-prefixed version does not produce a double-v release URL'
fi

new_case
expect_success 'SemVer prerelease and build metadata are accepted' --version 5.2.0-rc.1+build.123 --bin-dir "$CASE_BIN"
if assert_contains 'sockudo-v5.2.0-rc.1+build.123-x86_64-unknown-linux-gnu.tgz' "$CURL_LOG"; then
  pass 'prerelease and build metadata are preserved in the asset name'
else
  fail_test 'prerelease and build metadata are preserved in the asset name'
fi

# Latest release resolution.
section 'Latest release resolution.'
new_case
expect_success 'unset version resolves latest release' --bin-dir "$CASE_BIN"
if assert_contains '/releases/latest' "$CURL_LOG" && assert_contains '/download/v5.2.0/' "$CURL_LOG"; then
  pass 'latest release URL is resolved before downloading the archive'
else
  fail_test 'latest release URL is resolved before downloading the archive'
fi

new_case
TEST_VERSION=latest
expect_success 'explicit latest version resolves latest release' --bin-dir "$CASE_BIN"

new_case
MOCK_LATEST_MODE=invalid
expect_failure 'unexpected latest-release URL is rejected' 'could not determine the latest release version' --bin-dir "$CASE_BIN"

new_case
MOCK_CURL_FAIL=1
expect_failure 'latest-release lookup failure stops installation' '' --bin-dir "$CASE_BIN"

# Environment variables, CLI precedence, and destination defaults.
section 'Environment variables, CLI precedence, and destination defaults.'

new_case
TEST_VERSION=5.2.0
TEST_ENV_BIN_DIR="$CASE_DIR/env-bin"
expect_success 'SOCKUDO_INSTALL_DIR and SOCKUDO_VERSION are honored'
if [ -x "$TEST_ENV_BIN_DIR/sockudo" ]; then
  pass 'environment-selected directory receives the binary'
else
  fail_test 'environment-selected directory receives the binary'
fi

new_case
TEST_VERSION=1.2.3
TEST_ENV_BIN_DIR="$CASE_DIR/env-bin"
expect_success 'CLI arguments override installer environment variables' --version 5.2.0 --bin-dir "$CASE_BIN"
if [ -x "$CASE_BIN/sockudo" ] && [ ! -e "$TEST_ENV_BIN_DIR/sockudo" ]; then
  pass 'CLI values take precedence over environment values'
else
  fail_test 'CLI values take precedence over environment values'
fi

new_case
expect_success 'default destination is HOME/.local/bin'
if [ -x "$CASE_HOME/.local/bin/sockudo" ]; then
  pass 'default installation goes to ~/.local/bin'
else
  fail_test 'default installation goes to ~/.local/bin'
fi

new_case
TEST_ENV_BIN_DIR="$CASE_DIR/bin with spaces"
expect_success 'installation directory containing spaces is supported'

new_case
expect_success 'PATH reminder is printed when destination is absent from PATH' --bin-dir "$CASE_BIN"
if assert_contains "Add $CASE_BIN to PATH" "$OUT"; then
  pass 'PATH reminder contains the selected directory'
else
  fail_test 'PATH reminder contains the selected directory'
fi

new_case
TEST_ENV_BIN_DIR="$MOCK_BIN"
TEST_PATH="$MOCK_BIN:$FULL_PATH"
expect_success 'PATH reminder is omitted when destination is already on PATH'
if assert_not_contains 'Add ' "$OUT"; then
  pass 'no redundant PATH reminder is printed'
else
  fail_test 'no redundant PATH reminder is printed'
fi

# Platform and libc target selection.
section 'Platform and libc target selection.'

new_case
MOCK_UNAME_M=amd64
expect_success 'amd64 maps to x86_64 target' --version 5.2.0 --bin-dir "$CASE_BIN"
if assert_contains 'x86_64-unknown-linux-gnu' "$CURL_LOG"; then pass 'amd64 target name is correct'; else fail_test 'amd64 target name is correct'; fi

new_case
MOCK_UNAME_M=aarch64
expect_success 'aarch64 GNU target installs' --version 5.2.0 --bin-dir "$CASE_BIN"
if assert_contains 'aarch64-unknown-linux-gnu' "$CURL_LOG"; then pass 'aarch64 target name is correct'; else fail_test 'aarch64 target name is correct'; fi

new_case
MOCK_UNAME_M=arm64
expect_success 'arm64 maps to aarch64 target' --version 5.2.0 --bin-dir "$CASE_BIN"
if assert_contains 'aarch64-unknown-linux-gnu' "$CURL_LOG"; then pass 'arm64 target name is correct'; else fail_test 'arm64 target name is correct'; fi

new_case
MOCK_LDD_OUTPUT='musl libc (x86_64)'
expect_success 'musl libc target installs' --version 5.2.0 --bin-dir "$CASE_BIN"
if assert_contains 'x86_64-unknown-linux-musl' "$CURL_LOG"; then pass 'musl target name is correct'; else fail_test 'musl target name is correct'; fi

new_case
MOCK_UNAME_S=Darwin
expect_failure 'non-Linux operating systems are rejected' 'prebuilt binaries are available only for Linux' --bin-dir "$CASE_BIN"
if [ ! -s "$CURL_LOG" ]; then pass 'unsupported operating system makes no requests'; else fail_test 'unsupported operating system makes no requests'; fi

new_case
MOCK_UNAME_M=riscv64
expect_failure 'unsupported Linux architectures are rejected' 'unsupported Linux architecture: riscv64' --bin-dir "$CASE_BIN"
if [ ! -s "$CURL_LOG" ]; then pass 'unsupported architecture makes no requests'; else fail_test 'unsupported architecture makes no requests'; fi

# Dependency checks. Each restricted PATH contains every utility except the
# specific command under test.
section 'Required dependencies'

for missing_dependency in curl tar install grep gzip; do
  path_variant="$WORK/path-without-$missing_dependency"
  make_path "$path_variant" "$missing_dependency"
  new_case
  TEST_PATH=$path_variant
  expect_failure "missing $missing_dependency dependency is reported" "$missing_dependency is required" --version 5.2.0 --bin-dir "$CASE_BIN"
done

# Checksum handling. Checksum filename matching is deliberately outside this test set.
section 'Download and checksum verification'

new_case
MOCK_BAD_CHECKSUM=1
expect_failure 'a checksum mismatch stops installation' '' --version 5.2.0 --bin-dir "$CASE_BIN"
if assert_download_dir_cleaned; then
  pass 'temporary download directory is removed after checksum failure'
else
  fail_test 'temporary download directory is removed after checksum failure'
fi

new_case
printf '%s\n' 'old sockudo binary' > "$CASE_BIN/sockudo"
"$REAL_CHMOD" 0755 "$CASE_BIN/sockudo"
MOCK_BAD_CHECKSUM=1
expect_failure 'checksum mismatch leaves existing installation untouched' '' --version 5.2.0 --bin-dir "$CASE_BIN"
if "$REAL_GREP" -F 'old sockudo binary' "$CASE_BIN/sockudo" >/dev/null 2>&1; then pass 'checksum failure preserves the previous binary'; else fail_test 'checksum failure preserves the previous binary'; fi
if assert_no_temp_files "$CASE_BIN"; then pass 'checksum failure creates no destination temporary files'; else fail_test 'checksum failure creates no destination temporary files'; fi

new_case
MOCK_CURL_FAIL_DOWNLOAD=1
expect_failure 'archive download failure stops installation' '' --version 5.2.0 --bin-dir "$CASE_BIN"
if assert_download_dir_cleaned; then
  pass 'temporary download directory is removed after download failure'
else
  fail_test 'temporary download directory is removed after download failure'
fi

new_case
MOCK_CURL_FAIL_CHECKSUM=1
expect_failure 'checksum download failure stops installation' '' --version 5.2.0 --bin-dir "$CASE_BIN"
if assert_download_dir_cleaned; then
  pass 'temporary download directory is removed after checksum download failure'
else
  fail_test 'temporary download directory is removed after checksum download failure'
fi

no_checksum_path="$WORK/path-without-checksum-tools"
make_path "$no_checksum_path" checksum-tools
new_case
TEST_PATH=$no_checksum_path
expect_failure 'installation fails clearly when no checksum utility exists' 'sha256sum or shasum is required' --version 5.2.0 --bin-dir "$CASE_BIN"
if assert_download_dir_cleaned; then
  pass 'temporary download directory is removed when checksum tools are missing'
else
  fail_test 'temporary download directory is removed when checksum tools are missing'
fi

no_sha256_path="$WORK/path-without-sha256sum"
make_path "$no_sha256_path" sha256sum
new_case
TEST_PATH=$no_sha256_path
expect_success 'shasum fallback verifies the downloaded archive' --version 5.2.0 --bin-dir "$CASE_BIN"
if [ -x "$CASE_BIN/sockudo" ]; then pass 'shasum fallback completes installation'; else fail_test 'shasum fallback completes installation'; fi

# Archive extraction and extracted-binary validation.
section 'Archive extraction and validation'

new_case
MOCK_ARCHIVE_MODE=missing
expect_failure 'archive without sockudo binary is rejected' 'release archive does not contain the sockudo binary' --version 5.2.0 --bin-dir "$CASE_BIN"
if assert_download_dir_cleaned; then pass 'temporary files are removed after missing-binary failure'; else fail_test 'temporary files are removed after missing-binary failure'; fi

new_case
MOCK_ARCHIVE_MODE=nonexec
expect_success 'non-executable temporary binary can be installed' --version 5.2.0 --bin-dir "$CASE_BIN"

if [ -x "$CASE_BIN/sockudo" ] &&
  "$REAL_GREP" -F 'non-executable fixture' "$CASE_BIN/sockudo" >/dev/null 2>&1; then
  pass 'non-executable temporary binary is installed with executable permissions'
else
  fail_test 'non-executable temporary binary is installed with executable permissions'
fi

if assert_download_dir_cleaned; then
  pass 'temporary files are removed after installation'
else
  fail_test 'temporary files are removed after installation'
fi
new_case
MOCK_ARCHIVE_MODE=corrupt
expect_failure 'corrupt archive fails extraction' '' --version 5.2.0 --bin-dir "$CASE_BIN"
if assert_download_dir_cleaned; then pass 'temporary files are removed after extraction failure'; else fail_test 'temporary files are removed after extraction failure'; fi

# Atomic installation and failure cleanup.
section 'Installation and failure recovery'

new_case
printf '%s\n' 'old sockudo binary' > "$CASE_BIN/sockudo"
"$REAL_CHMOD" 0755 "$CASE_BIN/sockudo"
expect_success 'successful installation replaces an existing binary' --version 5.2.0 --bin-dir "$CASE_BIN"
if "$REAL_GREP" -F 'sockudo test binary' "$CASE_BIN/sockudo" >/dev/null 2>&1 && [ -x "$CASE_BIN/sockudo" ]; then
  pass 'replacement binary has the new contents and executable permissions'
else
  fail_test 'replacement binary has the new contents and executable permissions'
fi
if assert_no_temp_files "$CASE_BIN"; then pass 'successful installation leaves no temporary destination files'; else fail_test 'successful installation leaves no temporary destination files'; fi
if assert_download_dir_cleaned; then pass 'successful installation removes the download directory'; else fail_test 'successful installation removes the download directory'; fi

new_case
printf '%s\n' 'old sockudo binary' > "$CASE_BIN/sockudo"
"$REAL_CHMOD" 0755 "$CASE_BIN/sockudo"
MOCK_INSTALL_FAIL=1
expect_failure 'failed staging write aborts installation' 'could not install to' --version 5.2.0 --bin-dir "$CASE_BIN"
if "$REAL_GREP" -F 'old sockudo binary' "$CASE_BIN/sockudo" >/dev/null 2>&1; then
  pass 'failed staging write preserves the previous binary'
else
  fail_test 'failed staging write preserves the previous binary'
fi
if assert_no_temp_files "$CASE_BIN"; then pass 'failed staging write removes its temporary file'; else fail_test 'failed staging write removes its temporary file'; fi

new_case
printf '%s\n' 'old sockudo binary' > "$CASE_BIN/sockudo"
"$REAL_CHMOD" 0755 "$CASE_BIN/sockudo"
MOCK_MV_FAIL=1
expect_failure 'failed atomic rename aborts installation' 'could not replace existing sockudo binary' --version 5.2.0 --bin-dir "$CASE_BIN"
if "$REAL_GREP" -F 'old sockudo binary' "$CASE_BIN/sockudo" >/dev/null 2>&1; then
  pass 'failed atomic rename preserves the previous binary'
else
  fail_test 'failed atomic rename preserves the previous binary'
fi
if assert_no_temp_files "$CASE_BIN"; then pass 'failed atomic rename removes its temporary file'; else fail_test 'failed atomic rename removes its temporary file'; fi

new_case
"$REAL_MKDIR" "$CASE_BIN/sockudo"
expect_failure 'destination path already occupied by a directory is rejected' "$CASE_BIN/sockudo is a directory" --version 5.2.0 --bin-dir "$CASE_BIN"
if [ -d "$CASE_BIN/sockudo" ]; then pass 'existing destination directory is left untouched'; else fail_test 'existing destination directory is left untouched'; fi

# TERM while curl is running must terminate the installer and clean its temp dir.
section 'Signal handling'

new_case
MOCK_CURL_BLOCK=1
PATH="$TEST_PATH" \
HOME="$CASE_HOME" \
SOCKUDO_VERSION=5.2.0 \
SOCKUDO_INSTALL_DIR="$CASE_BIN" \
MOCK_CURL_LOG="$CURL_LOG" \
MOCK_DOWNLOAD_DIR_FILE="$DOWNLOAD_DIR_FILE" \
MOCK_CURL_PID_FILE="$CURL_PID_FILE" \
MOCK_ARCHIVE_GOOD="$GOOD_ARCHIVE" \
MOCK_ARCHIVE_MISSING="$MISSING_ARCHIVE" \
MOCK_ARCHIVE_NONEXEC="$NONEXEC_ARCHIVE" \
MOCK_ARCHIVE_CORRUPT="$CORRUPT_ARCHIVE" \
MOCK_ARCHIVE_MODE=good \
MOCK_CURL_FAIL=0 \
MOCK_CURL_FAIL_DOWNLOAD=0 \
MOCK_CURL_FAIL_CHECKSUM=0 \
MOCK_CURL_BLOCK=1 \
MOCK_LATEST_MODE=valid \
MOCK_LATEST_VERSION=5.2.0 \
MOCK_UNAME_S=Linux \
MOCK_UNAME_M=x86_64 \
MOCK_LDD_OUTPUT='GNU libc' \
MOCK_INSTALL_FAIL=0 \
MOCK_MV_FAIL=0 \
MOCK_BAD_CHECKSUM=0 \
REAL_CP="$REAL_CP" \
REAL_SHA256SUM="$REAL_SHA256SUM" \
REAL_AWK="$REAL_AWK" \
REAL_INSTALL="$REAL_INSTALL" \
REAL_MV="$REAL_MV" \
REAL_SLEEP="$REAL_SLEEP" \
  "$REAL_SH" "$INSTALLER" --version 5.2.0 --bin-dir "$CASE_BIN" > "$OUT" 2> "$ERR" &
installer_pid=$!

wait_count=0
while [ ! -s "$CURL_PID_FILE" ] && [ "$wait_count" -lt 100 ]; do
  "$REAL_SLEEP" 0.05
  wait_count=$((wait_count + 1))
done

if [ ! -s "$CURL_PID_FILE" ] || [ ! -s "$DOWNLOAD_DIR_FILE" ]; then
  kill -TERM "$installer_pid" 2>/dev/null || :
  wait "$installer_pid" 2>/dev/null || :
  fail_test 'installer enters the download phase for signal-cleanup test'
else
  curl_pid=$($REAL_CAT "$CURL_PID_FILE")
  download_dir=$($REAL_CAT "$DOWNLOAD_DIR_FILE")
  kill -TERM "$installer_pid" 2>/dev/null || :
  kill -TERM "$curl_pid" 2>/dev/null || :
  if wait "$installer_pid"; then
    signal_status=0
  else
    signal_status=$?
  fi
  if [ "$signal_status" -eq 143 ] && [ ! -e "$download_dir" ]; then
    pass 'SIGTERM exits with status 143 and removes the temporary download directory'
  else
    fail_test 'SIGTERM exits with status 143 and removes the temporary download directory'
  fi
fi

printf '\n1..%s\n' "$TEST_COUNT"
printf '# Passed: %s\n' "$PASS_COUNT"
printf '# Failed: %s\n' "$FAIL_COUNT"

if [ "$FAIL_COUNT" -ne 0 ]; then
  exit 1
fi

printf '# All installer tests passed.\n'