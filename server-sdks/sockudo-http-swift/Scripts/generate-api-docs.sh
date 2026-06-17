#! /bin/sh

set -e

if ! which jazzy >/dev/null; then
  echo "Error: Jazzy not installed, see https://github.com/realm/Jazzy or run 'gem install jazzy' to install it"
  exit 1
fi

if ! which jq >/dev/null; then
  echo "Error: jq not installed, run 'brew install jq' to install it"
  exit 1
fi

read -p 'Enter release tag (without quotes): ' RELEASE_TAG

AUTHOR_NAME="Pusher Limited"
AUTHOR_URL="https://pusher.com"
GITHUB_ORIGIN=$(git remote get-url origin)
GITHUB_URL=${GITHUB_ORIGIN%".git"}
MODULE_NAME=$(swift package dump-package | jq --raw-output '.name')

echo "Generating public API docs from release tag $RELEASE_TAG"

# The 'arch -x86_64' command is redundant on Intel Macs, and runs Jazzy under Rosetta 2 on Apple Silicon Macs for compatibility
arch -x86_64 jazzy \
--module $MODULE_NAME \
--module_version $RELEASE_TAG \
--author $AUTHOR_NAME \
--author_url $AUTHOR_URL \
--github_url $GITHUB_URL \
--github-file-prefix $GITHUB_URL/tree/$RELEASE_TAG