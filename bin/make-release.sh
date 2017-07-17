#!/bin/bash

bin="`dirname "$0"`"
ROOT_DIR="`cd "$bin/../"; pwd`"

function show_help() {
cat <<EOM
Usage: $0 [options]
  --release        release version
  --next           next development version
  --help           display usage of the script
EOM
}

for i in "$@"; do
  case $i in
    --release=*)
      RELEASE_VERSION="${i#*=}"
    shift ;;
    --next=*)
      NEXT_VERSION="${i#*=}"
    shift ;;
    --help)
      show_help
      exit 0
    shift ;;
  esac
done

# Script to make release, commit changes and tag, and push changes to remote
cd $ROOT_DIR

if [[ -z "$RELEASE_VERSION" ]]; then
  echo "[error] Release version is not set, use --release=XYZ to set"
  exit 1
fi

if [[ -z "$NEXT_VERSION" ]]; then
  echo "[error] Next dev version is not set, use --next=XYZ to set"
  exit 1
fi

if ! $(git diff --quiet); then
  echo "[error] Found unstaged changes! Commit or discard changes first."
  exit 1
fi

echo "[info] Clean target directory and run tests"
sbt clean test || exit 1

echo "[info] Pull latest changes"
git pull

echo "[info] Set release version"
# set release version
echo "version in ThisBuild := \"$RELEASE_VERSION\"" > version.sbt
echo "module.exports = \"$RELEASE_VERSION\";" > static/js/version.js

# add modified files and commit them
git add version.sbt
git add static/js/version.js
git commit -m "Setting version to $RELEASE_VERSION"
git tag "v$RELEASE_VERSION"

echo "[info] Run make-distribution"
bin/make-distribution.sh --tgz --name="$RELEASE_VERSION"

echo "[info] Set next development version"
echo "version in ThisBuild := \"$NEXT_VERSION\"" > version.sbt
echo "module.exports = \"$NEXT_VERSION\";" > static/js/version.js

# commit next dev version
git add version.sbt
git add static/js/version.js
git commit -m "Prepare next dev version $NEXT_VERSION"

echo "Done. Do not forget to push commits and tags!"
echo "Latest commits added:"
git log --pretty=oneline -n5
