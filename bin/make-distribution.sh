#!/bin/bash

bin="`dirname "$0"`"
ROOT_DIR="`cd "$bin/../"; pwd`"

function show_help() {
cat <<EOM
Usage: $0 [options]
  --name            distribution name suffix, e.g. --name=xyz
  --tgz             package release into .tgz file
  --help            display usage of the script
EOM
}

# Build flags will be saved as part of release in addition to manifest
BUILD_FLAGS="$@"

for i in "$@"; do
  case $i in
    --name=*)
      MAKE_SUFFIX="${i#*=}"
    shift ;;
    --tgz)
      MAKE_TGZ="true"
    shift ;;
    --help)
      show_help
      exit 0
    shift ;;
  esac
done

echo "[info] Make .tgz = ${MAKE_TGZ:-false}"
echo "[info] Use suffix = ${MAKE_SUFFIX:-''}"

# Prepare directories
cd $ROOT_DIR

echo "[info] Project directory $ROOT_DIR"
TARGET_DIR=$ROOT_DIR/target
echo "[info] Target directory $TARGET_DIR"

if [[ -n "$MAKE_SUFFIX" ]]; then
  RELEASE_NAME="history-server-bin-$MAKE_SUFFIX"
else
  RELEASE_NAME="history-server-bin"
fi
rm -rf "$TARGET_DIR/$RELEASE_NAME"
rm -f "$TARGET_DIR/$RELEASE_NAME.tgz"
mkdir -p "$TARGET_DIR/$RELEASE_NAME"
echo "[info] Prepared release directory $TARGET_DIR/$RELEASE_NAME"

# Start build process
echo "[info] Build assembly jar"
sbt assembly

echo "[info] Build static files in dist dir"
npm run prod

# Start assembly of artefacts
echo "[info] Assemble artefacts in release directory"
for f in $(find $TARGET_DIR/scala-2.11 -name *.jar -type f); do
  echo "[info] Found jar $f, will be added to distribution"
  cp $f $TARGET_DIR/$RELEASE_NAME
done
echo "[info] Copy dist folder into release directory"
cp -r $ROOT_DIR/dist $TARGET_DIR/$RELEASE_NAME/
echo "[info] Copy conf folder into release directory"
cp -r $ROOT_DIR/conf $TARGET_DIR/$RELEASE_NAME/
echo "[info] Copy sbin folder into release directory"
cp -r $ROOT_DIR/sbin $TARGET_DIR/$RELEASE_NAME/
echo "[info] Copy license"
cp $ROOT_DIR/LICENSE $TARGET_DIR/$RELEASE_NAME/
echo "[info] Copy readme"
cp $ROOT_DIR/README.md $TARGET_DIR/$RELEASE_NAME/

# Create file based on manifest[s] and build flags
echo "[info] Create RELEASE file"
RELEASE_MANIFEST_FILE="$TARGET_DIR/$RELEASE_NAME/RELEASE"
touch $RELEASE_MANIFEST_FILE
echo "Build flags: $BUILD_FLAGS" >> $RELEASE_MANIFEST_FILE
echo "Build date: $(date)" >> $RELEASE_MANIFEST_FILE
echo "Git branch: $(git rev-parse --abbrev-ref HEAD)" >> $RELEASE_MANIFEST_FILE
echo "Git HEAD rev: $(git log -n1 --pretty=%H)" >> $RELEASE_MANIFEST_FILE
echo "Git repo clean: $(git diff --quiet && echo 'true' || echo 'false')" >> $RELEASE_MANIFEST_FILE

echo "" >> $RELEASE_MANIFEST_FILE
for f in $(find $TARGET_DIR/$RELEASE_NAME -name *.jar -type f); do
  unzip -pq $f META-INF/MANIFEST.MF >> $RELEASE_MANIFEST_FILE
done

# tgz is enabled, we pack release folder into archive and delete release folder
if [[ -n "$MAKE_TGZ" ]]; then
  TARNAME="$RELEASE_NAME.tgz"
  tar czf "$TARGET_DIR/$TARNAME" -C "$TARGET_DIR" "$RELEASE_NAME"
  rm -rf $TARGET_DIR/$RELEASE_NAME
  echo "[info] Created $TARNAME"
fi

echo "[info] Done, see $TARGET_DIR for release artefacts"
