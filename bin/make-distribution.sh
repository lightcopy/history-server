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

# Command-line options
for i in "$@"; do
  case $i in
    # Daemon process (true/false)
    --name=*)
      RELEASE_NAME="${i#*=}"
    shift ;;
    --tgz)
      MAKE_TGZ="true"
    shift ;;
    # Display help
    --help)
      show_help
      exit 0
    shift ;;
  esac
done

echo "[info] Project directory $ROOT_DIR"
TARGET_DIR=$ROOT_DIR/target
echo "[info] Target directory $TARGET_DIR"

if [[ -n "$RELEASE_NAME" ]]; then
  RELEASE_NAME="history-server-bin-$RELEASE_NAME"
else
  RELEASE_NAME="history-server-bin"
fi
rm -rf "$TARGET_DIR/$RELEASE_NAME"
rm -f "$TARGET_DIR/$RELEASE_NAME.tgz"
mkdir -p "$TARGET_DIR/$RELEASE_NAME"
echo "[info] Prepared release directory $TARGET_DIR/$RELEASE_NAME"

echo "[info] Build assembly jar"
sbt assembly

echo "[info] Build static files in dist dir"
npm run prod

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

if [[ -n "$MAKE_TGZ" ]]; then
  TARNAME="$RELEASE_NAME.tgz"
  echo "Create $TARNAME"
  tar czf "$TARGET_DIR/$TARNAME" -C "$TARGET_DIR" "$RELEASE_NAME"
  rm -rf $TARGET_DIR/$RELEASE_NAME
fi

echo "[info] Done, see $TARGET_DIR for release artefacts"
