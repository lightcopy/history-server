#!/bin/bash

bin="`dirname "$0"`"
ROOT_DIR="`cd "$bin/../"; pwd`"

# Start dev server to test API, does not require Mongo
sbt compile \
  -Dspark.eventLog.dir="$ROOT_DIR/work" \
  -Dlog4j.configuration="file:$ROOT_DIR/conf/log4j.properties" \
  "runMain com.github.lightcopy.history.DevServer"
