#!/bin/bash

bin="`dirname "$0"`"
ROOT_DIR="`cd "$bin/../"; pwd`"

function show_help() {
cat <<EOM
Usage: $0 [options]
  --daemon, -d      launch service as daemon process, e.g. -d or --daemon=true/false
  --help            display usage of the script
EOM
}

function resolve_daemon_mode() {
  OPTION_USE_DAEMON="${i#*=}"
  if [[ "$OPTION_USE_DAEMON" == "-d" ]]; then
    echo "true"
  elif [[ "$OPTION_USE_DAEMON" == "true" ]]; then
    echo "true"
  elif [[ "$OPTION_USE_DAEMON" == "false" ]]; then
    echo ""
  else
    echo "ERROR"
  fi
}

# Command-line options:
for i in "$@"; do
  case $i in
    # Daemon process (true/false)
    -d|--daemon=*)
      OPTION_USE_DAEMON=$(resolve_daemon_mode)
      if [[ "$OPTION_USE_DAEMON" == "ERROR" ]]; then
cat <<EOM
[WARN] Unrecognized value $OPTION_USE_DAEMON for '--daemon' option
Run '--help' to display possible options, default false is used
EOM
        OPTION_USE_DAEMON=""
      fi
    shift ;;
    # Display help
    --help)
      show_help
      exit 0
    shift ;;
  esac
done

# Import current settings from configuration
. "$ROOT_DIR/conf/history-server-env.sh"

# Compile path to all source files including assembly jar
# When running as distribution, jar is located in root directory; when running from repository
# jar will be compiled and stored in target/scala-2.11/...
# We search for any .jar file in root directory recursively and build sources path
PATH_TO_SOURCES="$ROOT_DIR"
for f in $(find $ROOT_DIR -name *.jar -type f); do
  PATH_TO_SOURCES="$PATH_TO_SOURCES:$f"
done

# Resolve log4j configuration file
# If property is not set, fall back to default log4j.properties
if [[ -z "$LOG4J_CONF_FILE" ]]; then
  LOG4J_CONF_FILE="file:$ROOT_DIR/conf/log4j.properties"
fi

# Start service in script root directory (to load dist)
cd $ROOT_DIR
LAUNCH_CMD="java \
  -Xmx512m -XX:MaxPermSize=256m \
  -Dlog4j.configuration='$LOG4J_CONF_FILE' \
  -Dhttp.host='$HISTORY_SERVER_HOST' \
  -Dhttp.port='$HISTORY_SERVER_PORT' \
  -Dspark.eventLog.dir='$SPARK_EVENT_LOG_DIR' \
  -Dmongo.address='$MONGO_CONNECTION' \
  -cp $PATH_TO_SOURCES com.github.lightcopy.history.Server"

if [[ -n "$OPTION_USE_DAEMON" ]]; then
  echo "[INFO] Launching service as daemon process"
  eval "nohup $LAUNCH_CMD 0<&- &>/dev/null &"
else
  eval "$LAUNCH_CMD"
fi
