#!/bin/bash

bin="`dirname "$0"`"
ROOT_DIR="`cd "$bin/../"; pwd`"

# Return current active service PID/s
function get_application_pids() {
  echo $(ps aux | grep '[j]ava' | grep 'com.github.lightcopy.history.Server' | awk '{print $2}')
}

# We potentially can have multiple PIDs for application, e.g. when launching with make
# So we have to shutdown all of them gracefully
function shutdown() {
  SIG="INT"
  # Resolve termination signal
  if [[ -n "$1" ]]; then
    SIG="$1"
  fi

  while [[ -n $(get_application_pids) ]]; do
    for pid in $(get_application_pids); do
      echo "[INFO] Stop application process $pid..."
      kill -s $SIG $pid
      sleep 3
    done
  done
}

# Request application exit
shutdown "TERM"
