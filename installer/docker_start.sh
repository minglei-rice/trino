#!/bin/bash

set -e

_term() {
  echo "Caught SIGTERM signal. Shutdown trino server gracefully"

  response=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:"$TRINO_SERVER_PORT"/v1/info/coordinator)
  if [ "$response" == "200" ]; then
    echo "This node is coordinator. Shutdown immediately"
    exit 0
  fi

  echo "This node is worker. Shutdown gracefully"
  response=$(curl -X PUT -s -o /dev/null -w "%{http_code}" -d '"SHUTTING_DOWN"' -H "Content-type: application/json" -H "X-Trino-User: admin" http://localhost:"$TRINO_SERVER_PORT"/v1/info/state)
  if [ "$response" == "200" ]; then
    echo "Shutting down worker in 4 minutes"
    sleep 250
    exit 0
  fi

  echo "Failed to shutdown worker gracefully"
  exit 1
}

trap _term SIGTERM

# check env
cd $(dirname "$0")
if [[ "$TRINO_HOME_BASE" != $(pwd) ]]; then
  echo "Current directory $(pwd) should be the same as TRINO_HOME_BASE=$TRINO_HOME_BASE"
  exit 1
fi
if [ -z "$DEPLOY_ENV" ]; then
  echo "DEPLOY_ENV is missing"
  exit 1
fi
if [ ! -r version ]; then
  echo "version file does not exist or is not readable"
  exit 1
fi

# prepare env
export TRINO_USER=root
TRINO_VERSION=$(cat version)
export TRINO_VERSION
export TRINO_ENVIRONMENT="$DEPLOY_ENV"_docker

# install trino
installer/install.sh
./switch_version.sh

# start server in background
source trino-env.sh
./launcher.sh start "$TRINO_LAUNCH_OPTIONS"

# keep current process alive to pass caster health check
sleep 90
while true; do
  trino_pid=$(jps | grep -m 1 "TrinoServer" | awk '{print $1}')
  cur_time=$(date '+%Y-%m-%dT%H:%M:%S')
  if [ -z "$trino_pid" ]; then
    echo "$cur_time Server is not running"
    exit 1
  fi
  echo "$cur_time Server is running with pid $trino_pid"
  sleep 30 &
  wait $!
done
