#!/bin/bash

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

cd $(dirname "$0")
source trino-env.sh
if [[ $TRINO_ENVIRONMENT != *docker ]]; then
  echo "Server is not installed in docker. Please run launcher.sh directly"
  exit 1
fi
./launcher.sh start

sleep 15
while true; do
  trino_pid=$(jps | grep -m 1 "TrinoServer" | awk '{print $1}')
  cur_time=$(date '+%Y-%m-%dT%H:%M:%S')
  if [ -z "$trino_pid" ]; then
    echo "$cur_time Server is not running"
    exit 1
  fi
  echo "$cur_time Server is running with pid $trino_pid"
  sleep 60 &
  wait $!
done
