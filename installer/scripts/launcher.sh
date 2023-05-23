#!/bin/bash

cd $(dirname "$0")
source trino-env.sh

if [ ! -d "$TRINO_ETC_DIR" ]; then
  echo "Config directory $TRINO_ETC_DIR not found"
  exit 1
fi

if [ ! -d "$TRINO_DATA_DIR" ]; then
  echo "Data directory $TRINO_DATA_DIR not found"
  exit 1
fi

launcher --etc-dir="$TRINO_ETC_DIR" --data-dir="$TRINO_DATA_DIR" "$@"
