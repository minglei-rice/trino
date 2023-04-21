#!/bin/bash

source envs.sh

set -e -x

java -version

if ls | grep -q '\.jar$'; then
    cp *.jar $TRINO_BASE_DIR/
fi
