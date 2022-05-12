#!/bin/bash

source envs.sh

set -e -x

java -version

cp *.jar $TRINO_BASE_DIR/
