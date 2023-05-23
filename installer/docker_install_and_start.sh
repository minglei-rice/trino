#!/bin/bash

set -e

export TRINO_USER=root
TRINO_VERSION=$(cat "$TRINO_HOME_BASE"/version)
export TRINO_VERSION

cd $(dirname "$0")
./install.sh

cd "$TRINO_HOME_BASE"
./switch_version.sh
./docker_start.sh
