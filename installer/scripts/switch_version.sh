#!/bin/bash

cd $(dirname "$0")
source trino-env.sh

# validate whether trino with specified version has been installed
if [ -z "$TRINO_VERSION" ]; then
  echo "Please specify version of trino via TRINO_VERSION"
  exit 1
fi
install_dir=$TRINO_HOME_BASE/trino-server-$TRINO_VERSION
if [ ! -d "$install_dir" ]; then
  echo "$install_dir does not exist. Please check whether trino has been installed"
  exit 1
fi

# create soft link
if [ -e "$TRINO_HOME" ]; then
  rm -f "$TRINO_HOME"
fi
ln -s "$install_dir" "$TRINO_HOME"
