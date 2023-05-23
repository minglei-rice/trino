#!/bin/bash

usage() {
  echo "Usage: $0 [-c <cluster_name>] [-u]"
  echo "Options:"
  echo -e "  -c <cluster_name> \t Load configuration for specified cluster"
  echo -e "  -u                \t Update configuration only"
  exit 1
}

set -e

# parse argument
while getopts "c:u" flag; do
  case "${flag}" in
    c) cluster=${OPTARG};;
    u) update_conf_only=true;;
    *) usage;;
  esac
done

cd "$(dirname "$0")"/cluster-env
if [ -n "$cluster" ]; then
  echo "Installing trino for cluster $cluster..."
  cluster_env_file=$cluster-env.sh
  if [ ! -f "$cluster_env_file" ]; then
    echo "Env file $cluster_env_file not found"
    exit 1
  fi
  source "$cluster_env_file"
fi
source default-env.sh
cd ..

chown -R "$TRINO_USER" .
scripts/install/create_directories.sh
sudo -u "$TRINO_USER" -E scripts/install/check_dependencies.sh
if [ "$update_conf_only" = true ]; then
  sudo -u "$TRINO_USER" -E scripts/install/update_conf.sh
else
  sudo -u "$TRINO_USER" -E scripts/install/install_trino.sh
  sudo -u "$TRINO_USER" -E scripts/install/update_conf.sh
  sudo -u "$TRINO_USER" -E scripts/install/prepare_env.sh
fi
