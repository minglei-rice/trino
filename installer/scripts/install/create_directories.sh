#!/bin/bash

# Create directory and set owner if the given owner is not root
# Arguments:
#   $1: dir path
#   $2: dir owner
#   $3: dir name for printed message
function create_directory() {
  if [ ! -d "$1" ]; then
    echo "Creating $3 $1 since it does not exist"
    mkdir -p -m 755 "$1"
    chown -R "$2" "$1"
  fi
}

echo "Creating Trino directories..."

create_directory "$TRINO_HOME_BASE" "$TRINO_USER" "Trino base directory"
create_directory "$TRINO_DATA_DIR" "$TRINO_USER" "Trino data directory"
create_directory "$TRINO_ETC_DIR" "$TRINO_USER" "Trino config directory"
