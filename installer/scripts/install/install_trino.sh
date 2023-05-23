#!/bin/bash

echo "Installing trino with version $TRINO_VERSION..."

install_dir=$TRINO_HOME_BASE/trino-server-$TRINO_VERSION
tar_file=$TRINO_HOME_BASE/trino-server-$TRINO_VERSION.tar.gz

# remove previous installation if exists
if [ -d "$install_dir" ]; then
	echo "Removing existing installation in $install_dir..."
	rm -rf "$install_dir"
fi

# extract trino
tar -zxf "$tar_file" --directory="$TRINO_HOME_BASE"
