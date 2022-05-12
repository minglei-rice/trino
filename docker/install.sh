#!/bin/bash
## Notice:
## If you want install trino or dependencies independently, please
## enter into ./script to launch the correspoding bash file.
##

set -e -x

SCRIPTS_DIR=$TRINO_BASE_DIR/docker/script

## install
cd $SCRIPTS_DIR
./install_dependencies.sh
./install_trino.sh

