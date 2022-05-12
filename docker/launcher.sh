#!/bin/bash

TRINO_SCRIPT_DIR=$TRINO_BASE_DIR/docker/script

source $TRINO_SCRIPT_DIR/envs.sh
source $TRINO_SCRIPT_DIR/init_conf.sh

if [ x"${TRINO_ETC_DIR}" == "x" ]; then
        echo "You should specifiy a valid etc directory, which is isolated from Trino install path."
        exit 100
fi

if [ x"${TRINO_DATA_DIR}" == "x" ]; then
        echo "You should specifiy a valid node.data-dir directory, which is isolated from Trino install path."
        exit 100
fi

export PATH=${TRINO_BASE_DIR}/trino/bin:$PATH

launcher --etc-dir=${TRINO_ETC_DIR} --data-dir=${TRINO_DATA_DIR} --server-log-file=${TRINO_LOG_DIR}/var/log/server.log --launcher-log-file=${TRINO_LOG_DIR}/var/log/launcher.log  "$@"
