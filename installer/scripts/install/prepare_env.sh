#!/bin/bash

echo "Preparing environment variables and scripts to launch Trino..."

cp scripts/trino-env.sh.template "$TRINO_HOME_BASE"/trino-env.sh
cp scripts/launcher.sh "$TRINO_HOME_BASE"
cp scripts/switch_version.sh "$TRINO_HOME_BASE"

cd "$TRINO_HOME_BASE"
chmod +x ./*.sh
sed -i "s#{TRINO_HOME_BASE}#$TRINO_HOME_BASE#" trino-env.sh
sed -i "s#{TRINO_VERSION}#$TRINO_VERSION#" trino-env.sh
sed -i "s#{TRINO_ETC_DIR}#$TRINO_ETC_DIR#" trino-env.sh
sed -i "s#{TRINO_DATA_DIR}#$TRINO_DATA_DIR#" trino-env.sh
sed -i "s#{TRINO_LOG_DIR}#$TRINO_LOG_DIR#" trino-env.sh
sed -i "s#{TRINO_JAVA_HOME}#$TRINO_JAVA_HOME#" trino-env.sh
sed -i "s#{TRINO_ENVIRONMENT}#$TRINO_ENVIRONMENT#" trino-env.sh
sed -i "s#{TRINO_SERVER_PORT}#$TRINO_SERVER_PORT#" trino-env.sh
