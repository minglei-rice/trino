#!/bin/bash

echo "Updating trino configuration..."

echo "Removing existing config files in $TRINO_ETC_DIR..."
rm -rf "${TRINO_ETC_DIR:?}/"*

echo "Copying config files to $TRINO_ETC_DIR..."
cp -r conf/* "$TRINO_ETC_DIR"
cd "$TRINO_ETC_DIR"

# rename templates
role="worker"
if [ "$TRINO_COORDINATOR_HOSTNAME" == "$(hostname -s)" ] || [ "$TRINO_COORDINATOR_HOSTNAME" == "127.0.0.1" ] || [ "$TRINO_COORDINATOR_HOSTNAME" == "localhost" ] ; then
  role="coordinator"
fi
templates=$(ls ./*.$role.template)
for template in $templates; do
  mv "$template" "${template%".$role.template"}"
done
rm -f ./*.coordinator.template
rm -f ./*.worker.template
templates=$(ls ./*.template)
for template in $templates; do
  mv "$template" "${template%".template"}"
done

# set catalog config from env
sed -i "s#{TRINO_ETC_DIR}#$TRINO_ETC_DIR#g" catalog/*.properties
sed -i "s#{TRINO_KEYTAB}#$TRINO_KEYTAB#g" catalog/*.properties

# set hadoop config from env
sed -i "s#{TRINO_ETC_DIR}#$TRINO_ETC_DIR#g" ./*.xml
sed -i "s#{ALLUXIO_MASTER}#$ALLUXIO_MASTER#g" ./*.xml
sed -i "s#{ALLUXIO_CACHE}#$ALLUXIO_CACHE#g" ./*.xml

# set jvm config from env
sed -i "s#{JVM_XMS}#$JVM_XMS#g" jvm.config
sed -i "s#{JVM_XMX}#$JVM_XMX#g" jvm.config
sed -i "s#{JVM_XSS}#$JVM_XSS#g" jvm.config
sed -i "s#{JVM_RESERVED_CODE_CACHE_SIZE}#$JVM_RESERVED_CODE_CACHE_SIZE#g" jvm.config
sed -i "s#{JVM_META_SPACE_SIZE}#$JVM_META_SPACE_SIZE#g" jvm.config
sed -i "s#{JVM_MAX_META_SPACE_SIZE}#$JVM_MAX_META_SPACE_SIZE#g" jvm.config

sed -i "s#{TRINO_ETC_DIR}#$TRINO_ETC_DIR#g" jvm.config
sed -i "s#{TRINO_DATA_DIR}#$TRINO_DATA_DIR#g" jvm.config
sed -i "s#{TRINO_LOG_DIR}#$TRINO_LOG_DIR#g" jvm.config
sed -i "s#{TRINO_JMX_EXPORTER}#$TRINO_JMX_EXPORTER#g" jvm.config

# set jmx exporter config from env
sed -i "s#{TRINO_CLUSTER_NAME}#$TRINO_CLUSTER_NAME#g" jmx_exporter_config.yaml

# set config properties from env
sed -i "s#{TRINO_ENVIRONMENT}#$TRINO_ENVIRONMENT#g" config.properties
sed -i "s#{SCHEDULER_INCLUDE_COORDINATOR}#$SCHEDULER_INCLUDE_COORDINATOR#g" config.properties
sed -i "s#{TRINO_SERVER_PORT}#$TRINO_SERVER_PORT#g" config.properties
sed -i "s#{TRINO_COORDINATOR_HOSTNAME}#$TRINO_COORDINATOR_HOSTNAME#g" config.properties
sed -i "s#{TRINO_COORDINATOR_PORT}#$TRINO_COORDINATOR_PORT#g" config.properties
sed -i "s#{QUERY_MAX_MEMORY}#$QUERY_MAX_MEMORY#g" config.properties
sed -i "s#{QUERY_MAX_MEMORY_PER_NODE}#$QUERY_MAX_MEMORY_PER_NODE#g" config.properties
sed -i "s#{QUERY_MAX_SCAN_PHYSICAL_BYTES}#$QUERY_MAX_SCAN_PHYSICAL_BYTES#g" config.properties
sed -i "s#{EXCHANGE_MAX_CONTENT_LENGTH}#$EXCHANGE_MAX_CONTENT_LENGTH#g" config.properties
sed -i "s#{TASK_CONCURRENCY}#$TASK_CONCURRENCY#g" config.properties
