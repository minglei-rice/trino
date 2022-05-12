#!/bin/bash

set -e -x

TRINO_TEMPLATE_CONF_DIR=${TRINO_TEMPLATE_CONF_DIR:-$TRINO_BASE_DIR/docker/conf}

## init properties

if [ "`echo "${TRINO_COORDINATOR}" | tr '[:upper:]' '[:lower:]'`" = "true" ];
then
    cat $TRINO_TEMPLATE_CONF_DIR/trino/etc/config.properties-coordinator > ${TRINO_ETC_DIR}/config.properties
    cat $TRINO_TEMPLATE_CONF_DIR/trino/etc/jmx_exporter_config.yaml-coordinator > ${TRINO_ETC_DIR}/jmx_exporter_config.yaml
else
    cat $TRINO_TEMPLATE_CONF_DIR/trino/etc/config.properties-worker > ${TRINO_ETC_DIR}/config.properties
    cat $TRINO_TEMPLATE_CONF_DIR/trino/etc/jmx_exporter_config.yaml-worker > ${TRINO_ETC_DIR}/jmx_exporter_config.yaml
fi

## reset trino confs from env
sed --in-place "s/{SERVER_HTTP_PORT}/${SERVER_HTTP_PORT:-9090}/" ${TRINO_ETC_DIR}/config.properties
sed --in-place "s#{TRINO_DISCOVERY_HOST}#${TRINO_DISCOVERY_HOST:-127.0.0.1}#" ${TRINO_ETC_DIR}/config.properties
sed --in-place "s/{TRINO_DISCOVERY_PORT}/${TRINO_DISCOVERY_PORT:-9090}/" ${TRINO_ETC_DIR}/config.properties
sed --in-place "s/{QUERY_MAX_MEMORY}/${QUERY_MAX_MEMORY:-1960GB}/" ${TRINO_ETC_DIR}/config.properties
sed --in-place "s/{QUERY_MAX_MEMORY_PER_NODE}/${QUERY_MAX_MEMORY_PER_NODE:-70GB}/" ${TRINO_ETC_DIR}/config.properties
sed --in-place "s/{QUERY_MAX_TOTAL_MEMORY_PER_NODE}/${QUERY_MAX_TOTAL_MEMORY_PER_NODE:-80GB}/" ${TRINO_ETC_DIR}/config.properties
sed --in-place "s/{TASK_MAX_WORKER_THREADS}/${TASK_MAX_WORKER_THREADS:-32}/" ${TRINO_ETC_DIR}/config.properties
sed --in-place "s/{TASK_CONCURRENCY}/${TASK_CONCURRENCY:-16}/" ${TRINO_ETC_DIR}/config.properties

## reset catalog confs from env
sed -i "s#{TRINO_BASE_DIR}#${TRINO_BASE_DIR}#g" ${TRINO_ETC_DIR}/catalog/iceberg.properties
sed -i "s#{TRINO_ETC_DIR}#${TRINO_ETC_DIR}#g" ${TRINO_ETC_DIR}/catalog/iceberg.properties
sed -i "s#{TRINO_BASE_DIR}#${TRINO_BASE_DIR}#g" ${TRINO_ETC_DIR}/catalog/hive.properties
sed -i "s#{TRINO_ETC_DIR}#${TRINO_ETC_DIR}#g" ${TRINO_ETC_DIR}/catalog/hive.properties
sed -i "s#{TRINO_ETC_DIR}#${TRINO_ETC_DIR}#g" ${TRINO_ETC_DIR}/core-site.xml

## reset jvm confs from env
JVM_CONF_FILE=${TRINO_ETC_DIR}/jvm.config
cat $TRINO_TEMPLATE_CONF_DIR/trino/etc/jvm.config.template > $JVM_CONF_FILE

sed -i "s#{TRINO_BASE_DIR}#${TRINO_BASE_DIR}#g" $JVM_CONF_FILE
sed -i "s#{TRINO_ETC_DIR}#${TRINO_ETC_DIR}#g" $JVM_CONF_FILE
sed -i "s#{TRINO_LOG_DIR}#${TRINO_LOG_DIR}#g" $JVM_CONF_FILE
sed -i "s#{JVM_XMS}#${JVM_XMS:-256G}#g" $JVM_CONF_FILE
sed -i "s#{JVM_XMX}#${JVM_XMX:-256G}#g" $JVM_CONF_FILE
sed -i "s#{JVM_XSS}#${JVM_XSS:-8M}#g" $JVM_CONF_FILE
sed -i "s#{JVM_RESERVED_CODE_CACHE_SIZE}#${JVM_RESERVED_CODE_CACHE_SIZE:-2G}#g" $JVM_CONF_FILE
sed -i "s#{HADOOP_USER_NAME}#${HADOOP_USER_NAME:-trino}#g" $JVM_CONF_FILE

## alluxio
if [ "${ALLUXIO_CACHE}" != "true" ]; then
    ALLUXIO_CACHE=false
fi
sed -i "s#{ALLUXIO_CACHE}#${ALLUXIO_CACHE}#g" ${TRINO_ETC_DIR}/hdfs-site.xml
