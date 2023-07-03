#!/bin/bash

# Get formatted boolean value from given original value.
# Return default value if original value is empty.
# Arguments:
#   $1: original value
#   $2: default value, "true" or "false"
function format_boolean() {
  val=$1
  if [ -z "$1" ]; then
    val=$2
  fi
  if [ "$(echo "$val" | tr '[:upper:]' '[:lower:]')" == true ]; then
    echo true
  else
    echo false
  fi
}

export TRINO_USER=${TRINO_USER:-trino}

export TRINO_ENVIRONMENT=${TRINO_ENVIRONMENT:-unknown}
if [ -n "$DEPLOY_ENV" ]; then
  # this imply that node is started in docker
  export TRINO_ENVIRONMENT="$DEPLOY_ENV"_docker
fi

# directories
export TRINO_HOME_BASE=${TRINO_HOME_BASE:-/data/app/trino}
export TRINO_HOME=${TRINO_HOME_BASE:-$TRINO_HOME_BASE/trino}
export TRINO_ETC_DIR=${TRINO_ETC_DIR:-$TRINO_HOME_BASE/etc}

TRINO_DATA_DIR=${TRINO_DATA_DIR:-/data/log/trino}
if [[ $TRINO_ENVIRONMENT == *docker ]]; then
  TRINO_DATA_DIR=$TRINO_DATA_DIR/$TRINO_ENVIRONMENT-$(hostname -s)
else
  TRINO_DATA_DIR=$TRINO_DATA_DIR/$TRINO_ENVIRONMENT-physical
fi
export TRINO_DATA_DIR
export TRINO_LOG_DIR=${TRINO_LOG_DIR:-$TRINO_DATA_DIR/var/log}

# dependencies
export TRINO_JAVA_HOME=${TRINO_JAVA_HOME:-$TRINO_HOME_BASE/jdk}
export TRINO_JMX_EXPORTER=${TRINO_JMX_EXPORTER:-$TRINO_HOME_BASE/jmx_prometheus_javaagent.jar}
export TRINO_KEYTAB=${TRINO_KEYTAB:-/etc/security/keytabs/trino.keytab}

# trino version
export TRINO_VERSION=${TRINO_VERSION:-400}

# coordinator
if [ -z "$TRINO_COORDINATOR_HOSTNAME" ]; then
  echo "Please specify TRINO_COORDINATOR_HOSTNAME"
  exit 1
fi
export TRINO_COORDINATOR_PORT=${TRINO_COORDINATOR_PORT:-9090}

# server
export TRINO_SERVER_PORT=${TRINO_SERVER_PORT:-9090}

# cluster name for prometheus metrics
export TRINO_CLUSTER_NAME=${TRINO_CLUSTER_NAME:-$TRINO_COORDINATOR_HOSTNAME}

# alluxio cache
export ALLUXIO_MASTER_HOSTNAME=${ALLUXIO_MASTER_HOSTNAME:-jscs-olap-presto-01}
export ALLUXIO_MASTER_PORT=${ALLUXIO_MASTER_PORT:-19998}
export ALLUXIO_MASTER=$ALLUXIO_MASTER_HOSTNAME:$ALLUXIO_MASTER_PORT
export ALLUXIO_CACHE=${ALLUXIO_CACHE:-INDEX}

# jvm.config
export JVM_XMS=${JVM_XMS:-256G}
export JVM_XMX=${JVM_XMX:-256G}
export JVM_XSS=${JVM_XSS:-8M}
export JVM_RESERVED_CODE_CACHE_SIZE=${JVM_RESERVED_CODE_CACHE_SIZE:-2G}
export JVM_META_SPACE_SIZE=${JVM_META_SPACE_SIZE:-1G}
export JVM_MAX_META_SPACE_SIZE=${JVM_MAX_META_SPACE_SIZE:-2G}

# config.properties
SCHEDULER_INCLUDE_COORDINATOR=$(format_boolean "$SCHEDULER_INCLUDE_COORDINATOR" false)
export SCHEDULER_INCLUDE_COORDINATOR
export QUERY_MAX_MEMORY=${QUERY_MAX_MEMORY:-2320GB}
export QUERY_MAX_MEMORY_PER_NODE=${QUERY_MAX_MEMORY_PER_NODE:-80GB}
export QUERY_MAX_SCAN_PHYSICAL_BYTES=${QUERY_MAX_SCAN_PHYSICAL_BYTES:-10TB}
export EXCHANGE_MAX_CONTENT_LENGTH=${EXCHANGE_MAX_CONTENT_LENGTH:-256MB}
export TASK_CONCURRENCY=${TASK_CONCURRENCY:-16}
