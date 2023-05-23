#!/bin/bash

# Check whether the given file or directory exists and is readable
# Arguments:
#   $1: file path
#   $2: file name for printed message
function check_readable() {
  if [ ! -r "$1" ]; then
    echo "$2 $1 does not exist or is not readable"
    exit 1
  fi
}

echo "Checking Trino dependencies..."

# check java
check_readable "$TRINO_JAVA_HOME" "Java home"
echo "Java version used in Trino: $("$TRINO_JAVA_HOME"/bin/java --version | head -n 1)"

# check exporter
check_readable "$TRINO_JMX_EXPORTER" "Jmx exporter"

# check keytab
check_readable "$TRINO_KEYTAB" "Trino keytab"
