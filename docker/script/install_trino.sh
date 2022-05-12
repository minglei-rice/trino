#!/bin/bash

source envs.sh

set -e -x

TRINO_FILE=${TRINO_BASE_DIR}/trino-server*.tar.gz
TRINO_PACKAGE_DIR=${TRINO_BASE_DIR}/${TRINO_VERSION}
TRINO_SOFT_LINK=${TRINO_BASE_DIR}/trino

if [ -d $TRINO_ETC_DIR ]; then
    if [ -w $TRINO_ETC_DIR -a -r $TRINO_ETC_DIR ]; then
        echo "Tino data directory existed and is readable/writable."
    else
        chmod 777 -R $TRINO_ETC_DIR
    fi
else
    mkdir -p -m 777 $TRINO_ETC_DIR
fi

if [ -d $TRINO_DATA_DIR ]; then
    if [ -w $TRINO_DATA_DIR -a -r $TRINO_DATA_DIR ]; then
        echo "Tino data directory existed and is readable/writable."
    else
        chmod 777 -R $TRINO_DATA_DIR
    fi
else
    mkdir -p -m 777 $TRINO_DATA_DIR
fi

if [ -d $TRINO_LOG_DIR ]; then
    if [ -w $TRINO_LOG_DIR -a -r $TRINO_LOG_DIR ]; then
        echo "Tino log directory existed and is readable/writable."
    else
        chmod 777 -R $TRINO_LOG_DIR
    fi
else
    mkdir -p -m 777 $TRINO_LOG_DIR
fi

## decompress
if [[ -d $TRINO_PACKAGE_DIR && x"$TRINO_VERSION" != "x" ]]; then
	echo "Found $TRINO_PACKAGE_DIR, try to delete before installing $TRINO_FILE ..."
	rm -rf $TRINO_PACKAGE_DIR
fi
tar -zxf $TRINO_FILE --directory=${TRINO_BASE_DIR}

## soft link
if [ -f $TRINO_SOFT_LINK ]; then
	rm $TRINO_SOFT_LINK
fi
ln -s $TRINO_PACKAGE_DIR $TRINO_SOFT_LINK

## installing configurations
rm -rf ${TRINO_ETC_DIR}
cp -r ../conf/trino/* ${TRINO_ETC_DIR}

## copy kerbs keytab
cp ../conf/trino.keytab ${TRINO_BASE_DIR}
