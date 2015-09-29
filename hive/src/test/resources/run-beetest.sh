#!/bin/bash

if [[ $# -ne 1 ]]; then
  echo "Usage: "
  echo "> export BEETEST_BUILD_PATH=<path-to-beetest-target-dir>"
  echo "> export EXHIBIT_HIVE_JAR=<path-to-exhibit-hive-jar>"
  echo "> $0 <test-case-folder>"
  exit 1
fi

if [[ -z ${BEETEST_BUILD_PATH+x} ]]; then
  echo "Variable BEETEST_BUILD_PATH not set"
  exit 2
fi

if [[ -z ${EXHIBIT_HIVE_JAR+x} ]]; then
  echo "Variable EXHIBIT_HIVE_JAR not set"
  exit 3
fi

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
CONFIG=$SCRIPT_DIR/local-config/hive-site.xml
DELETE_TEST_DIR_ON_EXIT=FALSE
USE_MINI_CLUSTER=TRUE
TEST_CASE=$1

if [[ ! -d "$SCRIPT_DIR/local-config" ]]; then
  echo "Hive config directory 'local-config' not found"
  exit 4
fi

EXPANDED_CONF=/tmp/exhibit-beetest-hive-site.xml
cat $CONFIG | sed -e "s@HIVE_AUX_JAR@${EXHIBIT_HIVE_JAR}@g" > $EXPANDED_CONF

CP=${EXHIBIT_HIVE_JAR}:$(find `pwd` ${BEETEST_BUILD_PATH} -name "*.jar" | tr "\n" ":")
java -cp $CP                            \
  -Dhadoop.root.logger=ERROR,console    \
  com.spotify.beetest.TestQueryExecutor \
  ${TEST_CASE} ${EXPANDED_CONF} ${USE_MINI_CLUSTER} ${DELETE_TEST_DIR_ON_EXIT} \
  2>&1 | grep -v MetricsSystemImpl
