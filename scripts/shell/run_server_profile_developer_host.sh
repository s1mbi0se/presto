#!/bin/bash

PRESTO_HOME=~/simbiose/code/presto/
pushd ${PRESTO_HOME};

# go to scripts folder
pushd ~/simbiose/scripts/shell/presto/;

# split configuration files
./config_splitter.sh ~/simbiose/configs/presto/ config.properties
popd;

# make config directories
mkdir -p ${PRESTO_HOME}/presto-server-main/etc/catalog
# move tmp configuration files to destination
mv ~/simbiose/configs/config.properties.tmp ${PRESTO_HOME}/presto-server-main/etc/config.properties
mv ~/simbiose/configs/node.properties.tmp ${PRESTO_HOME}/presto-server-main/etc/node.properties
mv ~/simbiose/configs/jvm.config.tmp ${PRESTO_HOME}/presto-server-main/etc/jvm.config
mv ~/simbiose/configs/log.properties.tmp ${PRESTO_HOME}/presto-server-main/etc/log.properties
mv ~/simbiose/configs/api-config.properties.tmp ${PRESTO_HOME}/presto-server-main/etc/catalog/api-config.properties
mv ~/simbiose/configs/shannondb.properties.tmp ${PRESTO_HOME}/presto-server-main/etc/catalog/shannondb.properties

pushd ${PRESTO_HOME}/presto-server-main/;
export MAVEN_OPTS="-ea \
    -XX:+UseG1GC \
    -XX:G1HeapRegionSize=32M \
    -XX:+UseGCOverheadLimit \
    -XX:+ExplicitGCInvokesConcurrent \
    -Xmx512M \
    -Dconfig=${PRESTO_HOME}/presto-server-main/etc/config.properties \
    -Dlog.levels-file=${PRESTO_HOME}/presto-server-main/etc/log.properties \
    -Djdk.attach.allowAttachSelf=true \
    -Dhive.metastore.uri=thrift://localhost:9083 \
    -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005"; \
mvn exec:java -Dexec.mainClass="io.prestosql.server.PrestoServer";
popd;
