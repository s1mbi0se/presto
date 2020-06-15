#!/bin/bash

# get presto home dir
PRESTO_HOME=/simbiose/presto_user/presto-server-${PRESTO_VERSION}

set -o allexport; source ${PRESTO_HOME}/configs/environment_variables.txt; set +
# go to presto root folder
pushd ${PRESTO_HOME}

# go to scripts folder
pushd ./scripts/shell/;
# split configuration files
./config_splitter.sh ${PRESTO_HOME}/configs config.properties
popd;

# make config directories
mkdir -p ${PRESTO_HOME}/etc/catalog
# move tmp configuration files to destination
mv ./configs/config.properties.tmp ${PRESTO_HOME}/etc/config.properties
mv ./configs/node.properties.tmp ${PRESTO_HOME}/etc/node.properties
mv ./configs/jvm.properties.tmp ${PRESTO_HOME}/etc/jvm.properties
mv ./configs/log.properties.tmp ${PRESTO_HOME}/etc/log.properties
mv ./configs/api-config.properties.tmp ${PRESTO_HOME}/etc/catalog/api-config.properties
mv ./configs/shannondb.properties.tmp ${PRESTO_HOME}/etc/catalog/shannondb.properties
popd;

# go to presto home directory
pushd ${PRESTO_HOME};
# run server
echo 'running server'
./bin/launcher run
popd;
popd;
