#!/bin/bash

set -o allexport; source /simbiose/presto_user/configs/environment_variables.txt; set +
# go to presto root folder
pushd /simbiose/presto_user/
# get presto home dir
PRESTO_HOME=$(ls -d */ | grep presto-server | head -n 1)

# go to scripts folder
pushd ./scripts/shell/;
# split configuration files
./config_splitter.sh /simbiose/presto_user/configs config.properties
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
