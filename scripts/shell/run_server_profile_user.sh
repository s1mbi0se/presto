#!/bin/bash

set -o allexport; source /simbiose/configs/environment_variables.txt; set +
PRESTO_HOME=/simbiose/presto_user/presto-server/
alias python=python3
# go to presto root folder
pushd ${PRESTO_HOME};

# go to scripts folder
pushd /simbiose/scripts/shell/;

# split configuration files
./config_splitter.sh /simbiose/configs config.properties
popd;

# make config directories
mkdir -p ${PRESTO_HOME}/etc/catalog
# move tmp configuration files to destination
mv /simbiose/configs/config.properties.tmp ${PRESTO_HOME}/etc/config.properties
sed -i 's/plugin.bundles=\\//g' ${PRESTO_HOME}/etc/config.properties
sed -i 's/^..\/.*//g' ${PRESTO_HOME}/etc/config.properties
mv /simbiose/configs/node.properties.tmp ${PRESTO_HOME}/etc/node.properties
mv /simbiose/configs/jvm.config.tmp ${PRESTO_HOME}/etc/jvm.config
mv /simbiose/configs/log.properties.tmp ${PRESTO_HOME}/etc/log.properties
mv /simbiose/configs/api-config.properties.tmp ${PRESTO_HOME}/etc/catalog/api-config.properties
mv /simbiose/configs/shannondb.properties.tmp ${PRESTO_HOME}/etc/catalog/shannondb.properties

# run server
echo 'running server'
./bin/launcher run
popd;
