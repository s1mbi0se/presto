#!/usr/bin/env bash

CONFIG_DIR=$1
CONFIG_FILE=$2

pushd ${CONFIG_DIR}
awk 'BEGIN{
        FILENAMES[0]="config.properties.tmp";
        FILENAMES[1]="node.properties.tmp";
        FILENAMES[2]="jvm.config.tmp";
        FILENAMES[3]="log.properties.tmp"
        FILENAMES[4]="api-config.properties.tmp"
        FILENAMES[5]="shannondb.properties.tmp"
     }
     NR%2==0{ print > FILENAMES[i++] }' RS='"' ${CONFIG_FILE}
popd;
