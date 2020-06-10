#!/bin/bash

pushd /home/${USER}/simbiose/code/presto/presto-server-main/;
export MAVEN_OPTS="-ea \
    -XX:+UseG1GC \
    -XX:G1HeapRegionSize=32M \
    -XX:+UseGCOverheadLimit \
    -XX:+ExplicitGCInvokesConcurrent \
    -Xmx512M \
    -Dconfig=/home/${USER}/simbiose/code/presto/configs/config.properties \
    -Dlog.levels-file=/home/${USER}/simbiose/code/presto/presto-server-main/etc/log.properties \
    -Djdk.attach.allowAttachSelf=true \
    -Dhive.metastore.uri=thrift://localhost:9083 \
    -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005"; \
mvn exec:java -Dexec.mainClass="io.prestosql.server.PrestoServer";
popd;
