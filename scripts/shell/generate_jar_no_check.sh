#!/bin/bash

./mvnw -pl '!presto-server-rpm,!presto-docs,!presto-proxy,!presto-testing-server-launcher,!presto-verifier' \
	clean install -TC2 \
	-DskipTests \
	-Dmaven.javadoc.skip=true \
	-Dmaven.source.skip=true \
	-Dair.check.skip-all=true \
    -offline
