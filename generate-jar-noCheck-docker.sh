#!/bin/bash

./mvnw -pl '!presto-server-rpm,!presto-docs' clean install \
    -TC2 -DskipTests -Dmaven.javadoc.skip=true -Dmaven.source.skip=true -Dair.check.skip-all=true
