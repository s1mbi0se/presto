#!/bin/bash

###########################################
# Example:
# $ ./generate_jar_no_check_partial.sh presto-main
###########################################

./mvnw clean install -TC2 \
	-DskipTests \
	-Dmaven.javadoc.skip=true \
	-Dmaven.source.skip=true \
	-Dair.check.skip-all=true \
    -offline \
    -pl $1 -amd
