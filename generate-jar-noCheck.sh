#!/bin/bash

echo 'fix .m2 permissions'
sudo chown -R $USER:$USER $HOME/simbiose/services-data/shannondb/.m2/repository

echo 'updating presto'
git pull
git submodule sync && git submodule update --init --recursive --remote

echo 'building presto'
sudo ./mvnw clean -Dmaven.repo.local="$HOME/simbiose/services-data/shannondb/.m2/repository/"
./mvnw -pl '!presto-server-rpm,!presto-docs' clean install \
    -TC2 -DskipTests -Dmaven.javadoc.skip=true -Dmaven.source.skip=true -Dair.check.skip-all=true \
    -Dmaven.repo.local="$HOME/simbiose/services-data/shannondb/.m2/repository/"
