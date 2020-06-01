#!/bin/bash

sudo ./mvnw clean -Dmaven.repo.local="$HOME/simbiose/services-data/shannondb/.m2/repository/"
./mvnw clean -Dmaven.repo.local="$HOME/simbiose/services-data/shannondb/.m2/repository/"
