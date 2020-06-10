#!/bin/bash

pushd /simbiose/presto_user/presto-server/;
./bin/launcher run --config=/simbiose/configs/config.properties
popd;
