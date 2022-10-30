#!/bin/bash

SCRIPT_PATH=$(dirname $(realpath -s $0))

docker run --rm \
  -v $SCRIPT_PATH/data:/data \
  -v $SCRIPT_PATH/src:/app \
   ambientweather

cd $SCRIPT_PATH/data && sqlite3 ambientweather.db 'select date,tempf, temp1f, temp4f from dbtable order by dateutc desc limit 1;' >> temps.log
