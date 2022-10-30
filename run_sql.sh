#!/bin/bash

SCRIPT_PATH=$(dirname $(realpath -s $0))

docker run --rm \
   -v $SCRIPT_PATH/data:/data \
   ambientweather \
   sqlite3 -readonly -box ../data/ambientweather.db \
     "select
         date,tempf,temp1f,temp4f
      from
         dbtable
      where
         dateutc > (
               select
                 dateutc
               from
                 dbtable
               order by
                 dateutc
               desc limit 1)
                 - 60 * 40 * 1000"

