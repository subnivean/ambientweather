#!/bin/bash

TESLA_DATA_PATH=$(dirname $(realpath -s ./data/))
SCRIPT_PATH=$(dirname $(realpath -s $0))

docker run --rm -it \
  -v $SCRIPT_PATH/data:/data \
  -v $SCRIPT_PATH/src:/app \
  -v $TESLA_DATA_PATH:/tesla_data \
   ambientweather-2 /bin/bash

