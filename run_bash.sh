#!/bin/bash

TESLA_DATA_PATH=$(dirname $(realpath -s ./data/))
SCRIPT_PATH=$(dirname $(realpath -s $0))

docker run --rm -it \
  -v $SCRIPT_PATH/data:/data \
  -v $SCRIPT_PATH/src:/app \
  -v $SCRIPT_PATH/ipython:/root/.ipython \
  -v $TESLA_DATA_PATH:/tesla_data \
   allinone-py311 /bin/bash --rcfile /bashrc

