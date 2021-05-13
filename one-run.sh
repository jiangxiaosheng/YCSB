#!/bin/bash
PREFIX=$1
LOAD=$2
bash load.sh -w=$char > ${PREFIX}${LOAD}.txt
bash run.sh -w=$char >> ${PREFIX}${LOAD}.txt
