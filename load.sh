#!/bin/bash

set -ex

THREAD_NUM=16
REPLICATOR_ADDR="128.110.153.86:50050"
REPLICATOR_BATCH_SIZE=10
WORKLOAD=_test

for i in "$@"
do
case $i in
    -t=*|--threads=*)
    THREAD_NUM="${i#*=}"
    shift # pass argument=value
    ;;
    -r=*|--replicator_addr=*)
    REPLICATOR_ADDR="${i#*=}"
    shift # pass argument=value
    ;;
    -b=*|--replicator_batch_size=*)
    REPLICATOR_BATCH_SIZE="${i#*=}"
    shift # pass argument=value
    ;;
    -w=*|--workload=*)
    WORKLOAD="${i#*=}"
    shift # pass argument=value
    ;;
    --default)
    DEFAULT=YES
    shift # pass argument with no value
    ;;
    *)
          # unknown option
    ;;
esac
done

./bin/ycsb.sh load rocksdb -s \
  -P workloads/workload${WORKLOAD}\
  -p rocksdb.dir=/users/$USER/test \
  -p status.interval=1 \
  -threads ${THREAD_NUM} \
  -target 800000 \
  -replicator_addr ${REPLICATOR_ADDR} \
  -replicator_batch_size ${REPLICATOR_BATCH_SIZE}
