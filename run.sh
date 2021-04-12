#!/bin/bash

set -ex

THREAD_NUM=16
REPLICATOR_ADDR="128.110.153.141:50051"
REPLICATOR_BATCH_SIZE=10

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
    --default)
    DEFAULT=YES
    shift # pass argument with no value
    ;;
    *)
          # unknown option
    ;;
esac
done

./bin/ycsb.sh run rocksdb -s \
  -P workloads/workload1 \
  -p rocksdb.dir=/users/$USER/test \
  -threads ${THREAD_NUM} \
  -replicator_addr ${REPLICATOR_ADDR} \
  -replicator_batch_size ${REPLICATOR_BATCH_SIZE}
