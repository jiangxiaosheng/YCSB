#!/bin/bash
./bin/ycsb.sh run rocksdb -s -P workloads/workload$1 -p rocksdb.dir=/tmp/ycsb-rocksdb-data -threads $2 > shard-$1.txt

