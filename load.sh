#!/bin/bash
rm /mnt/sdb/archive_dbs/*
cgexec -g memory:9 bin/ycsb.sh load rocksdb -s -P workloads/workloada -p rocksdb.dir=/mnt/sdb/archive_dbs
# bin/ycsb.sh load rocksdb -s -P workloads/workloada -p rocksdb.dir=/mnt/sdb/archive_dbs
