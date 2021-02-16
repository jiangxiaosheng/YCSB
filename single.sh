#!/bin/bash
for char in b c d e f
do 
	echo pin$char
	for i in {1..3}
	do
		./bin/ycsb load rocksdb -s -P workloads/workload$char -p rocksdb.dir=/tmp/ycsb-rocksdb-data >> out$char.txt
		./bin/ycsb run rocksdb -s -P workloads/workload$char -p rocksdb.dir=/tmp/ycsb-rocksdb-data >> out$char.txt
	done
	rm -rf /tmp/ycsb-rocksdb-data/
done
