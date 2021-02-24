#!/bin/bash
for char in a b c d e f 
do 
	echo pin$char
	for i in {1..1}
	do
		./bin/ycsb load rocksdb -s -P workloads/workload$char -p rocksdb.dir=/tmp/ycsb-rocksdb-data -threads 8 >> out$char.txt
		./bin/ycsb run rocksdb -s -P workloads/workload$char -p rocksdb.dir=/tmp/ycsb-rocksdb-data -threads 8 >> out$char.txt
	done
	rm -rf /tmp/ycsb-rocksdb-data/
done
