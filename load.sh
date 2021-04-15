./bin/ycsb load rocksdb -s -P workloads/workloada \
-p rocksdb.dir=/tmp/ycsb-rocksdb-data \
-p rocksdb.optionsfile=./rocksdb/rocksdb_config.ini \
-threads 12 \
-p hdrhistogram.percentiles=5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95,99,99.9 