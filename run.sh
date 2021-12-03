# rm /mnt/sdb/archive_dbs/*
cgexec -g memory:9 bin/ycsb.sh run rocksdb -s -P workloads/workloada -p rocksdb.dir=/mnt/sdb/archive_dbs
# bin/ycsb.sh run rocksdb -s -P workloads/workloada -p rocksdb.dir=/mnt/sdb/archive_dbs
