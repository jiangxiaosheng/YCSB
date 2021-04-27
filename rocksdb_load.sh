#---------------------------------------------------------------------------------------
WORKLOAD="workloads/workloada"
#---------------------------------------------------------------------------------------
ROCKSDB_DIR="/tmp/ycsb-rocksdb-data"
SST_DIR="/mnt/sdb/archive_dbs/sst_dir"
ROCKSDB_CONFIG_FILE="./rocksdb/rocksdb_config.ini"
#---------------------------------------------------------------------------------------
LOAD_OUT_FILE="load_out.txt"
TOP_OUT_FILE="cpu_usage.txt"

rm -rf $ROCKSDB_DIR
rm ${SST_DIR}/*

function remove_or_touch {
    if [ -f $1 ]; then
        rm $1
    fi
    touch $1
}

echo "remove or touch output file"
remove_or_touch $LOAD_OUT_FILE
remove_or_touch $TOP_OUT_FILE

#trim ssd
fstrim -v /

# Writes data buffered in memory out to disk, then clear memory cache(page cache).
sudo -S sync; echo 1 | sudo tee /proc/sys/vm/drop_caches

{ ./bin/ycsb load rocksdb -s \
-P ${WORKLOAD} \
-p rocksdb.dir=${ROCKSDB_DIR} \
-p rocksdb.optionsfile=${ROCKSDB_CONFIG_FILE} \
-p rocksdb.sstdir="${SST_DIR}/" \
-threads 8 \
-p hdrhistogram.percentiles=5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95,99,99.9 \
| tee $LOAD_OUT_FILE; } &


{ top -u root -b -d 0.2 -o +%CPU -w 512 | grep "java" --line-buffered >> $TOP_OUT_FILE; } &
wait -n

echo -n "CPU Usage : "
awk '{ total += $9 } END { print total/NR }' $TOP_OUT_FILE
kill 0