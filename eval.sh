#!/bin/bash

set -x

if [ $# -lt 5 ]; then
    echo "Usage: bash eval.sh replicator_port mode(run or load) workload target_rate ip_0 ... ip_N"
    exit
fi

port=$1
mode=$2
workload=$3
rate=$4
shift 4
shard_num=$#

# kill zumbie replicator, heads, and tails
bash kill.sh $*
sleep 5

# start nodes from tail to head
ip=($*)
replicator_args=''
sleep_ms=1000
# cp the dataset in parallel
for i in $(seq 1 $shard_num)
do
    for j in $(seq 1 $#)
    do
        # ssh ${USER}@${ip[$j-1]} "cd /mnt/sdb/; rm -rf rocksdb-${i} > /dev/null 2>&1; nohup cp -r rocksdb-${i}-backup rocksdb-${i} > /dev/null & echo \$! > rocksdb-${i}-pid.txt"
        ssh ${USER}@${ip[$j-1]} "cd /mnt/sdb/; sudo rm -rf rocksdb-${i} > /dev/null 2>&1; nohup cp -r rocksdb-${i}-backup rocksdb-${i} > /dev/null & echo \$! > rocksdb-${i}-pid.txt"
    done
done

for i in $(seq 1 $shard_num)
do
    cur_port=`expr 8980 + $i`
    pre_port=`expr 8979 + $i`
    # start nodes in background
    for j in $(seq 1 $#)
    do
        cur_ip=${ip[$j-1]}
        pre_ip=${ip[$j-2]}
        case $i in
            1)
            replicator_args='-p tail'${j}'='${cur_ip}:${cur_port}' '$replicator_args
            ssh ${USER}@${cur_ip} "cd YCSB-${i}; nohup bash node.sh /mnt/sdb/rocksdb-${i} ${cur_port} tail null $sleep_ms > nohup.out 2>&1 & echo \$! > pid.txt"
            ;;

            $shard_num)
            chain=`expr $j - $shard_num + 1`
            if [ $chain -lt 1 ]
            then
                chain=`expr $chain + $shard_num`
            fi
            replicator_args='-p head'${chain}'='${cur_ip}:${cur_port}' '$replicator_args
            ssh ${USER}@${cur_ip} "cd YCSB-${i}; nohup bash node.sh /mnt/sdb/rocksdb-${i} ${cur_port} head ${pre_ip}:${pre_port} $sleep_ms > nohup.out 2>&1 & echo \$! > pid.txt"
            ;;

            *)
            ssh ${USER}@${cur_ip} "cd YCSB-${i}; nohup bash node.sh /mnt/sdb/rocksdb-${i} ${cur_port} mid ${pre_ip}:${pre_port} $sleep_ms > nohup.out 2>&1 & echo \$! > pid.txt"
            ;;
        esac
    done
    # wait for current nodes to be ready, then we start the next round of nodes
    for j in $(seq 1 $#)
    do
        cur_ip=${ip[$j-1]}
        ssh ${USER}@${cur_ip} "cd YCSB-${i}; until grep 'Server started' nohup.out > /dev/null; do sleep 1; done"
    done
done

echo $replicator_args
# start the replicator
./bin/ycsb.sh replicator rocksdb -s -P workloads/workloada -p port=$port -p shard=$shard_num $replicator_args > replicator.out 2>&1 &

# start iostat
for j in $(seq 1 $#)
do
    cur_ip=${ip[$j-1]}
    # ssh ${USER}@${cur_ip} "nohup iostat -yt 1 > iostat.out &"
    ssh ${USER}@${cur_ip} "nohup dstat --output dstat.csv > dstat.out &"
done
# iostat -yt 1 > iostat.out &
dstat --output dstat.csv > dstat.out &

# start ycsb
bash $mode.sh $workload localhost:$port $shard_num $sleep_ms $rate > ycsb.out 2>&1
grep Throughput ycsb.out

# kill replicator, heads, and tails
bash kill.sh $*
sleep 5