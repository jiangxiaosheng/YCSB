#!/bin/bash

set -x

if [ $# != 8 ]; then
    echo "Usage: bash eval.sh phase workload rate suffix client_num mode shard_num replication_factor"
    echo "Got: $@"
    exit
fi

phase=$1
workload=$2
rate=$3
suffix=$4
client_num=$5
mode=$6
shard_num=$7
rf=$8
cpu_num=$client_num

echo -e "\033[0;32m ${phase} ${workload} ${mode} \033[0m"

replicator_port=50040
shard_port=50050
rubble_dir="/mnt/data/my_rocksdb/rubble"

ssh_with_retry()
{
    local ip=$1
    local cmd=$2
    until ssh ${USER}@${ip} "$cmd exit 0"
    do
        sleep 1
    done
}

launch_node()
{
    local ip=$1
    local port=$2
    local addr=$3
    local sid=$4
    local rid=$5

    local cgroup_opts="cgexec -g cpuset:rubble-cpu -g memory:rubble-mem"
    # gprof_opts="env HEAPPROFILE=${rubble_dir}/shard-${sid}.hprof LD_PRELOAD=/usr/local/lib/libtcmalloc.so"
    local log="shard-${sid}.out"
    
    ssh_with_retry ${ip} "cd ${rubble_dir}; \
        ulimit -n 999999; ulimit -c unlimited; \
        nohup sudo ${cgroup_opts} ${gprof_opts} ./db_node ${port} ${addr} ${sid} ${rid} ${rf} > ${log} 2>&1 &"
}

set_cgroups()
{
    for (( i=0; i<${rf}; i++ ))
    do
        local ip="10.10.1."$(($i + 2))
        ssh_with_retry ${ip} "cd ${rubble_dir}; sudo bash create_cgroups.sh ${cpu_num} > /dev/null 2>&1;"
    done
}

launch_all_nodes()
{
    # 1. prepare the environment
    for (( i=0; i<${rf}; i++ ))
    do
        local ip="10.10.1."$(($i + 2))
        ssh_with_retry ${ip} "cd ${rubble_dir}; sudo killall db_node dstat iostat perf > /dev/null 2>&1;"
        ssh_with_retry ${ip} "cd ${rubble_dir}; sudo bash clean.sh ${shard_num} > /dev/null 2>&1;"
        ssh_with_retry ${ip} "cd ${rubble_dir}; sudo bash change-mode.sh ${mode} 1 4;"
    done
    set_cgroups

    # 2. launch DB instances on the server
    for (( i=0; i<${shard_num}; i++ ))
    do
        local port=$(($shard_port + $i))

        for (( j=0; j<${rf}; j++ ))
        do
            local ip="10.10.1."$((($i + $j) % $rf + 2))
            if [ $j -eq $(($rf - 1)) ]
            then
                local next_ip="10.10.1.1:"$replicator_port
            else
                local next_ip="10.10.1."$((($i + $j + 1) % $rf + 2))":"$port
            fi
            launch_node $ip $port ${next_ip} $i $j
        done
    done
}

record_stats()
{
    for (( i=0; i<${rf}; i++ ))
    do
        local ip="10.10.1."$(($i + 2))
        ssh_with_retry ${ip} "cd ${rubble_dir}; nohup sudo bash dstat.sh ${cpu_num} > /dev/null 2>&1 &"
        ssh_with_retry ${ip} "cd ${rubble_dir}; nohup sudo bash iostat.sh > /dev/null 2>&1 &"
        ssh_with_retry ${ip} "cd ${rubble_dir}; ps aux | grep -E './db_node' > pids.out;"
        ssh_with_retry ${ip} "cd ${rubble_dir}; nohup top -H -b -d 1 -w 512 > top.out 2>&1 &"
        # ssh_with_retry ${ip} "cd ${rubble_dir}; nohup bash perf.sh ${suffix} 0,2,4,6 250 > /dev/null 2>&1 &"
    done
}

assemble_args()
{
    local arg="-p shard=${shard_num} -p client=${client_num} "
    for (( i=0; i<${shard_num}; i++ ))
    do
        local port=$(($shard_port + $i))
        local head_ip="10.10.1."$(($i % $rf + 2))
        local tail_ip="10.10.1."$((($i + $rf - 1) % rf + 2))
        arg=$arg"-p head${i}=${head_ip}:${port} -p tail${i}=${tail_ip}:${port} "
    done

    echo $arg
}

relax_cpu()
{
    for (( i=0; i<${rf}; i++ ))
    do
        local ip="10.10.1."$(($i + 2))
        local cpu=`ssh_with_retry ${ip} "lscpu;" | grep "NUMA node0 CPU(s)" | awk '{print $(NF)}'`
        ssh_with_retry ${ip} "sudo cgset -r cpuset.cpus=${cpu} rubble-cpu;"
    done
}

wait_pending_jobs()
{
    pid=()

    for (( i=0; i<${shard_num}; i++ ))
    do
        db_dir="/mnt/data/db/shard-${i}/db/LOG"

        for (( j=0; j<${rf}; j++ ))
        do
            local ip="10.10.1."$((($i + $j) % $rf + 2))
            ssh_with_retry ${ip} "cd ${rubble_dir}; bash wait-pending-jobs.sh ${db_dir};" &
            pid[$(($i * $rf + $j))]=$!
        done
    done

    for i in ${pid[@]}
    do
        wait $i
    done
}

massacre()
{
    sudo killall java
    for (( i=0; i<${rf}; i++ ))
    do
        local ip="10.10.1."$(($i + 2))
        ssh_with_retry ${ip} "sudo killall db_node dstat iostat perf top;"
    done
}

process_results()
{
	start_cut=$1
	end_cut=$2
    grep Throughput ycsb.out
    cp ycsb.out ycsb-${suffix}.out
    cp replicator.out replicator-${suffix}.out
    python3 plot-thru.py ycsb-${suffix}.out 10 ${start_cut} ${end_cut}

    for (( i=0; i<${rf}; i++ ))
    do
        local ip="10.10.1."$(($i + 2))
        ssh_with_retry ${ip} "cd ${rubble_dir}; bash save-result.sh ${shard_num} ${suffix};"
        ssh_with_retry ${ip} "cd ${rubble_dir}; python3 plot-dstat.py dstat-${suffix}.csv 10 ${cpu_num};"
        ssh_with_retry ${ip} "cd ${rubble_dir}; python3 plot-iostat.py iostat-${suffix}.out 10;"
    done
}

update_workload_file() {
    local shard_num=$1
    for wl in "a" "b" "c" "d" "e" "f" "g"
    do
        local cnt=$(( $shard_num * 10000000 ))
        sed -i "s/recordcount=[0-9]\+/recordcount=${cnt}/g" workloads/workload${wl}
        sed -i "s/operationcount=[0-9]\+/operationcount=${cnt}/g" workloads/workload${wl}
    done
}

# 1. start db instances
launch_all_nodes

# 2. start the replicator
sudo killall java
replicator_args=$(assemble_args)
./bin/ycsb.sh replicator rocksdb -s -P workloads/workload${workload} \
    -p port=$replicator_port $replicator_args -p replica=$rf > replicator.out 2>&1 &

# 3. load the database
# update_workload_file $shard_num
sleep_ms=1000
echo "" > ycsb.out
if [ $phase != load ]; then
    #relax_cpu

    bash load.sh $workload localhost:$replicator_port $shard_num $sleep_ms 90000 $client_num > ycsb.out 2>&1

    wait_pending_jobs

    set_cgroups
fi

# 4. record performance metrics
record_stats

# 5. run YCSB
bash $phase.sh $workload localhost:$replicator_port $shard_num $sleep_ms $rate $client_num >> ycsb.out 2>&1

# 6. kill all processes
massacre

# 7. save results and plot figures
start_cut=1000
end_cut=300
process_results ${start_cut} ${end_cut}
