# How to compile

    #!/bin/bash
    sudo apt update &&\
    sudo apt install default-jdk maven -y &&\
    git clone https://github.com/cc4351/YCSB.git &&\
    cd YCSB &&\
    git checkout perf &&\
    ./build.sh
    echo "YCSB build completed"
    echo "./load.sh #number_of_threads"
    echo "./run.sh #number_of_threads --e.g. ./run.sh 16"

# What to modify before load and run

    ## IP and port number for replicator
    open core/src/main/java/site/ycsb/Client.java
    goto line 344, replace the default string with your ip and port number (for replicator)

    ## number of operations to run
    load.sh and run.sh runs workloads/workload1 by default (100% READ), change the record count and opcount if necessary

# How to load and run
    ./load.sh #_of_threads for load (with specification in workloads/workload1)
    ./run.sh #_of_threads for run (with specification in workloads/workload1)

# utilities

    ## to perform a thread test on the replicator-node combo
    (variable number of YCSB clients from 1 to 128 clients, connected to Replicator and Node)

    cd /mnt/sdb/YCSB
    bash thread-test.sh name_of_log_file 

# If you are to use the java replicator

    goto YCSB/replicator
    bash build-rpc.sh rubble_kv_store.proto
    ** open Replicator.java and replace the listening port number (default to 50051) and the node address for GET
    ** note that the PUT operation is currently not supported by the replicator
    mv Replicator.java target/rubblejava/
    bash compile.sh rubblejava/Replicator

    --> Replicator should be running now

    
