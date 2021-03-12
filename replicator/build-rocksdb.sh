#!/bin/bash
git clone https://github.com/camelboat/my_rocksdb.git
cd my_rocksdb && git checkout rubble
cd ..
wget https://raw.githubusercontent.com/camelboat/EECS_6897_Distributed_Storage_System_Project_Scripts/rubble/setup_scripts/gRPC/grpc_setup.sh
bash grpc_setup.sh
export PATH=/root:$PATH
apt get install libgflags-dev
cd /mnt/sdb/my_rocksdb/nlohmann_json/json
git clone https://github.com/nlohmann/json.git
mv json/* .
echo "end of script"
