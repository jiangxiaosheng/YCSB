#!/bin/bash
cd /mnt/sdb
git clone https://github.com/camelboat/my_rocksdb.git
git clone https://github.com/camelboat/EECS_6897_Distributed_Storage_System_Project_Scripts.git
cd my_rocksdb && git checkout rubble
cd ..
wget https://raw.githubusercontent.com/camelboat/EECS_6897_Distributed_Storage_System_Project_Scripts/rubble/setup_scripts/gRPC/grpc_setup.sh
wget https://raw.githubusercontent.com/camelboat/EECS_6897_Distributed_Storage_System_Project_Scripts/rubble/setup_scripts/gRPC/cmake_install.sh
bash grpc_setup.sh
apt get install libgflags-dev
export PATH=/root:/root/bin:$PATH
cd /mnt/sdb/my_rocksdb/nlohmann_json/json
git clone https://github.com/nlohmann/json.git
mv json/* .
echo "end of script"
