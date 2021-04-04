#!/bin/bash
# run this script in sudo mode
mkdir -p /mnt/sdb && cd /mnt/sdb
apt update
apt install default-jdk maven -y
git clone https://github.com/cc4351/YCSB.git
cd YCSB
git checkout singleOp
./build.sh