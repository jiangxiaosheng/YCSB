#!/bin/bash

set -ex

cp Replicator.java ./target/rubblejava/
cp test.yml ./target/config/

cd target/
YCSB_HOME=/mnt/sdb/YCSB

javac -classpath $YCSB_HOME/core/target/classes/:$YCSB_HOME/rocksdb/target/dependency/*:$YCSB_HOME/replicator/lib/*:.: $1.java
java -ea -classpath $YCSB_HOME/core/target/classes/:$YCSB_HOME/rocksdb/target/dependency/*:$YCSB_HOME/replicator/lib/*:.:  $1

REPLICATOR_PID=$!
echo ${REPLICATOR_PID}

mkdir -p /tmp/rubble_proc/
echo "REPLICATOR ${REPLICATOR_PID}" >> /tmp/rubble_proc/proc_table
