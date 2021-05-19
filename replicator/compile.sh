#!/bin/bash

set -ex

cp Replicator.java ./target/rubblejava/
mkdir -p ./target/config/
cp test.yml ./target/config/

cd target/
YCSB_HOME=../../
DEPENDENCIES=$YCSB_HOME/core/target/classes/rubblejava/:$YCSB_HOME/rocksdb/target/dependency/*:$YCSB_HOME/replicator/lib/*:.:
javac -classpath ${DEPENDENCIES} $1.java
java -ea -classpath ${DEPENDENCIES} $1

REPLICATOR_PID=$!
echo ${REPLICATOR_PID}

mkdir -p /tmp/rubble_proc/
echo "REPLICATOR ${REPLICATOR_PID}" >> /tmp/rubble_proc/proc_table
