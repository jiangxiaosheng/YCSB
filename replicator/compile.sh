#!/bin/bash

set -ex
mkdir -p ./target/rubblejava
cp Replicator.java ./target/rubblejava/
mkdir -p ./target/config/
cp test.yml ./target/config/

cd target/
YCSB_HOME=../../
TARGET='rubblejava/Replicator'
DEPENDENCIES=$YCSB_HOME/core/target/classes/:$YCSB_HOME/rocksdb/target/dependency/*:$YCSB_HOME/replicator/lib/*:.:

javac -classpath ${DEPENDENCIES} ${TARGET}.java
java -ea -classpath ${DEPENDENCIES} ${TARGET}

REPLICATOR_PID=$!
echo ${REPLICATOR_PID}

mkdir -p /tmp/rubble_proc/
echo "REPLICATOR ${REPLICATOR_PID}" >> /tmp/rubble_proc/proc_table
