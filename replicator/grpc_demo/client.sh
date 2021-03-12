#!/bin/bash
cd target/
YCSB_HOME=/users/$USER/YCSB
javac -classpath $YCSB_HOME/core/target/classes/:$YCSB_HOME/rocksdb/target/dependency/*:.: $1
java -classpath $YCSB_HOME/core/target/classes/:$YCSB_HOME/rocksdb/target/dependency/*:.: $1
