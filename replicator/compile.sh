#!/bin/bash
cd target/
YCSB_HOME=/mnt/sdb/YCSB
javac -classpath $YCSB_HOME/core/target/classes/:$YCSB_HOME/rocksdb/target/dependency/*:.: $1.java
java -ea -classpath $YCSB_HOME/core/target/classes/:$YCSB_HOME/rocksdb/target/dependency/*:.:  $1
