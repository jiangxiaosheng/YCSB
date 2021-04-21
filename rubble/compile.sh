#!/bin/bash

YCSB_HOME=/mnt/sdb/YCSB

REPLICATOR_DIR=/rubble/src/main/java/site/ycsb/db/rubble

javac -classpath $YCSB_HOME/rubble/target/dependency/* ${REPLICATOR_DIR}/Replicator.java
java -ea -classpath $YCSB_HOME/rubble/target/dependency/* ${REPLICATOR_DIR}/Replicator
