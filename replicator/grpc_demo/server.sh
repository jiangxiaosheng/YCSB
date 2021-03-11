#!/bin/bash
cd target
javac -classpath ../../:../../../core/target/classes/:../../../rocksdb/target/dependency/*:. ../Downstream.java
java -classpath ../../:../../../core/target/classes/:../../../rocksdb/target/dependency/*:. ../Downstream.java
