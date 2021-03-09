#!/bin/bash
cd target
javac -classpath ../../:../../../core/target/classes/:../../../rocksdb/target/dependency/*:. ../Upstream.java
java -classpath ../../:../../../core/target/classes/:../../../rocksdb/target/dependency/*:. ../Upstream.java
