#!/bin/bash
protoc --proto_path=. --java_out=target/ *.proto
# protoc --proto_path=./include/google/protobuf/ --java_out=. *.proto
# javac -classpath ../:../../core/target/classes/:../../rocksdb/target/dependency/*:. HelloWorldClient.java
# java -classpath ../:../../core/target/classes/:../../rocksdb/target/dependency/*:. TestRep 128.110.155.5 1234
