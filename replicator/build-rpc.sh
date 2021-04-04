#!/bin/bash
path=""
filename=""
if [[ "$#" -eq "0" ]]
then
    echo "default to /mnt/sdb/core/src/main/proto/rubble_kv_store.proto"
    path="/mnt/sdb/YCSB/core/src/main/proto/"
    filename="rubble_kv_store.proto"
else
    echo "please make sure proto file is in current directory"
    path="."
    filename=$1
fi
mkdir -p target/
./protoc --plugin=protoc-gen-grpc-java --grpc-java_out=target --proto_path=$path $filename
./protoc --proto_path=$path --java_out=target/ $filename
cd target
# javac -classpath ../../:../../../core/target/classes/:../../../rocksdb/target/dependency/*:. ../*.java
# javac -classpath ../:../../core/target/classes/:../../rocksdb/target/dependency/*:./target/*:. HelloWorldClient.java
# java -classpath ../:../../core/target/classes/:../../rocksdb/target/dependency/*:. TestRep 128.110.155.5 1234
