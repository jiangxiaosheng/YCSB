#!/bin/bash
if [[ "$#" -eq "0" ]]
    then echo "run with cmd: ./build-rpc.sh filename-of-proto-without-.proto"
    exit
fi
mkdir -p target/
./protoc --plugin=protoc-gen-grpc-java --grpc-java_out=target --proto_path=. $1.proto
./protoc --proto_path=. --java_out=target/ $1.proto
cd target
javac -classpath ../../:../../../core/target/classes/:../../../rocksdb/target/dependency/*:. ../*.java
# javac -classpath ../:../../core/target/classes/:../../rocksdb/target/dependency/*:./target/*:. HelloWorldClient.java
# java -classpath ../:../../core/target/classes/:../../rocksdb/target/dependency/*:. TestRep 128.110.155.5 1234
