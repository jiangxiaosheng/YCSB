#!/bin/bash
wget https://repo1.maven.org/maven2/io/grpc/protoc-gen-grpc-java/1.36.0/protoc-gen-grpc-java-1.36.0-linux-x86_64.exe
mv protoc-gen-grpc-java/1.36.0/protoc-gen-grpc-java-1.36.0-linux-x86_64.exe protoc-gen-grpc-java/1.36.0/protoc-gen-grpc-java
chmod +x protoc-gen-grpc-java
protoc --plugin=protoc-gen-grpc-java --grpc-java_out=. --proto_path=. *.proto
# protoc --proto_path=./include/google/protobuf/ --java_out=. *.proto
# javac -classpath ../:../../core/target/classes/:../../rocksdb/target/dependency/*:. HelloWorldClient.java
# java -classpath ../:../../core/target/classes/:../../rocksdb/target/dependency/*:. TestRep 128.110.155.5 1234
