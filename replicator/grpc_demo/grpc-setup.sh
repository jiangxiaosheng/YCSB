#!/bin/bash
wget https://repo1.maven.org/maven2/io/grpc/protoc-gen-grpc-java/1.36.0/protoc-gen-grpc-java-1.36.0-linux-x86_64.exe
mv protoc-gen-grpc-java-1.36.0-linux-x86_64.exe protoc-gen-grpc-java
chmod +x protoc-gen-grpc-java
wget https://raw.githubusercontent.com/camelboat/EECS_6897_Distributed_Storage_System_Project_Scripts/rubble/rubble/protos/rubble_kv_store.proto
