rm -rf rocksdb.dir
javac -classpath ../core/target/classes/:../rocksdb/target/dependency/*:. Replicator.java
javac -classpath ../core/target/classes/:../rocksdb/target/dependency/*:. Server.java
java -classpath ../core/target/classes/:../rocksdb/target/dependency/*:. Server
