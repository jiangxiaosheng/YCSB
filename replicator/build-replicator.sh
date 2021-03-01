rm -rf rocksdb.dir
javac -classpath ../core/target/classes/:../rocksdb/target/dependency/*:. Replicator.java
java -classpath ../core/target/classes/:../rocksdb/target/dependency/*:. Replicator
