rm -rf rocksdb.dir
javac -classpath ../core/target/classes/:../rocksdb/target/dependency/*:. Node.java
java -classpath ../core/target/classes/:../rocksdb/target/dependency/*:. Node
