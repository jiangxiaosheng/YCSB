rm -rf rocksdb.dir
javac -classpath ../core/target/classes/:../rocksdb/target/dependency/*:. Node.java
java -classpath ../core/target/classes/:../rocksdb/target/dependency/*:. Node true 128.110.153.127 9876 2345
