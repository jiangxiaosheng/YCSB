rm -rf rocksdb.dir
javac -classpath ../core/target/classes/:../rocksdb/target/dependency/*:. Node.java
java -classpath ../core/target/classes/:../rocksdb/target/dependency/*:. Node false 128.110.153.219 3456 2345
