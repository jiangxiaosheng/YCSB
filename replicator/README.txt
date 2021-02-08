Compile Replicator with reference to ycsb class files:
- javac -classpath ../core/target/classes/:../rocksdb/target/dependency/*:. Replicator.java

Then compiler Server.java/Driver function with:
- javac -classpath ../core/target/classes/:. Server.java
