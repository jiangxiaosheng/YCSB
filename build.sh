 mvn -pl rocksdb -am clean package -Dmaven.test.skip=true
 rm rocksdb/target/dependency/rocksdbjni-6.14.6.jar
 cp /root/rocksdb/java/target/rocksdbjni-6.14.0.jar rocksdb/target/dependency/
 rm -rf /tmp/rocksdb/