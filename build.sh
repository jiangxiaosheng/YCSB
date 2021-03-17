# build for native YCSB
mvn -pl rocksdb -am clean package -Dmaven.test.skip=true -Dcheckstyle.skip=true
