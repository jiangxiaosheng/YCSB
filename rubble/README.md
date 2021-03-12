How to run helloworldClient.java:

under the top level directory, first compile and install dependecies to local repositories:

`mvn clean compile`
`mvn install`

then run : 
`mvn exec:java -pl site.ycsb:replicator -Dexec.mainClass=site.ycsb.replicator.helloworld.HelloWorldClient`

build replicator:
`mvn -pl site.ycsb:repliactor -am clean package`