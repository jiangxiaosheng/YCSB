## build rubble:
under the YCSB directory, run:
```mvn -pl rubble -am clean package```

## run the replicator:
```mvn exec:java -Dexec.mainClass=site.ycsb.db.rubble.Replicator```
config file config.yml is under src/main/resources

## run the loading phase:
```sudo bash load.sh```