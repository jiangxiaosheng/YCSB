./bin/ycsb load rubble -s -P workloads/workloada \
-threads 1 \
-p targetaddr=0.0.0.0:50050 \
-p batchsize=1000 \
-p hdrhistogram.percentiles=5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95,99,99.9 