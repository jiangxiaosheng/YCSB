#!/bin/bash
i=1
while [ $i -lt 512 ] 
do
  echo $i "threads -------"
  # grep -m 1 "long tail latency" thread-test-$i.txt
  # echo "long tail count: " 
  # grep -c "long tail latency" thread-test-$i.txt
  # if [ $i -lt 3 ]
  # then
    # grep -m $i "latency" thread-test-$i.txt
  # else
    # grep -m 3 "latency" thread-test-$i.txt
 #  fi
  # echo "throughput"
  grep "Throughput" thread-test-$i.txt
  i=$((i*2))
done
  

