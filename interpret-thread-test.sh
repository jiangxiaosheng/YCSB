#!/bin/bash
i=1
while [ $i -lt 512 ] 
do
  echo $i "threads -------"
  if [ $i -lt 3 ]
  then
    grep -m $i "latency" thread-test-$i.txt
  else
    grep -m 3 "latency" thread-test-$i.txt
  fi
  i=$((i*2))
done
  

