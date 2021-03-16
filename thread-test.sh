#!/bin/bash
i=1
while [ $i -lt 512 ] 
do
  echo $i
  bash run.sh $i > thread-test-$i.txt
  i=$((i*2))
done
  
