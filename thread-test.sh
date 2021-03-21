#!/bin/bash
i=1
while [ $i -lt 128 ] 
do
  echo $i
  bash run.sh $i >> good-1mil.txt
  i=$((i*2))
done
  
