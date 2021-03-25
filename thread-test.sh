#!/bin/bash
i=1
while [ $i -lt 256 ] 
do
  echo $i
  bash run.sh $i >> $1
  i=$((i*2))
done
  
