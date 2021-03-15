#!/bin/bash
for char in a b c d e f 
do 
	echo out-$char
	grep "Throughput" out$char.txt
done
