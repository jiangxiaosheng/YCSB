#!/bin/bash
char=$2
prefix=$1
bash load.sh -w=$char > $prefix$char.txt
bash run.sh -w=$char >> $prefix$char.txt
