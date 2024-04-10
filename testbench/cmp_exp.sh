#!/bin/bash
conf=in_mem.cfg
exp1=read
exp2=readseg
# Check if there is a command line argument
if [[ $# -eq 3 ]]; then
    conf=$1
    exp1=$2
    exp2=$3
else
    echo "conf"
    echo -n ">>"
    read conf
    echo "exp 1"
    echo -n ">>"
    read exp1
    echo "exp 2"
    echo -n ">>"
    read exp2
fi
 
bash bench_leanstore.sh $conf $exp1  | tee ../logs/${conf%.cfg}_${exp1}.log
bash bench_leanstore.sh $conf $exp2  | tee ../logs/${conf%.cfg}_${exp2}.log
