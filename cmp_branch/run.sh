#!/bin/bash

cd ../build && make
cd -

DRAM=100
TC=1000000
SSD_FILE=ssd
rm $SSD_FILE
touch $SSD_FILE


branch=$(git rev-parse --abbrev-ref HEAD)
sudo bash ../testbench/shared.sh

numactl -N 0 ../build/frontend/test \
    --ssd_path=$SSD_FILE \
    --ycsb_tuple_count=$TC \
    --print_tx_console=false \
    --dram_gib=$DRAM \
    --pp_threads=1 \
    --ycsb_scan=true \
    --worker_threads=1 > log_${branch}