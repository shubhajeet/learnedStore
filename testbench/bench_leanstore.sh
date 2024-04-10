#!/bin/bash

# This script is used to run my program.
help() {
  echo "Enter a variable"
  echo "==========================================================="
  echo "variable type:"
  echo "-load: load data to leanstore and store the trace"
  echo "-read: recover the data from leanstore by reading the trace"
  echo "-origin: run origin leanstore ycsb benchmark"
  echo "-clear: remove all the related files"
  echo "==========================================================="
}
exp=read
conf=in_mem.cfg
# Check if there is a command line argument
if [[ $# -eq 2 ]]; then
  conf=$1
  exp=$2
else
  help
  echo -n ">>"
  read exp
fi

# print current command and parameters
echo "bench_leanstore.sh $conf $exp"
echo "---------------------------"
date

# load configuration file
source $conf
source dram.sh

# use variables defined in the configuration file
echo "Experiment Config File: $EXP_CONFIG_FILE"
echo "Number of items: $NUM"
echo "DRAM size: $DRAM"
echo "SSD file name: $SSD_FILE"
echo "Metadata file name: $META_FILE"
echo "Trace file name: $TRACE_FILE"
echo "Collect stats: $COLLECT_STATS"
echo "Attach segment stored in file: $ATTACH_SEG_FILE"
echo "Spline points in file: $SPLINE_FILE"
echo "Mapping in file: $MAPPING_FILE"
echo "Sequential Workload: $STEP "
echo "Sequential Operations: $SEQ_OPS"

pmzr='perf stat -e cycles:u,instructions,cache-references,cache-misses,page-faults,L1-dcache-loads,L1-dcache-load-misses,LLC-loads,LLC-load-misses,LLC-stores,LLC-store-misses,branch-load-misses '
if [ $MODE = "Debug" ]; then
  echo "run mode: $MODE"
  PREFIX_CMD="gdb --args"
# PREFIX_CMD="gdb -ex run --args"
elif [ $MODE = "RelWithDebInfo" ]; then
  echo "run mode: $MODE"
  PREFIX_CMD="sudo gdb --args"
else
  # Actual benchmark command
  # This does not force data to be in same numa node
  # PREFIX_CMD=""
  # PREFIX_CMD="numactl -C 1 -m 1 sudo $pmzr"
  # PREFIX_CMD="numactl -C 1 -m 1 sudo"
  # This command will run the given command with the given args on cores 1,3 on a NUMA machine
  # PREFIX_CMD="numactl -C 1,3 sudo"

  # PREFIX_CMD="sudo gdb --args"
  # This forces data to be in same numa node
  # PREFIX_CMD="numactl -N 1 -m 1 sudo"
  PREFIX_CMD="sudo"
fi
# run command in gdb
BUILD_DIR=../build_$MODE/
bash compile.sh $BUILD_DIR
# dstat -c -m -d -D total,nvme0n1 -r -fs -T 1
sudo bash drop_cache.sh
# COLLECT_STATS=true
# collect_pstat_stats() {
#   dstat -c -m -d -D /dev/sda -r -fs -T --output ${STATS_LOC}_dstat.csv 1 &
#   PIDPSTATSTAT1=$!
# }

# collect_iostat_stats() {
#   iostat -yxmt 1 >${STATS_LOC}_iostat.log &
#   PIDPSTATSTAT2=$!
# }

# kill_collect_stats() {
#   #   sudo kill -9 $PIDPSTATSTAT1
#   sudo kill -9 $PIDPSTATSTAT2
#   # Kill all remaining background jobs
#   for job in $(jobs -p); do
#     sudo kill $job
#   done
#   bash ../experiments/get_iostat.sh ${STATS_LOC}_iostat.log ${STATS_LOC}_iostat.png
# }

# if [ $COLLECT_STATS = true ]; then
#   # start stats collection
#   # collect_pstat_stats
#   collect_iostat_stats
# fi
case $exp in
overhead)
  # Create the ssd file for leanstore
  # Start the benchmarks
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=genrandom,fastload,statistics,writetracetoread,readall,readallwithseg \
    --step=$STEP \
    --seq_operation=$SEQ_OPS \
    --worker_threads=$WORKERS \
    --pp_threads=$PP_THREADS \
    --ssd_path=./$SSD_FILE \
    --dram_gib=$DRAM \
    --csv_path=$PROF \
    --cool_pct=40 \
    --free_pct=1 \
    --contention_split=false \
    --xmerge=false \
    --print_tx_console=false \
    --recover=false \
    --persist=false \
    --tracefile=$TRACE_FILE \
    --attached_segments_file=$ATTACH_SEG_FILE \
    --segments_file=$SPLINE_FILE \
    --secondary_mapping_file=$MAPPING_FILE \
    --max_error=$MAX_ERROR
  # --read=$READ
  # --dram_gib=$DRAM \
  ;;
scratchzip)
  # Create the ssd file for leanstore
  # touch $SSD_FILE
  # Start the benchmarks
    # --benchmarks=genrandom,load,statistics,writetracetoread,fasttrain,readzip,readzipwithseg \
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --step=$STEP \
    --benchmarks=genrandom,loadsorted,statistics,writetracetoread,fasttrain,readzip,readzipwithseg \
    --seq_operation=$SEQ_OPS \
    --worker_threads=$WORKERS \
    --pp_threads=$PP_THREADS \
    --ssd_path=./$SSD_FILE \
    --dram_gib=$DRAM \
    --csv_path=$PROF \
    --cool_pct=40 \
    --free_pct=1 \
    --contention_split=false \
    --xmerge=false \
    --print_tx_console=false \
    --recover=false \
    --persist=false \
    --tracefile=$TRACE_FILE \
    --attached_segments_file=$ATTACH_SEG_FILE \
    --segments_file=$SPLINE_FILE \
    --secondary_mapping_file=$MAPPING_FILE \
    --max_error=$MAX_ERROR \
    --readtime=300
    # --readtime=$READTIME
  # --read=$READ
  # --dram_gib=$DRAM \
  # --benchmarks=load,statistics,readall,poolstats \
  ;;
scratch)
  # Create the ssd file for leanstore
  # touch $SSD_FILE
  # Start the benchmarks
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --step=$STEP \
    --benchmarks=writetraceload,load,statistics,writetracetoread,readall,poolstats \
    --seq_operation=$SEQ_OPS \
    --worker_threads=$WORKERS \
    --pp_threads=$PP_THREADS \
    --ssd_path=./$SSD_FILE \
    --dram_gib=$DRAM \
    --csv_path=$PROF \
    --cool_pct=40 \
    --free_pct=1 \
    --contention_split=false \
    --xmerge=false \
    --print_tx_console=false \
    --recover=false \
    --persist=false \
    --tracefile=$TRACE_FILE \
    --attached_segments_file=$ATTACH_SEG_FILE \
    --segments_file=$SPLINE_FILE \
    --secondary_mapping_file=$MAPPING_FILE \
    --max_error=$MAX_ERROR
  # --read=$READ
  # --dram_gib=$DRAM \
  # --benchmarks=load,statistics,readall,poolstats \
  ;;
load)
  # Create the ssd file for leanstore
  # touch $SSD_FILE
  # Start the benchmarks
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=genrandom,load \
    --step=$STEP \
    --seq_operation=$SEQ_OPS \
    --worker_threads=$WORKERS \
    --pp_threads=$PP_THREADS \
    --ssd_path=./$SSD_FILE \
    --dram_gib=$DRAM \
    --csv_path=$PROF \
    --cool_pct=40 \
    --free_pct=1 \
    --contention_split=false \
    --xmerge=false \
    --print_tx_console=false \
    --recover=false \
    --persist=false \
    --tracefile=$TRACE_FILE \
    --attached_segments_file=$ATTACH_SEG_FILE \
    --segments_file=$SPLINE_FILE \
    --secondary_mapping_file=$MAPPING_FILE \
    --max_error=$MAX_ERROR
  ;;
scanall)
  # Create the ssd file for leanstore
  # touch $SSD_FILE
  # Start the benchmarks
    # --benchmarks=genrandom,load,statistics,fasttrain,writetracetoread,readzip,readzipwithseg,readzipflat,readzipflatwithseg \
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
  --benchmarks=genrandom,load,statistics,fasttrain,writetracetoread,scanallasc,scanallascseg \
    --step=$STEP \
    --seq_operation=$SEQ_OPS \
    --worker_threads=$WORKERS \
    --pp_threads=$PP_THREADS \
    --ssd_path=./$SSD_FILE \
    --dram_gib=$DRAM \
    --csv_path=$PROF \
    --cool_pct=40 \
    --free_pct=1 \
    --contention_split=false \
    --xmerge=false \
    --print_tx_console=false \
    --recover=false \
    --persist=false \
    --tracefile=$TRACE_FILE \
    --attached_segments_file=$ATTACH_SEG_FILE \
    --segments_file=$SPLINE_FILE \
    --secondary_mapping_file=$MAPPING_FILE \
    --max_error=$MAX_ERROR \
    --readtime=$READTIME
  ;;

scan)
  # Create the ssd file for leanstore
  # touch $SSD_FILE
  # Start the benchmarks
    # --benchmarks=genrandom,load,statistics,fasttrain,writetracetoread,readzip,readzipwithseg,readzipflat,readzipflatwithseg \
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
  --benchmarks=genrandom,load,statistics,fasttrain,writetracetoread,scanasc,scanascseg \
    --step=$STEP \
    --seq_operation=$SEQ_OPS \
    --worker_threads=$WORKERS \
    --pp_threads=$PP_THREADS \
    --ssd_path=./$SSD_FILE \
    --dram_gib=$DRAM \
    --csv_path=$PROF \
    --cool_pct=40 \
    --free_pct=1 \
    --contention_split=false \
    --xmerge=false \
    --print_tx_console=false \
    --recover=false \
    --persist=false \
    --tracefile=$TRACE_FILE \
    --attached_segments_file=$ATTACH_SEG_FILE \
    --segments_file=$SPLINE_FILE \
    --secondary_mapping_file=$MAPPING_FILE \
    --max_error=$MAX_ERROR \
    --readtime=$READTIME
  ;;
inmem)
  # Create the ssd file for leanstore
  # touch $SSD_FILE
  # Start the benchmarks
    # --benchmarks=genrandom,load,statistics,fasttrain,writetracetoread,readzip,readzipwithseg,readzipflat,readzipflatwithseg \
  # --benchmarks=genrandom,load,writetracetoread,statistics,readall,readallwithseg \
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --step=$STEP \
  --benchmarks=genrandom,loadsorted,fasttrain,writetracetoread,statistics,readall,readallwithrmi,readallwithrs,readallwithseg \
    --seq_operation=$SEQ_OPS \
    --worker_threads=$WORKERS \
    --pp_threads=$PP_THREADS \
    --ssd_path=./$SSD_FILE \
    --dram_gib=$DRAM \
    --csv_path=$PROF \
    --nmodels=$NMODELS \
    --cool_pct=40 \
    --free_pct=1 \
    --contention_split=false \
    --xmerge=false \
    --print_tx_console=false \
    --recover=false \
    --persist=false \
    --tracefile=$TRACE_FILE \
    --attached_segments_file=$ATTACH_SEG_FILE \
    --segments_file=$SPLINE_FILE \
    --secondary_mapping_file=$MAPPING_FILE \
    --max_error=$MAX_ERROR \
    --readtime=$READTIME
  ;;
insert)
  # Create the ssd file for leanstore
  # touch $SSD_FILE
  # Start the benchmarks
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=genrandom,load,statistics \
    --step=$STEP \
    --seq_write_operation=$SEQ_WRITE_OPS \
    --seq_operation=$SEQ_OPS \
    --worker_threads=$WORKERS \
    --pp_threads=$PP_THREADS \
    --ssd_path=./$SSD_FILE \
    --dram_gib=$DRAM \
    --csv_path=$PROF \
    --cool_pct=40 \
    --free_pct=1 \
    --contention_split=false \
    --xmerge=false \
    --print_tx_console=false \
    --recover=false \
    --persist=false \
    --tracefile=$TRACE_FILE \
    --attached_segments_file=$ATTACH_SEG_FILE \
    --segments_file=$SPLINE_FILE \
    --secondary_mapping_file=$MAPPING_FILE \
    --max_error=$MAX_ERROR \
    --readtime=$READTIME
  ;;
insertfast)
  # Create the ssd file for leanstore
  # touch $SSD_FILE
  # Start the benchmarks
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=genrandom,fastload,statistics \
    --step=$STEP \
    --seq_write_operation=$SEQ_WRITE_OPS \
    --seq_operation=$SEQ_OPS \
    --worker_threads=$WORKERS \
    --pp_threads=$PP_THREADS \
    --ssd_path=./$SSD_FILE \
    --dram_gib=$DRAM \
    --csv_path=$PROF \
    --cool_pct=40 \
    --free_pct=1 \
    --contention_split=false \
    --xmerge=false \
    --print_tx_console=false \
    --recover=false \
    --persist=false \
    --tracefile=$TRACE_FILE \
    --attached_segments_file=$ATTACH_SEG_FILE \
    --segments_file=$SPLINE_FILE \
    --secondary_mapping_file=$MAPPING_FILE \
    --max_error=$MAX_ERROR \
    --readtime=$READTIME
  ;;

twoinsert)
  # Create the ssd file for leanstore
  # touch $SSD_FILE
  # Start the benchmarks
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=genrandom,load,statistics,fasttrain,writetracetoread,genrandom,load \
    --step=$STEP \
    --seq_write_operation=$SEQ_WRITE_OPS \
    --seq_operation=$SEQ_OPS \
    --worker_threads=$WORKERS \
    --pp_threads=$PP_THREADS \
    --ssd_path=./$SSD_FILE \
    --dram_gib=$DRAM \
    --csv_path=$PROF \
    --cool_pct=40 \
    --free_pct=1 \
    --contention_split=false \
    --xmerge=false \
    --print_tx_console=false \
    --recover=false \
    --persist=false \
    --tracefile=$TRACE_FILE \
    --attached_segments_file=$ATTACH_SEG_FILE \
    --segments_file=$SPLINE_FILE \
    --secondary_mapping_file=$MAPPING_FILE \
    --max_error=$MAX_ERROR \
    --readtime=$READTIME
  ;;

twoinsertfast)
  # Create the ssd file for leanstore
  # touch $SSD_FILE
  # --benchmarks=genrandom,fastload,statistics,fasttrain,writetracetoread,readall,readallwithseg,genrandom,fastload,statistics,readall,readallwithseg,statistics \
  # Start the benchmarks
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --step=$STEP \
    --benchmarks=genrandom,fastload,statistics,fasttrain,writetracetoread,genrandom,fastload \
    --seq_write_operation=$SEQ_WRITE_OPS \
    --seq_operation=$SEQ_OPS \
    --worker_threads=$WORKERS \
    --pp_threads=$PP_THREADS \
    --ssd_path=./$SSD_FILE \
    --dram_gib=$DRAM \
    --csv_path=$PROF \
    --cool_pct=40 \
    --free_pct=1 \
    --contention_split=false \
    --xmerge=false \
    --print_tx_console=false \
    --recover=false \
    --persist=false \
    --tracefile=$TRACE_FILE \
    --attached_segments_file=$ATTACH_SEG_FILE \
    --segments_file=$SPLINE_FILE \
    --secondary_mapping_file=$MAPPING_FILE \
    --max_error=$MAX_ERROR \
    --readtime=$READTIME
  ;;
ycsba)
  # Create the ssd file for leanstore
  # touch $SSD_FILE
  # Start the benchmarks
    # --benchmarks=readtraceload,writetraceload,ycsba \
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --step=$STEP \
    --benchmarks=genrandom,load,writetracetoread,genrandom,ycsba \
    --seq_operation=$SEQ_OPS \
    --worker_threads=$WORKERS \
    --pp_threads=$PP_THREADS \
    --ssd_path=./$SSD_FILE \
    --dram_gib=$DRAM \
    --csv_path=$PROF \
    --cool_pct=40 \
    --free_pct=1 \
    --contention_split=false \
    --xmerge=false \
    --print_tx_console=false \
    --recover=false \
    --persist=false \
    --tracefile=$TRACE_FILE \
    --attached_segments_file=$ATTACH_SEG_FILE \
    --segments_file=$SPLINE_FILE \
    --secondary_mapping_file=$MAPPING_FILE \
    --max_error=$MAX_ERROR
  ;;
ycsbaseg)
  # Create the ssd file for leanstore
  # touch $SSD_FILE
  # Start the benchmarks
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=genrandom,fastload,writetracetoread,fasttrain,genrandom,ycsbaseg \
    --step=$STEP \
    --seq_operation=$SEQ_OPS \
    --worker_threads=$WORKERS \
    --pp_threads=$PP_THREADS \
    --ssd_path=./$SSD_FILE \
    --dram_gib=$DRAM \
    --csv_path=$PROF \
    --cool_pct=40 \
    --free_pct=1 \
    --contention_split=false \
    --xmerge=false \
    --print_tx_console=false \
    --recover=false \
    --persist=false \
    --tracefile=$TRACE_FILE \
    --attached_segments_file=$ATTACH_SEG_FILE \
    --segments_file=$SPLINE_FILE \
    --secondary_mapping_file=$MAPPING_FILE \
    --max_error=$MAX_ERROR
  ;;
ycsbb)
  # Create the ssd file for leanstore
  # touch $SSD_FILE
  # Start the benchmarks
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=genrandom,load,writetracetoread,fasttrain,genrandom,ycsbb \
    --step=$STEP \
    --seq_operation=$SEQ_OPS \
    --worker_threads=$WORKERS \
    --pp_threads=$PP_THREADS \
    --ssd_path=./$SSD_FILE \
    --dram_gib=$DRAM \
    --csv_path=$PROF \
    --cool_pct=40 \
    --free_pct=1 \
    --contention_split=false \
    --xmerge=false \
    --print_tx_console=false \
    --recover=false \
    --persist=false \
    --tracefile=$TRACE_FILE \
    --attached_segments_file=$ATTACH_SEG_FILE \
    --segments_file=$SPLINE_FILE \
    --secondary_mapping_file=$MAPPING_FILE \
    --max_error=$MAX_ERROR
  ;;
ycsbbseg)
  # Create the ssd file for leanstore
  # touch $SSD_FILE
  # Start the benchmarks
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --step=$STEP \
    --benchmarks=genrandom,fastload,writetracetoread,fasttrain,genrandom,ycsbbseg \
    --seq_operation=$SEQ_OPS \
    --worker_threads=$WORKERS \
    --pp_threads=$PP_THREADS \
    --ssd_path=./$SSD_FILE \
    --dram_gib=$DRAM \
    --csv_path=$PROF \
    --cool_pct=40 \
    --free_pct=1 \
    --contention_split=false \
    --xmerge=false \
    --print_tx_console=false \
    --recover=false \
    --persist=false \
    --tracefile=$TRACE_FILE \
    --attached_segments_file=$ATTACH_SEG_FILE \
    --segments_file=$SPLINE_FILE \
    --secondary_mapping_file=$MAPPING_FILE \
    --max_error=$MAX_ERROR
  ;;
create)
  # Create the ssd file for leanstore
  rm $TRACE_FILE
  rm $META_FILE
  rm $SSD_FILE
  touch $SSD_FILE
  touch $TRACE_FILE
  touch $META_FILE

  # Start the benchmarks
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=genrandom,loadsorted,writetracetoread,fasttrain,readtracesave \
    --step=$STEP \
    --seq_operation=$SEQ_OPS \
    --worker_threads=$WORKERS \
    --pp_threads=$PP_THREADS \
    --ssd_path=./$SSD_FILE \
    --csv_path=$PROF \
    --cool_pct=40 \
    --free_pct=1 \
    --contention_split=false \
    --xmerge=false \
    --print_tx_console=false \
    --recover=false \
    --persist=true \
    --persist_file=$META_FILE \
    --tracefile=$TRACE_FILE \
    --attached_segments_file=$ATTACH_SEG_FILE \
    --segments_file=$SPLINE_FILE \
    --secondary_mapping_file=$MAPPING_FILE \
    --max_error=$MAX_ERROR \
    --dram_gib=$DRAM
  # --dram_gib=${mem_80_percent} \
  ;;
fastretrain)
  # Create the ssd file for leanstore
  # rm $TRACE_FILE
  # rm $META_FILE
  # rm $SSD_FILE
  # touch $SSD_FILE

    # --dram_gib=${mem_80_percent} \
  # Start the benchmarks
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=fasttrain \
    --step=$STEP \
    --dram_gib=$DRAM \
    --seq_operation=$SEQ_OPS \
    --worker_threads=$WORKERS \
    --pp_threads=$PP_THREADS \
    --ssd_path=./$SSD_FILE \
    --csv_path=$PROF \
    --cool_pct=40 \
    --free_pct=1 \
    --contention_split=false \
    --xmerge=false \
    --print_tx_console=false \
    --recover=true \
    --persist=true \
    --persist_file=$META_FILE \
    --recover_file=$META_FILE \
    --tracefile=$TRACE_FILE \
    --attached_segments_file=$ATTACH_SEG_FILE \
    --segments_file=$SPLINE_FILE \
    --secondary_mapping_file=$MAPPING_FILE \
    --max_error=$MAX_ERROR
  ;;
retrainlr)
  # Create the ssd file for leanstore
  # rm $TRACE_FILE
  # rm $META_FILE
  # rm $SSD_FILE
  # touch $SSD_FILE

  # Start the benchmarks
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=readtraceload,trainlr \
    --dram_gib=${mem_80_percent} \
    --step=$STEP \
    --seq_operation=$SEQ_OPS \
    --worker_threads=$WORKERS \
    --pp_threads=$PP_THREADS \
    --ssd_path=./$SSD_FILE \
    --csv_path=$PROF \
    --cool_pct=40 \
    --free_pct=1 \
    --contention_split=false \
    --xmerge=false \
    --print_tx_console=false \
    --recover=true \
    --persist=true \
    --persist_file=$META_FILE \
    --recover_file=$META_FILE \
    --tracefile=$TRACE_FILE \
    --attached_segments_file=$ATTACH_SEG_FILE \
    --segments_file=$SPLINE_FILE \
    --secondary_mapping_file=$MAPPING_FILE \
    --max_error=$MAX_ERROR
  ;;
retrain)
  # Create the ssd file for leanstore
  # rm $TRACE_FILE
  # rm $META_FILE
  # rm $SSD_FILE
  # touch $SSD_FILE

  # Start the benchmarks
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=readtraceload,train \
    --dram_gib=${mem_80_percent} \
    --step=$STEP \
    --seq_operation=$SEQ_OPS \
    --worker_threads=$WORKERS \
    --pp_threads=$PP_THREADS \
    --ssd_path=./$SSD_FILE \
    --csv_path=$PROF \
    --cool_pct=40 \
    --free_pct=1 \
    --contention_split=false \
    --xmerge=false \
    --print_tx_console=false \
    --recover=true \
    --persist=true \
    --persist_file=$META_FILE \
    --recover_file=$META_FILE \
    --tracefile=$TRACE_FILE \
    --attached_segments_file=$ATTACH_SEG_FILE \
    --segments_file=$SPLINE_FILE \
    --secondary_mapping_file=$MAPPING_FILE \
    --max_error=$MAX_ERROR
  ;;
train)
  # Start the benchmarks
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=readtraceload,fasttrain \
    --dram_gib=${mem_80_percent} \
    --step=$STEP \
    --seq_operation=$SEQ_OPS \
    --worker_threads=$WORKERS \
    --pp_threads=$PP_THREADS \
    --ssd_path=./$SSD_FILE \
    --csv_path=$PROF \
    --cool_pct=40 \
    --free_pct=1 \
    --contention_split=false \
    --xmerge=false \
    --print_tx_console=false \
    --recover=true \
    --persist=false \
    --recover_file=$META_FILE \
    --tracefile=$TRACE_FILE \
    --attached_segments_file=$ATTACH_SEG_FILE \
    --segments_file=$SPLINE_FILE \
    --secondary_mapping_file=$MAPPING_FILE \
    --max_error=$MAX_ERROR
  ;;
readzip)
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=readtraceload,readzip \
    --step=$STEP \
    --seq_operation=$SEQ_OPS \
    --worker_threads=$WORKERS \
    --pp_threads=$PP_THREADS \
    --ssd_path=$SSD_FILE \
    --dram_gib=$DRAM \
    --csv_path=$PROF \
    --cool_pct=40 \
    --free_pct=1 \
    --contention_split=false \
    --xmerge=false \
    --print_tx_console=false \
    --recover=true \
    --persist=false \
    --recover_file=$META_FILE \
    --tracefile=$TRACE_FILE \
    --attached_segments_file=$ATTACH_SEG_FILE \
    --segments_file=$SPLINE_FILE \
    --secondary_mapping_file=$MAPPING_FILE \
    --max_error=$MAX_ERROR \
    --read=$READ \
    --readtime=$READTIME
  ;;
read)
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=readtraceload,readall \
    --step=$STEP \
    --seq_operation=$SEQ_OPS \
    --worker_threads=$WORKERS \
    --pp_threads=$PP_THREADS \
    --ssd_path=$SSD_FILE \
    --dram_gib=$DRAM \
    --csv_path=$PROF \
    --cool_pct=40 \
    --free_pct=1 \
    --contention_split=false \
    --xmerge=false \
    --print_tx_console=false \
    --recover=true \
    --persist=false \
    --recover_file=$META_FILE \
    --tracefile=$TRACE_FILE \
    --attached_segments_file=$ATTACH_SEG_FILE \
    --segments_file=$SPLINE_FILE \
    --secondary_mapping_file=$MAPPING_FILE \
    --max_error=$MAX_ERROR \
    --read=$READ \
    --readtime=$READTIME
  ;;
hotread)
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=readtraceload,readall,readall \
    --step=$STEP \
    --seq_operation=$SEQ_OPS \
    --worker_threads=$WORKERS \
    --pp_threads=$PP_THREADS \
    --ssd_path=./$SSD_FILE \
    --dram_gib=$DRAM \
    --csv_path=$PROF \
    --cool_pct=40 \
    --free_pct=1 \
    --contention_split=false \
    --xmerge=false \
    --print_tx_console=false \
    --recover=true \
    --persist=false \
    --recover_file=$META_FILE \
    --tracefile=$TRACE_FILE \
    --attached_segments_file=$ATTACH_SEG_FILE \
    --segments_file=$SPLINE_FILE \
    --secondary_mapping_file=$MAPPING_FILE \
    --max_error=$MAX_ERROR \
    --read=$READ \
    --readtime=$READTIME
  ;;
readzipseg)
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=readtraceload,readzipwithseg \
    --step=$STEP \
    --seq_operation=$SEQ_OPS \
    --worker_threads=$WORKERS \
    --pp_threads=$PP_THREADS \
    --ssd_path=./$SSD_FILE \
    --dram_gib=$DRAM \
    --csv_path=$PROF \
    --cool_pct=40 \
    --free_pct=1 \
    --contention_split=false \
    --xmerge=false \
    --print_tx_console=false \
    --recover=true \
    --persist=false \
    --recover_file=$META_FILE \
    --tracefile=$TRACE_FILE \
    --attached_segments_file=$ATTACH_SEG_FILE \
    --segments_file=$SPLINE_FILE \
    --secondary_mapping_file=$MAPPING_FILE \
    --max_error=$MAX_ERROR \
    --read=$READ \
    --readtime=$READTIME
  ;;
readseg)
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=readtraceload,readallwithseg \
    --step=$STEP \
    --seq_operation=$SEQ_OPS \
    --worker_threads=$WORKERS \
    --pp_threads=$PP_THREADS \
    --ssd_path=./$SSD_FILE \
    --dram_gib=$DRAM \
    --csv_path=$PROF \
    --cool_pct=40 \
    --free_pct=1 \
    --contention_split=false \
    --xmerge=false \
    --print_tx_console=false \
    --recover=true \
    --persist=false \
    --recover_file=$META_FILE \
    --tracefile=$TRACE_FILE \
    --attached_segments_file=$ATTACH_SEG_FILE \
    --segments_file=$SPLINE_FILE \
    --secondary_mapping_file=$MAPPING_FILE \
    --max_error=$MAX_ERROR \
    --read=$READ \
    --readtime=$READTIME
  ;;
hotreadseg)
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=readtraceload,readallwithseg,readallwithseg \
    --step=$STEP \
    --seq_operation=$SEQ_OPS \
    --worker_threads=$WORKERS \
    --pp_threads=$PP_THREADS \
    --ssd_path=./$SSD_FILE \
    --dram_gib=$DRAM \
    --csv_path=$PROF \
    --cool_pct=40 \
    --free_pct=1 \
    --contention_split=false \
    --xmerge=false \
    --print_tx_console=false \
    --recover=true \
    --persist=false \
    --recover_file=$META_FILE \
    --tracefile=$TRACE_FILE \
    --attached_segments_file=$ATTACH_SEG_FILE \
    --segments_file=$SPLINE_FILE \
    --secondary_mapping_file=$MAPPING_FILE \
    --max_error=$MAX_ERROR \
    --read=$READ \
    --readtime=$READTIME
  ;;
readrs)
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=readtraceload,readallwithrs \
    --step=$STEP \
    --seq_operation=$SEQ_OPS \
    --worker_threads=$WORKERS \
    --pp_threads=$PP_THREADS \
    --ssd_path=./$SSD_FILE \
    --dram_gib=$DRAM \
    --csv_path=$PROF \
    --cool_pct=40 \
    --free_pct=1 \
    --contention_split=false \
    --xmerge=false \
    --print_tx_console=false \
    --recover=true \
    --persist=false \
    --recover_file=$META_FILE \
    --tracefile=$TRACE_FILE \
    --attached_segments_file=$ATTACH_SEG_FILE \
    --segments_file=$SPLINE_FILE \
    --secondary_mapping_file=$MAPPING_FILE \
    --max_error=$MAX_ERROR \
    --read=$READ \
    --readtime=$READTIME
  ;;
hotreadrs)
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=readtraceload,readallwithrs,readallwithrs \
    --step=$STEP \
    --seq_operation=$SEQ_OPS \
    --worker_threads=$WORKERS \
    --pp_threads=$PP_THREADS \
    --ssd_path=./$SSD_FILE \
    --dram_gib=$DRAM \
    --csv_path=$PROF \
    --cool_pct=40 \
    --free_pct=1 \
    --contention_split=false \
    --xmerge=false \
    --print_tx_console=false \
    --recover=true \
    --persist=false \
    --recover_file=$META_FILE \
    --tracefile=$TRACE_FILE \
    --attached_segments_file=$ATTACH_SEG_FILE \
    --segments_file=$SPLINE_FILE \
    --secondary_mapping_file=$MAPPING_FILE \
    --max_error=$MAX_ERROR \
    --read=$READ \
    --readtime=$READTIME
  ;;
origin)
  rm $SSD_FILE
  touch $SSD_FILE

  $PREFIX_CMD ${BUILD_DIR}/frontend/ycsb \
    --ycsb_tuple_count=1000000 \
    --ycsb_read_ratio=0 \
    --worker_threads=4 \
    --pp_threads=4 \
    --ssd_path=./$SSD_FILE \
    --dram_gib=1 \
    --csv_path=./log \
    --cool_pct=40 \
    --free_pct=1 \
    --contention_split \
    --xmerge \
    --print_tx_console=false \
    --print_debug=false
  ;;
clear)
  # remove all the related files
  rm ${PROF}_*.csv
  rm *.log
  rm $TRACE_FILE
  rm $SSD_FILE
  rm $META_FILE
  ;;
*)
  help
  ;;
esac