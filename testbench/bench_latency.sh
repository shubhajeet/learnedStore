#!/bin/bash
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
echo "bench_latency.sh $conf $exp"
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
echo "Sequential Workload: $STEP"
echo "Sequential Operations: $SEQ_OPS"

if [ $MODE = "Debug" ]; then
  echo "run mode: $MODE"
  PREFIX_CMD="gdb --args"
# PREFIX_CMD="gdb -ex run --args"
elif [ $MODE = "RelWithDebInfo" ]; then
  echo "run mode: $MODE"
  PREFIX_CMD="gdb --args"
else
  # Actual benchmark command
  PREFIX_CMD="numactl -C 1 -m 1 sudo "
  # PREFIX_CMD="numactl -N 1 -m 1 sudo "
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
scratchsimulate)
  # Create the ssd file for leanstore
  # touch $SSD_FILE
  # Start the benchmarks
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=genrandom,load,writetracetoread,statistics,simulatereadlat \
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
scratchzipfian)
  # Create the ssd file for leanstore
  # touch $SSD_FILE
  # Start the benchmarks
  # Not impelemented yet1
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=genrandom,load,fasttrain,writetracetoread,statistics,readzipflat \
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
scratchftail)
  # Create the ssd file for leanstore
  # touch $SSD_FILE
  # Start the benchmarks
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=genrandom,load,fasttrain,writetracetoread,statistics,readftaillat \
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
inmem)
  # Create the ssd file for leanstore
  touch $SSD_FILE
  # Start the benchmarks
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=genrandom,load,writetracetoread,fasttrain,readlatwithseg \
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
    # --recover_file=$META_FILE \
    # --benchmarks=writetraceload,load,writetracetoread,fasttrain,readLatall,readLatallwithseg,readftaillat \
  ;;
readzipfian)
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=readtraceload,readzipflat \
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
read)
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=readtraceload,readlat \
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
hotread)
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=readtraceload,readlat,readlat \
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
readsegzipfian)
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=readtraceload,readzipflatwithseg \
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
    --benchmarks=readtraceload,readlatwithseg \
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
    --benchmarks=readtraceload,readLatallwithseg,readlatwithseg \
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
insertlat)
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1000 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=genrandom,loadlat,statistics \
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
    # --recover_file=$META_FILE \
    # --benchmarks=writetraceload,load,writetracetoread,fasttrain,readLatall,readLatallwithseg,readftaillat \
  ;;
lookup)
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1000 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=genrandom,load,statistics,writetracetoread,readlat \
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
    # --recover_file=$META_FILE \
    # --benchmarks=writetraceload,load,writetracetoread,fasttrain,readLatall,readLatallwithseg,readftaillat \
  ;;
lookupseg)
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1000 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=genrandom,load,writetracetoread,fasttrain,readlatwithseg \
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
    # --recover_file=$META_FILE \
    # --benchmarks=writetraceload,load,writetracetoread,fasttrain,readLatall,readLatallwithseg,readftaillat \
  ;;
lookuprs)
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1000 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=genrandom,load,writetracetoread,fasttrain,readlatwithrs \
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
    # --recover_file=$META_FILE \
    # --benchmarks=writetraceload,load,writetracetoread,fasttrain,readLatall,readLatallwithseg,readftaillat \
  ;;
lookuplr)
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1000 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=genrandom,load,writetracetoread,trainlr,readallwithlr \
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
    # --recover_file=$META_FILE \
    # --benchmarks=writetraceload,load,writetracetoread,fasttrain,readLatall,readLatallwithseg,readftaillat \
  ;;
*)
  help
  ;;
esac