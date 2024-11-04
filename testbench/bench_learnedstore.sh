#!/bin/bash
# Define functions for help message and error handling
function help {
  echo "Usage: $0 (load|read|inmem|readzipfian|read|hotread|clear) [CONFIG_FILE]"
  echo "  load: Load data to LeanStore and store the trace."
  echo "  read: Recover data from LeanStore by reading the trace."
  echo "  inmem: Run the in-memory benchmark."
  echo "  readzipfian: Read data using a Zipfian distribution."
  echo "  read: Read data using a uniform random distribution."
  echo "  hotread: Perform multiple read operations consecutively."
  echo "  clear: Remove all related files."
  echo "  CONFIG_FILE (optional): Path to the configuration file (defaults to in_mem.cfg)."
}

function error_exit {
  msg="$1"
  echo "Error: $msg" >&2
  exit 1
}

exp=read
conf=in_mem.cfg
# Check if there is a command line argument
if [[ $# -gt 1 ]]; then
  conf=$1
  exp=$2
else
  help
  echo -n ">>"
  read exp
fi

# print current command and parameters
echo "bench_learnedstore.sh $conf $exp"
echo "---------------------------"
date

# load configuration file
conf_path=$(dirname $conf)
full_conf=$conf
conf=$(basename $conf)
source $full_conf
source db.sh
source dram.sh

# parse command line
# Parse command line arguments to overwrite the config values
shift
shift
# Parse the command-line arguments and override the values if provided
exp_info=""
while [[ "$#" -gt 0 ]]; do
  exp_info="${exp_info}_$(echo $1 | sed s/--//)_${2}"
  case $1 in
  --MODE)
    MODE="$2"
    shift
    ;;
  --workers)
    WORKERS="$2"
    shift
    ;;
  --num)
    NUM="$2"
    shift
    ;;
  --dram)
    DRAM="$2"
    shift
    ;;
  --collect_stats)
    COLLECT_STATS="$2"
    shift
    ;;
  --mode)
    MODE="$2"
    shift
    ;;
  --pp_threads)
    PP_THREADS="$2"
    shift
    ;;
  --max_error)
    MAX_ERROR="$2"
    shift
    ;;
  --seq_ops)
    SEQ_OPS="$2"
    shift
    ;;
  --seq_write_ops)
    SEQ_WRITE_OPS="$2"
    shift
    ;;
  --step)
    STEP="$2"
    shift
    ;;
  --readtime)
    READTIME="$2"
    shift
    ;;
  --ssd_file)
    SSD_FILE="$2"
    shift
    ;;
  --meta_file)
    META_FILE="$2"
    shift
    ;;
  --trace_file)
    TRACE_FILE="$2"
    shift
    ;;
  --stats_loc)
    STATS_LOC="$2"
    shift
    ;;
  --attach_seg_file)
    ATTACH_SEG_FILE="$2"
    shift
    ;;
  --spline_file)
    SPLINE_FILE="$2"
    shift
    ;;
  --mapping_file)
    MAPPING_FILE="$2"
    shift
    ;;
  --prof)
    PROF="$2"
    shift
    ;;
  --*)
    echo "Unknown parameter passed: $1"
    exit 1
    ;;
  esac
  shift
done
PERSIST=false
RECOVER=false

# use variables defined in the configuration file

pmzr='perf stat -e cycles:u,instructions,cache-references,cache-misses,page-faults,L1-dcache-loads,L1-dcache-load-misses,LLC-loads,LLC-load-misses,LLC-stores,LLC-store-misses,branch-load-misses '
if [ "$MODE" = "Debug" ]; then
  echo "run mode: $MODE"
  PREFIX_CMD="gdb --args"
# PREFIX_CMD="gdb -ex run --args"
elif [ "$MODE" = "RelWithDebInfo" ]; then
  echo "run mode: $MODE"
  PREFIX_CMD="sudo gdb --args"
else
  # Actual benchmark command
  # This does not force data to be in same numa node
  # PREFIX_CMD=""
  # PREFIX_CMD="numactl -C 1 -m 1 sudo $pmzr"
  # PREFIX_CMD="numactl -C 9 -m 1 sudo"
  # PREFIX_CMD="numactl -C 11 -m 1 sudo"
  # This command will run the given command with the given args on cores 1,3 on a NUMA machine
  # PREFIX_CMD="numactl -C 1,3 sudo"
  # PREFIX_CMD="numactl -C 7,9 -m 1 sudo "

  # PREFIX_CMD="sudo gdb --args"
  # This forces data to be in same numa node
  # PREFIX_CMD="numactl -C 1,3,5,7,9,11,13,15,17,19 -m 1 sudo"
  # PREFIX_CMD="numactl -C 1 -m 1 sudo"
  PREFIX_CMD="numactl -N 1 -m 1 sudo"

  # PREFIX_CMD="sudo"
fi
# run command in gdb
BUILD_DIR=../build_$MODE/
bash compile.sh $BUILD_DIR
# dstat -c -m -d -D total,nvme0n1 -r -fs -T 1
sudo bash drop_cache.sh
# COLLECT_STATS=true
collect_pstat_stats() {
  dstat -c -m -d -D /dev/sda1 -r -fs -T --output ${STATS_LOC}/${exp_info}_dstat.csv 1 &
  # PID=$1
  # pidstat -d -p $PID 1 > ${STATS_LOC}/#{exp_info}_pidstats.log &
  PIDPSTATSTAT1=$!
}

collect_iostat_stats() {
  iostat -yxmt 1 >${STATS_LOC}/${exp_info}_iostat.log &
  PIDPSTATSTAT2=$!
}

kill_collect_stats() {
  sudo kill -9 $PIDPSTATSTAT1
  sudo kill -9 $PIDPSTATSTAT2
  # Kill all remaining background jobs
  for job in $(jobs -p); do
    sudo kill $job
  done
  # bash ../experiments/get_iostat.sh ${STATS_LOC}_iostat.log ${STATS_LOC}_iostat.png
}

case $exp in
overhead)
  remove_leanstore_db
  create_leanstore_db
  BENCHMARK=genrandom,fastload,statistics,writetracetoread,readall,readallwithseg
  ;;
scratchzip)
  remove_leanstore_db
  create_leanstore_db
  BENCHMARK=genrandom,loadsorted,statistics,writetracetoread,fasttrain,readzip,readzipwithseg
  ;;
scratch)
  remove_leanstore_db
  create_leanstore_db
  BENCHMARK=writetraceload,load,statistics,writetracetoread,readall
  ;;
load)
  BENCHMARK=genrandom,load
  ;;
load_fast)
  BENCHMARK=genrandom,fastload
  ;;
scanall)
  BENCHMARK=genrandom,load,statistics,fasttrain,writetracetoread,scanallasc,scanallascseg

  ;;
scan)
  BENCHMARK=genrandom,load,statistics,fasttrain,writetracetoread,scanasc,scanascseg
  ;;
inmem)
  BENCHMARK=genlinear,load,writetracetoread,readall
  ;;
inmem_fast)
  remove_leanstore_db
  create_leanstore_db
  BENCHMARK=genrandom,fastload,writetracetoread,readallwithseg
  ;;
insert)
  BENCHMARK=readtraceload,genrandom,load
  RECOVER=true
  ;;
insertfast)
  BENCHMARK=genrandom,fastload,statistics
  ;;
twoinsert)
  remove_leanstore_db
  create_leanstore_db
  BENCHMARK=genrandom,load,writetracetoread,readall,genrandom,load,writetracetoread,readall
  ;;
twoinsert_sorted)
  BENCHMARK=genrandom,load,writetracetoread,genrandom,loadsorted
  ;;
twoinsertfast_sorted)
  BENCHMARK=genrandom,fastload,writetracetoread,genrandom,fastloadsorted
  ;;
twoinsertfast)
  BENCHMARK=genrandom,fastload,writetracetoread,genrandom,fastload
  ;;
ycsba)
  remove_leanstore_db
  create_leanstore_db
  BENCHMARK=genrandom,load,writetracetoread,genrandom,ycsba
  ;;
ycsbaseg)
  remove_leanstore_db
  create_leanstore_db
  BENCHMARK=genrandom,fastload,writetracetoread,fasttrain,genrandom,ycsbaseg
  ;;
ycsbb)
  remove_leanstore_db
  create_leanstore_db
  BENCHMARK=genrandom,load,writetracetoread,fasttrain,genrandom,ycsbb
  ;;
ycsbbseg)
  remove_leanstore_db
  create_leanstore_db
  BENCHMARK=genrandom,fastload,writetracetoread,fasttrain,genrandom,ycsbbseg
  ;;
create_sorted)
  # Create the ssd file for leanstore
  remove_leanstore_db
  remove_trace
  create_leanstore_db
  touch $TRACE_FILE
  # BENCHMARK=genrandom,loadsorted,writetracetoread,fasttrain,readtracesave

  # Start the benchmarks
  BENCHMARK=genrandom,loadsorted,writetracetoread,fasttrain,readtracesave
  DRAM=${mem_80_percent}
  ;;
clear)
  remove_trace
  remove_leanstore_db
  exit 0
  ;;
create)
  # Create the ssd file for leanstore
  remove_leanstore_db
  create_leanstore_db

  # Start the benchmarks
  BENCHMARK=genrandom,load,writetracetoread,fasttrain,readtracesave
  RECOVER=false
  PERSIST=true
  DRAM=100
  # DRAM=${mem_80_percent}
  ;;
fastretrain)
  # Create the ssd file for leanstore
  # rm $TRACE_FILE
  # rm $META_FILE
  # rm $SSD_FILE
  # touch $SSD_FILE

  # DRAM=${mem_80_percent}
  # Start the benchmarks
  BENCHMARK=fasttrain
  RECOVER=true
  PERSIST=true
  ;;
retrain)
  # Create the ssd file for leanstore
  # rm $TRACE_FILE
  # rm $META_FILE
  # rm $SSD_FILE
  # touch $SSD_FILE

  BENCHMARK=readtraceload,train
  DRAM=${mem_80_percent}
  RECOVER=true
  PERSIST=true
  ;;
train)
  # Start the benchmarks
  BENCHMARK=readtraceload,fasttrain
  DRAM=${mem_80_percent}
  ;;
readzip)
  BENCHMARK=readtraceload,readzip
  RECOVER=true
  PERSIST=false
  ;;
read)
  BENCHMARK=readtraceload,readall
  RECOVER=true
  PERSIST=false
  ;;
hotread)
  BENCHMARK=readtraceload,readall,readall
  RECOVER=true
  PERSIST=false
  ;;
readzipseg)
  BENCHMARK=readtraceload,readzipwithseg
  RECOVER=true
  PERSIST=false
  ;;
readseg)
  BENCHMARK=readtraceload,readallwithseg
  RECOVER=true
  PERSIST=false
  ;;
hotreadseg)
  BENCHMARK=readtraceload,readallwithseg,readallwithseg
  RECOVER=true
  PERSIST=false
  ;;
origin)
  rm $SSD_FILE
  touch $SSD_FILE
  ;;
esac

# Function to check if a path is relative
is_relative() {
  case $1 in
  /*) return 1 ;; # Absolute path
  *) return 0 ;;  # Relative path
  esac
}

# Prepend conf_path to relative paths
if is_relative "$SSD_FILE"; then
  SSD_FILE="$conf_path/$SSD_FILE"
fi
if is_relative "META_FILE"; then
  META_FILE="$conf_path/$META_FILE"
fi
if is_relative "$SPLINE_FILE"; then
  SPLINE_FILE="$conf_path/$SPLINE_FILE"
fi
if is_relative "$TRACE_FILE"; then
  TRACE_FILE="$conf_path/$TRACE_FILE"
fi
if is_relative "$MAPPING_FILE"; then
  MAPPING_FILE="$conf_path/$MAPPING_FILE"
fi
if is_relative "$ATTACH_SEG_FILE"; then
  ATTACH_SEG_FILE="$conf_path/$ATTACH_SEG_FILE"
fi

# Example: print the final configuration to see what values are being used
echo "Experiment Config File: $EXP_CONFIG_FILE"
echo "SSD FILE: $SSD_FILE"
# echo "ROCKSDBDATABASE: $DATABASE"
echo "NUM: $NUM"
echo "DRAM: $DRAM"
# echo "ROCKSDBBRAM: $ROCKSDBDRAM"
echo "COLLECT_STATS: $COLLECT_STATS"
echo "MODE: $MODE"
echo "WORKERS: $WORKERS"
echo "PP_THREADS: $PP_THREADS"
echo "SEQ_THREADS: $SEQ_THREADS"
echo "NMODELS: $NMODELS"
echo "MAX_ERROR: $MAX_ERROR"
echo "SEQ_OPS: $SEQ_OPS"
echo "SEQ_WRITE_OPS: $SEQ_WRITE_OPS"
echo "STEP: $STEP"
echo "READTIME: $READTIME"
echo "SSD_FILE: $SSD_FILE"
echo "META_FILE: $META_FILE"
echo "TRACE_FILE: $TRACE_FILE"
echo "STATS_LOC: $STATS_LOC"
echo "ATTACH_SEG_FILE: $ATTACH_SEG_FILE"
echo "SPLINE_FILE: $SPLINE_FILE"
echo "MAPPING_FILE: $MAPPING_FILE"
echo "PROF: $PROF"

if [ $COLLECT_STATS = true ]; then
  # start stats collection
  collect_pstat_stats
  collect_iostat_stats
fi

$PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
  --report_interval=1 \
  --num=$NUM \
  --batch=100 \
  --step=$STEP \
  --benchmarks=$BENCHMARK \
  --seq_operation=$SEQ_OPS \
  --seq_write_operation=$SEQ_WRITE_OPS \
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
  --recover=$RECOVER \
  --recover_file=$META_FILE \
  --persist=$PERSIST \
  --persist_file=$META_FILE \
  --tracefile=$TRACE_FILE \
  --attached_segments_file=$ATTACH_SEG_FILE \
  --segments_file=$SPLINE_FILE \
  --secondary_mapping_file=$MAPPING_FILE \
  --max_error=$MAX_ERROR \
  --readtime=$READTIME

if [ $COLLECT_STATS = true ]; then
  # start stats collection
  kill_collect_stats
fi
