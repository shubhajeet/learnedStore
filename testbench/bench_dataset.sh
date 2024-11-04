#!/bin/bash
exp=read
conf=in_mem.cfg
# Check if there is a command line argument
function help() {
  echo "./bench_dataset.sh <cfg> <dataset>" # arguments are accessible through $1, $2,...
}

if [[ $# -eq 2 ]]; then
  conf=$1
  exp=$2
else
  help
  echo -n ">>"
  read exp
fi

# print current command and parameters
echo "bench_dataset.sh $conf $exp"
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
echo "Max Error: $MAX_ERROR"
echo "Collect stats: $COLLECT_STATS"
# echo "Attach segment stored in file: $ATTACH_SEG_FILE"
echo "Spline points in file: $SPLINE_FILE"
echo "Mapping in file: $MAPPING_FILE"
echo "Sequential Workload: $SEQ_TRACE "
echo "Sequential Operations: $SEQ_OPS"
echo "2nd Stage RMI linear models: $NMODELS"

if [ $MODE = "Debug" ]; then
  echo "run mode: $MODE"
  PREFIX_CMD="sudo gdb --args"
# PREFIX_CMD="gdb -ex run --args"
elif [ $MODE = "RelWithDebInfo" ]; then
  echo "run mode: $MODE"
  PREFIX_CMD="sudo gdb --args"
else
  # Actual benchmark command
  # PREFIX_CMD="numactl -N 1 -m 1 sudo"
  PREFIX_CMD="numactl -N 1 -m 1 sudo"
fi
# run command in gdb
BUILD_DIR=../build_$MODE/
bash compile.sh $BUILD_DIR
# dstat -c -m -d -D total,nvme0n1 -r -fs -T 1
sudo bash drop_cache.sh
# key size
# KEY_SIZE=uint32
KEY_SIZE=uint64

case $exp in
linear)
  TRACE_FILE=../data/linear/perflinear.csv
  ;;
simplelinear)
  TRACE_FILE=../data/linear/simple_linear.csv
  ;;
pieceLinear)
  TRACE_FILE=../data/gendata/linseg8_200M.csv
  ;;
amzn)
  TRACE_FILE=../data/SOSD/books_200M_${KEY_SIZE}
  ;;
fb)
  TRACE_FILE=../data/SOSD/fb_200M_${KEY_SIZE}
  ;;
logn)
  TRACE_FILE=../data/SOSD/lognormal_200M_${KEY_SIZE}
  ;;
norm)
  TRACE_FILE=../data/SOSD/normal_200M_${KEY_SIZE}
  ;;
uniform_sparse)
  TRACE_FILE=../data/SOSD/uniform_sparse_200M_${KEY_SIZE}
  ;;
uniform_dense)
  TRACE_FILE=../data/SOSD/uniform_dense_200M_${KEY_SIZE}
  ;;
clear)
  # remove all the related files
  rm ${PROF}_*.csv
  rm *.log
  rm $TRACE_FILE
  rm $SSD_FILE
  rm $META_FILE
  ;;
esac

case $exp in
lineargen)
  # --benchmarks=genlinear,load,fasttrain,trainlr,writetracetoread,readall,readallwithlr,readallwithrs,readallwithseg \
  # --benchmarks=genlinear,loadsorted,fasttrain,trainlr,creaters,creatermi,writetracetoread,readall,readallrs,readallrmi,readallwithlr,readallwithrs,readallwithrmi,readallwithseg \
  # --benchmarks=genlinear,loadsorted,trainlr,writetracetoread,readallwithlr \
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --step=$STEP \
    --benchmarks=genlinear,loadsorted,fasttrain,trainlr,creaters,creatermi,writetracetoread,readall,readallrs,readallrmi,readallwithlr,readallwithrs,readallwithrmi,readallwithseg \
    --nmodels=$NMODELS \
    --max_error=$MAX_ERROR \
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
    --persist=false
  ;;
randomgen)
  # --benchmarks=genrandom,loadsorted,fasttrain,trainlr,creaters,creatermi,writetracetoread,readall,readallrs,readallrmi,readallwithlr,readallwithrs,readallwithrmi,readallwithseg \
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --nmodels=$NMODELS \
    --benchmarks=genrandom,loadsorted,fasttrain,trainlr,writetracetoread,readall,readallwithlr,readallwithrs,readallwithrmi,readallwithseg \
    --seq_operation=$SEQ_OPS \
    --max_error=$MAX_ERROR \
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
    --persist=false
  ;;
*)
  $PREFIX_CMD ${BUILD_DIR}/frontend/benchmark_ycsb \
    --report_interval=1 \
    --num=$NUM \
    --batch=100 \
    --benchmarks=writetraceload,loadsorted,fasttrain,writetracetoread,readall,readallwithrs,readallwithseg \
    --seq_operation=$SEQ_OPS \
    --max_error=$MAX_ERROR \
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
    --tracefile=$TRACE_FILE
  # --benchmarks=writetraceload,load,fasttrain,trainlr,writetracetoread,readall,readallwithlr,readallwithrs,readallwithseg \
  ;;
*)
  help
  ;;
esac
