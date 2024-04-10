# YCSB Benchmark

## How to run the benchmark
1. Create a folder `build` in the root directory
2. Go to the `build`, and `cmake -DCMAKE_BUILD_TYPE=release ..`
3. Go back to testbench folder
4. Execute `sudo bash bench_leanstore.sh`


## Parameters
Mainly focus on the following parameters
1. num: number of operations
2. benchmarks: the benchmarks you want to run
3. worker_threads: number of worker threads
4. pp_threads: number of background threads
5. dram_gib: the dram capacity
