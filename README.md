# LearnedStore
[LearnedStore](https://jiangs.utasites.cloud/pubs/papers/Maharjan23-LearnedStore.pdf) [IEEExplorer](https://ieeexplore.ieee.org/abstract/document/10499467) uses learned index to accelerate search in btree based KV database. The project has been adapted from [LeanStore](https://db.in.tum.de/~leis/papers/leanstore.pdf) commit point: [d3d83143ee74c54c901fe5431512a46965377f4e](https://github.com/leanstore/leanstore/commit/d3d83143ee74c54c901fe5431512a46965377f4e), a high-performance OLTP storage engine optimized for many-core CPUs and NVMe SSDs. 

## Compiling
Cloning repositories and it's submodules:
` git clone --recurse-submodules git@github.com:shubhajeet/learnedStore.git`

Install dependencies:

`sudo apt-get install cmake libaio-dev libtbb-dev libsparsehash-dev`

Installing submodules
- you also need to install my [instrumentation library](https://github.com/shubhajeet/cppInstrumentation) -> location: submodules/instrumentation

```bash
mkdir submodules/instrumentation/build
cd submodules/instrumentation/build
cmake -DCMAKE_BUILD_TYPE=Release .. & make -j
```
- some hash map library https://github.com/skarupke/flat_hash_map -> location: submodules/flat_hash_map

`mkdir build_Release && cd build_Release && cmake -DCMAKE_BUILD_TYPE=release .. && make -j`

## Downloading Dataset
Please refer to the [SOSD](https://github.com/learnedsystems/SOSD) paper to get the SOSD Dataset. Benchmarking under the SOSD Databaset can be done using bench_dataset.sh script. The dataset must be stored in data folder.

## Running benchmark
bechmark scripts are in testbench folder. Scripts are bench_<>.sh. The config files are with .cfg extension and some example config can be found in the repo.

The experiment can be run using following command.
`bench_<>.sh <>.cfg <exp_name>`

Appropriate disk file should be created before running the experiments.

- bench_learnstore.sh :: script to measure leanstore and learnstore on created workload
- bench_latency.sh :: script to measure the latency
- bench_dataset.sh :: read only throughput experiments

## Experiment Figure Regeneration
1. Figure 2
```bash
./bench_learnstore.sh 200M.cfg read
./bench_learnstore.sh 200M.cfg readseg
```

2. Figure 3 : Measuring latency
Create SSD Dataset
```bash
./bench_learnstore.sh 200M_512b.cfg create
```
Measure latency
```bash
./bench_latency.sh 200M_512b.cfg read | tee /tmp/leanstore_cold_latency.log
./bench_latency.sh 200M_512b.cfg readseg | tee /tmp/learnstore_cold_latency.log
```
Convert to csv
```bash
grep latency: leanstore_cold_latency.log | cut -d' ' -f 2 | tee leanstore_cold_latency.log
```
Create graphs using experiments/percentile.ipynb

3. Figure 4 : The figure can be created using the dot file: experiment/state.dot
4. Figure 5 : Read only workload throughput
Read only throughput. Auto train should be disabled.
```bash
./bench_dataset.sh 200M.cfg lineargen
./bench_dataset.sh 200M.cfg randomgen
./bench_dataset.sh 200M.cfg pieceLinear
./bench_dataset.sh 200M.cfg amzn
./bench_dataset.sh 200M.cfg fb
./bench_dataset.sh 200M.cfg logn
./bench_dataset.sh 200M.cfg norm
```

## Cite
The code we used for our HDIS 2023 paper

```
@inproceedings{maharjanLearnedStore,
    author    = {Sujit Maharjan},
    title     = {From LeanStore to LearnedStore: Using a Learned Index to Improve Database Index Search},
    booktitle = {HDIS},
    year      = {2023}
}
```
