# LearnedStore
[LearnedStore](https://jiangs.utasites.cloud/pubs/papers/Maharjan23-LearnedStore.pdf) uses learned index to accelerate search in btree based KV database. The project has been adapted from [LeanStore](https://db.in.tum.de/~leis/papers/leanstore.pdf) commit point: [d3d83143ee74c54c901fe5431512a46965377f4e](https://github.com/leanstore/leanstore/commit/d3d83143ee74c54c901fe5431512a46965377f4e), a high-performance OLTP storage engine optimized for many-core CPUs and NVMe SSDs. 

## Compiling
Install dependencies:

`sudo apt-get install cmake libaio-dev libtbb-dev libsparsehash-dev`

- you also need to install my [instrumentation library](https://github.com/shubhajeet/cppInstrumentation) -> location: projects/instrumentation
- some hash map library https://github.com/skarupke/flat_hash_map -> location: projects/flat_hash_map

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
