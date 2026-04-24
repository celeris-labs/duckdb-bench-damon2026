# DaMoN'26 Benchmark for "Should I Hide My Duck in the Lake?"
This is the benchmarking code used to get the results presented in the short paper "Should I Hide My Duck in the Lake?". When cloning the repo, you have to initialize the submodules with:

``` bash
git submodule update --init --recursive
```

## Building the benchmark
To build the benchmark:
``` bash
mkdir build && cd build
cmake ..
make -j 64
cd ..
```

## Getting the data
To generate scale factor 30 as Parquet files in the directory `data/tpch-30` with default TPC-H ordering execute:
``` bash
./generate -b tpch -s 30
```

To get more options, including how to generate TPC-DS data, execute with `-h`. The ClickBench Parquet file can be downloaded with `./download_clickbench.sh`.

To put data into tmpfs on the HACC cluster (the benchmark were run on hacc-box-01 of the HACC cluster):
``` bash
sudo tmpfs-create -s 32 -n 0
cp -r data/tpch-30 /mnt/ramdisk
```

## Executing experiments & generating plots
To run experiments and plot the results (this requires some manual moving the measurements to the correct folders for now):
``` bash
./run_all.sh
python3 plot.py
```
