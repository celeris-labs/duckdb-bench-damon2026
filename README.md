# DaMoN'26 Benchmark for "Should I Hide My Duck in the Lake?"

To build:
```
mkdir build && cd build
cmake ..
make -j 32
cd ..
```

To generate data (the benchmark should be run on alveo-box-01):
```
python3 generate.py -s 1 -f parquet -o data/tpch-1
```

To put data into tmpfs on the HACC cluster:
```
sudo tmpfs-create
cp -r data/tpch-1 /mnt/ramdisk
```

To run:
```
./run_all.sh
```
