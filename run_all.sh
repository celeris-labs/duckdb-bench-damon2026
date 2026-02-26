#!/bin/bash

per_query="numactl -m 0 -N 0 ./build/queries"
throughput="numactl -m 0 -N 0 ./build/throughput"

sudo tmpfs-create -s 32 -n 0

# Figure 1(a)
mkdir -p measurements/throughput
python3 generate.py -s 10 -f parquet -o data/tpch-10
cp -r data/tpch-10 /mnt/ramdisk

sources=( parquet memory filtered )
for s in "${sources[@]}"
do
    $throughput -s $s -t 1-64 -S 3
done

mkdir -p measurements/throughput/tpch-10
mv measurements/throughput/*.json measurements/throughput/tpch-10

# Figure 1(b)
rm /mnt/ramdisk/*
python3 generate.py -s 30 -f parquet -o data/tpch-30
cp -r data/tpch-30 /mnt/ramdisk

sources=( parquet memory filtered )
for s in "${sources[@]}"
do
    for t in $(seq 1 64)
    do
        $throughput -s $s -t $t -S 4
    done
done

mkdir -p measurements/throughput/tpch-30
mv measurements/throughput/*.json measurements/throughput/tpch-30

# Figure 2
mkdir -p measurements/queries
sources=( parquet memory filtered )
for s in "${sources[@]}"
do
    $per_query -s $s -t 1
done

mkdir -p measurements/queries/tpch-30
mv measurements/queries/*.json measurements/queries/tpch-30

# Figure 3(a): CSV
rm /mnt/ramdisk/*
python3 generate.py -s 10 -f csv -o data/tpch-10-csv
cp -r data/tpch-10-csv /mnt/ramdisk

$throughput -s csv -t 1-64 -S 3

# Figure 3(a): JSON
rm /mnt/ramdisk/*
python3 generate.py -s 10 -f json -o data/tpch-10-json
cp -r data/tpch-10-json /mnt/ramdisk

$throughput -s csv -t 1-64 -S 3

mkdir -p measurements/throughput/other-10
mv measurements/throughput/*.json measurements/throughput/other-10

# Figure 3(b): Random
rm /mnt/ramdisk/*
python3 generate.py -s 30 -f parquet -o data/tpch-30-random -O random
cp -r data/tpch-30-random /mnt/ramdisk

$per_query -s parquet -t 1

mkdir -p measurements/queries/tpch-30-random
mv measurements/queries/*.json measurements/queries/tpch-30-random

# Figure 3(b): Sorted
rm /mnt/ramdisk/*
python3 generate.py -s 30 -f parquet -o data/tpch-30-sorted -O sorted
cp -r data/tpch-30-sorted /mnt/ramdisk

$per_query -s parquet -t 1

mkdir -p measurements/queries/tpch-30-sorted
mv measurements/queries/*.json measurements/queries/tpch-30-sorted
