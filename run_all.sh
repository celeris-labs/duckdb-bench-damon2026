#!/bin/bash

./build/damon_bench -s parquet -t 1
./build/damon_bench -s parquet -t 2
./build/damon_bench -s parquet -t 3
./build/damon_bench -s parquet -t 4
./build/damon_bench -s parquet -t 5
./build/damon_bench -s parquet -t 6
./build/damon_bench -s parquet -t 7
./build/damon_bench -s parquet -t 8

./build/damon_bench -s memory -t 1
./build/damon_bench -s memory -t 2
./build/damon_bench -s memory -t 3
./build/damon_bench -s memory -t 4
./build/damon_bench -s memory -t 5
./build/damon_bench -s memory -t 6
./build/damon_bench -s memory -t 7
./build/damon_bench -s memory -t 8