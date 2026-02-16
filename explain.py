import duckdb
import json
from enum import Enum
from glob import glob
import argparse

class Source(Enum):
    PARQUET   = 0
    CSV       = 1
    JSON      = 2
    MEMORY    = 3
    FILTERED  = 4
    PROJECTED = 5

def load_data(con, data_dir: str, source: Source):
    tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
    for table in tables:
        if source == Source.PARQUET:
            con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{data_dir}/{table}.parquet');")
        elif source == Source.CSV:
            con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_csv('{data_dir}/{table}.csv');")
        elif source == Source.JSON:
            con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_json('{data_dir}/{table}.json');")
        else:
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM read_parquet('{data_dir}/{table}.parquet');")
    
    if source == Source.FILTERED or source == Source.PROJECTED:
        for filename in glob(f"sql/filtered_views/*.sql"):
            with open(filename, "r") as f:
                con.execute(f.read())
    if source == Source.PROJECTED:
        for filename in glob(f"sql/projected_views/*.sql"):
            with open(filename, "r") as f:
                con.execute(f.read())

def explain_query(data_dir: str, source: Source, query_file: str):
    con = duckdb.connect(":memory:")
    
    load_data(con, data_dir, source)
    
    with open(query_file, "r") as f:
        query = f.read()
    
    explain_result = con.execute(f"EXPLAIN {query}").fetchall()
    
    print(f"\nEXPLAIN output for {query_file}:")
    print("=" * 80)
    for row in explain_result[0][1].split("\n"):
        print(row)
    
    con.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Execute EXPLAIN for a TPC-H query with a given source.",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-q", "--query",
        type=str,
        required=True,
        dest="query_file",
        metavar="QUERY",
        help="Path to query SQL file"
    )
    parser.add_argument(
        "-s", "--source",
        type=str,
        default="parquet",
        dest="source",
        metavar="SRC",
        help="Input source: parquet, csv, json, memory, filtered, projected (default: parquet)"
    )
    
    args = parser.parse_args()
    
    data_dir = "data/tpch-1"
    
    source_map = {
        "parquet": Source.PARQUET,
        "csv": Source.CSV,
        "json": Source.JSON,
        "memory": Source.MEMORY,
        "filtered": Source.FILTERED,
        "projected": Source.PROJECTED
    }
    
    if args.source not in source_map:
        print(f"Error: Invalid source '{args.source}'. Valid options: {', '.join(source_map.keys())}")
        exit(1)
    
    explain_query(data_dir, source_map[args.source], args.query_file)
