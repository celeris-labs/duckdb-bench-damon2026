import duckdb
from pathlib import Path
import argparse

def generate_tpch_parquet(output_dir: str = "data/tpch", format: str = "parquet", scale_factor: int = 1, ordering: str = "default"):
    """
    Generate TPC-H data and write each table to a Parquet file.
    
    Args:
        output_dir: Directory to write Parquet files
        scale_factor: TPC-H scale factor (default 1)
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    con = duckdb.connect(database=':memory:')
    
    # Install and load TPC-H extension
    con.execute("INSTALL tpch")
    con.execute("LOAD tpch")
    
    # Generate TPC-H data
    print(f"Generating TPC-H SF{scale_factor} data...")
    con.execute(f"CALL dbgen(sf={scale_factor})")
    
    # TPC-H tables
    tables = [
        "nation",
        "region", 
        "part",
        "supplier",
        "partsupp",
        "customer",
        "orders",
        "lineitem"
    ]

    copy_options = {
        "parquet": "FORMAT PARQUET, COMPRESSION SNAPPY",
        "csv": "FORMAT CSV",
        "json": "FORMAT JSON"
    }

    sort_column = {
        "lineitem": "l_shipdate",
        "orders": "o_orderdate"
    }
    
    # Export each table to Parquet
    for table in tables:
        filename = output_path / f"{table}.{format}"
        print(f"Writing {table} to {filename}...")
        if ordering == "sorted" and table in sort_column:
            con.execute(f"COPY (SELECT * FROM {table} ORDER BY {sort_column[table]}) TO '{filename}' ({copy_options[format]})")
        elif ordering == "random":
            con.execute(f"COPY (SELECT * FROM {table} ORDER BY random()) TO '{filename}' ({copy_options[format]})")
        else:
            con.execute(f"COPY {table} TO '{filename}' ({copy_options[format]})")
        
        # Print row count for verification
        row_count = con.execute(f"SELECT COUNT(*) FROM '{filename}'").fetchone()[0]
        print(f"  -> {row_count:,} rows")
    
    con.close()
    print(f"\nDone! Files written to {output_path.absolute()}")

def main():
    parser = argparse.ArgumentParser(
        description="Generate TPC-H data and export to Parquet files using DuckDB.",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-s", "--scale-factor",
        type=float,
        default=1,
        dest="scale_factor",
        metavar="SF",
        help="TPC-H scale factor (default: 1)"
    )
    parser.add_argument(
        "-f", "--format",
        type=str,
        default="parquet",
        dest="format",
        metavar="FORMAT",
        help="Output format (default: parquet)"
    )
    parser.add_argument(
        "-o", "--output",
        type=str,
        default="data/tpch-1",
        dest="output_dir",
        metavar="DIR",
        help="Output directory (default: data/tpch-1)"
    )
    parser.add_argument(
        "-O", "--ordering",
        type=str,
        default="default",
        dest="ordering",
        help="Ordering of the tables. Use 'sorted' to sort lineitem by l_shipdate and orders by o_orderdate or 'random' for a random permutation. (default: 'default')"
    )
    
    args = parser.parse_args()
    generate_tpch_parquet(scale_factor=args.scale_factor, format=args.format, output_dir=args.output_dir, ordering=args.ordering)

if __name__ == "__main__":
    main()
