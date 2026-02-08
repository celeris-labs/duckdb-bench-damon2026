import json
import glob
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def plot_cpu_time(df):
    data = df.copy()
    data = data[data["threads"] == 1]

    # Extract short query names: q01.sql -> Q1, q02.sql -> Q2, etc.
    data["query_label"] = data["query"].str.extract(r'q(\d+)\.sql')[0].astype(int).apply(lambda x: f"Q{x}")

    # Pivot to get parquet and memory side by side
    pivot = data.pivot_table(index="query_label", columns="source", values="cpu_time_sec")
    pivot = pivot.loc[sorted(pivot.index, key=lambda s: int(s[1:]))]

    # The "memory" portion is the base (compute time on in-memory data)
    # The "parquet" overhead is the difference: parquet - memory (decoding cost)
    pivot["memory_pct"] = (pivot["memory"] / pivot["parquet"]) * 100
    pivot["overhead_pct"] = ((pivot["parquet"] - pivot["memory"]) / pivot["parquet"]) * 100


    # --- Plot ---
    plt.rcParams.update({
        "grid.linestyle": "--",
        "grid.alpha": 1.0,
        "font.size": 12
    })

    COLOR_QUERY = "#4878d0"
    COLOR_DECODE = "#d65f5f"

    _, ax = plt.subplots(figsize=(8, 5))

    ax.set_axisbelow(True)
    ax.grid(axis='y')
    ax.grid(axis='x', visible=False)
    ax.set_ylim(0, 100)

    x = np.arange(len(pivot.index))
    width = 0.5

    # Bottom stack: memory (compute) time
    ax.bar(x, pivot["memory_pct"], width, label="Query", color=COLOR_QUERY)

    # Top stack: parquet overhead (decode)
    ax.bar(x, pivot["overhead_pct"], width, bottom=pivot["memory_pct"], label="Decode", color=COLOR_DECODE)

    ax.set_xticks(x)
    ax.set_xticklabels(pivot.index, rotation=90)
    ax.set_xlabel("TPC-H Query")
    ax.set_ylabel("CPU Time (sec)")
    ax.legend(loc="lower right", framealpha=1.0)

    plt.tight_layout()
    plt.savefig("cpu_time_stacked.pdf", bbox_inches="tight")

def plot_appetizer(df):
    data = df.copy()
    agg = data[data["query"].str.contains("q21.sql")]

    plt.rcParams.update({
        "grid.linestyle": "--",
        "grid.alpha": 1.0,
        "font.size": 12
    })

    COLOR_MEMORY = "#4878d0"
    COLOR_PARQUET = "#d65f5f"

    _, ax = plt.subplots(figsize=(8, 5))
    ax.set_axisbelow(True)
    ax.grid(axis='y')
    ax.grid(axis='x', visible=False)

    for source, color, marker in [("memory", COLOR_MEMORY, "o"), ("parquet", COLOR_PARQUET, "s")]:
        subset = agg[agg["source"] == source].sort_values("threads")
        ax.plot(subset["threads"], subset["latency_sec"],
                marker=marker, color=color, label=source.capitalize(), linewidth=2)

    ax.set_xlabel("Number of Threads")
    ax.set_ylabel("Latency (sec)")
    ax.set_xticks(sorted(df["threads"].unique()))
    ax.legend(loc="upper right", framealpha=1.0)
    plt.tight_layout()
    plt.savefig("latency_by_threads.pdf", bbox_inches="tight")

def main():
    data = []

    for filepath in glob.glob(os.path.join("measurements", "run-*.json")):
        with open(filepath, "r") as f:
            this_data = json.load(f) # each file contains a JSON array of objects
            data.extend(this_data)

    df = pd.DataFrame(data)

    plot_cpu_time(df)
    plot_appetizer(df)

if __name__ == "__main__":
    main()
