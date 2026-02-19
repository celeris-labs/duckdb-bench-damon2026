import json
import glob
import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import font_manager
import numpy as np
from matplotlib.ticker import FuncFormatter

for font_file in glob.glob("resources/*.ttf"):
    font_manager.fontManager.addfont(font_file)
plt.rcParams["font.family"] = "Inter 18pt"

# --- Plot ---
plt.rcParams.update({
    "grid.linestyle": "-",
    "grid.alpha": 1.0,
    "grid.color": "#E5E5EA",
    "font.size": 12,
    "axes.spines.right": False,
    "axes.spines.left": False,
    "axes.edgecolor": "#E5E5EA",
    "text.color": "#3A3A3C",
    "axes.labelcolor": "#3A3A3C",
    "xtick.color": "#3A3A3C",
    "ytick.color": "#3A3A3C",
})

COLOR_QUERY = "#5E81AC"
COLOR_DECODE = "#BF616A"
COLOR_FILTERED = "#A3BE8C"

COLOR_CSV = "#D08770"
COLOR_JSON = "#B48EAD"

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
    pivot["filter_pct"] = ((pivot["memory"] - pivot["filtered"]) / pivot["parquet"]) * 100
    pivot["decode_pct"] = ((pivot["parquet"] - pivot["memory"]) / pivot["parquet"]) * 100
    pivot["query_pct"] = (pivot["filtered"] / pivot["parquet"]) * 100

    _, ax = plt.subplots(figsize=(8, 3.5))

    ax.set_axisbelow(True)
    ax.grid(axis='y')
    ax.grid(axis='x', visible=False)
    ax.set_ylim(0, 100)
    ax.set_xlim(-0.8, len(pivot.index) + 1.0)
    ax.tick_params(axis='both', length=0)

    x = np.arange(len(pivot.index))
    width = 0.5

    ax.bar(x, [100 for _ in range(len(pivot.index))], width, label="Remaining query", color=COLOR_QUERY)
    ax.bar(x, pivot["filter_pct"], width, bottom=pivot["decode_pct"], label="Filtering", color=COLOR_FILTERED)
    ax.bar(x, pivot["decode_pct"], width, label="Parquet decoding", color=COLOR_DECODE)

    avg_query = 100.0 - pivot["query_pct"].mean()
    avg_decode = pivot["decode_pct"].mean()
    ax.axhline(y=avg_query, color=COLOR_FILTERED, linestyle="--", linewidth=1.5)
    ax.axhline(y=avg_decode, color=COLOR_DECODE, linestyle="--", linewidth=1.5)

    ax.text(len(pivot.index) - 0.5, avg_query + 1.5, f"{avg_query:.0f}%", ha="left")
    ax.text(len(pivot.index) - 0.5, avg_decode + 1.5, f"{avg_decode:.0f}%", ha="left")

    ax.set_xticks(x)
    ax.set_xticklabels(pivot.index, rotation=90)
    ax.set_xlabel("TPC-H Query", fontweight="bold")
    ax.set_ylabel("CPU Time (%)", fontweight="bold")
    ax.legend(loc="lower center", bbox_to_anchor=(0.5, 1.0), ncol=3, frameon=False, prop={'weight': 'bold'})

    plt.tight_layout()
    plt.savefig("plots/cpu_time_stacked.pdf", bbox_inches="tight")

def plot_appetizer(df_10, df_30):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(8, 3.5))
    
    for ax, data, sf, label, streams in [(ax1, df_10.copy(), 10, "a", 3), (ax2, df_30.copy(), 30, "b", 4)]:
        ax.set_axisbelow(True)
        ax.grid(axis='y')
        ax.grid(axis='x')
        ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x/1000:.0f}k" if x > 0 else "0"))
        ax.set_xlim(0, 65)
        ax.tick_params(axis='both', length=0)
        
        factor = streams * 22 * 3600
        subset = data[(data["source"] == "parquet") & (data["streams"] == streams)].sort_values("threads")
        ax.plot(subset["threads"], factor / subset["runtime_sec"], marker="s", color=COLOR_DECODE, 
                label="Parquet files", markersize=2)
        
        subset = data[(data["source"] == "memory") & (data["streams"] == streams)].sort_values("threads")
        ax.plot(subset["threads"], factor / subset["runtime_sec"], marker="o", color=COLOR_FILTERED, 
                label="Tables", markersize=2)
        
        subset = data[(data["source"] == "filtered") & (data["streams"] == streams)].sort_values("threads")
        ax.plot(subset["threads"], factor / subset["runtime_sec"], marker="^", color=COLOR_QUERY, 
                label="Pre-filtered tables", markersize=2)
        
        memory_val = factor / data[(data["source"] == "filtered") & (data["threads"] == 16) & (data["streams"] == streams)]["runtime_sec"].values[0]
        ax.axhline(y=memory_val, color=COLOR_QUERY, linestyle="--")
        ax.text(subset["threads"].values[-1] * 0.8, memory_val + (ax.get_ylim()[1] - ax.get_ylim()[0]) * 0.02, f"{memory_val:.0f} Q/h", ha="left",
                fontsize=10)

        if sf == 10:
            ax.annotate("16 threads",
                xy=(16, memory_val),
                xytext=(10, memory_val + (ax.get_ylim()[1] - ax.get_ylim()[0]) * 0.15),
                arrowprops=dict(arrowstyle="->", color="#3A3A3C", lw=1.0),
                fontsize=10, ha="center",
            )
        
        ax.set_xlabel("Number of threads", fontweight="bold")
        ticks = sorted(data["threads"].unique())
        ticks = [t for t in ticks if t == 1 or t % 8 == 0]
        ax.set_xticks(ticks)
        ax.set_xticklabels(ticks)
        ax.set_title(f"({label}) TPC-H scale factor {sf}", fontsize=12, y=-0.35)
    
    ax1.set_ylabel("Queries / h", fontweight="bold")

    # Single shared legend on top
    handles, labels = ax1.get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 1.08), ncol=3, frameon=False, prop={'weight': 'bold'})
    
    plt.tight_layout()
    plt.savefig("plots/latency_by_threads.pdf", bbox_inches="tight")

def plot_csv_json(df_10):
    _, ax = plt.subplots(figsize=(8, 3.5))

    data = df_10.copy()

    ax.set_axisbelow(True)
    ax.grid(axis='y')
    ax.grid(axis='x')
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x/1000:.0f}k" if x > 0 else "0"))
    ax.set_xlim(0, 65)
    ax.tick_params(axis='both', length=0)
    
    factor = 3 * 22 * 3600
    subset = data[(data["source"] == "csv") & (data["streams"] == 3)].sort_values("threads")
    ax.plot(subset["threads"], factor / subset["runtime_sec"], marker="s", color=COLOR_CSV, 
            label="CSV files", markersize=4)
    
    subset = data[(data["source"] == "json") & (data["streams"] == 3)].sort_values("threads")
    ax.plot(subset["threads"], factor / subset["runtime_sec"], marker="o", color=COLOR_JSON, 
            label="JSON files", markersize=4)
    
    ax.set_xlabel("Number of threads", fontweight="bold")
    ticks = sorted(data["threads"].unique())
    ticks = [t for t in ticks if t == 1 or t % 8 == 0]
    ax.set_xticks(ticks)
    ax.set_xticklabels(ticks)
    ax.set_ylabel("Queries / h", fontweight="bold")

    # Single shared legend on top
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, 1.2), ncol=3, frameon=False, prop={'weight': 'bold'})
    
    plt.tight_layout()
    plt.savefig("plots/csv_json.pdf", bbox_inches="tight")

def main():
    # Queries plots
    queries_data = []

    for filepath in glob.glob(os.path.join("measurements", "queries", "tpch-10", "*.json")):
        with open(filepath, "r") as f:
            this_data = json.load(f) # each file contains a JSON array of objects
            queries_data.extend(this_data)

    queries_df = pd.DataFrame(queries_data)

    plot_cpu_time(queries_df)

    # Throughput plots
    throughput_data_10 = []
    throughput_data_30 = []

    for filepath in glob.glob(os.path.join("measurements", "throughput", "tpch-10", "*.json")):
        with open(filepath, "r") as f:
            this_data = json.load(f) # each file contains a JSON object
            throughput_data_10.append(this_data)
    for filepath in glob.glob(os.path.join("measurements", "throughput", "tpch-30", "*.json")):
        with open(filepath, "r") as f:
            this_data = json.load(f) # each file contains a JSON object
            throughput_data_30.append(this_data)
    
    throughput_df_10 = pd.DataFrame(throughput_data_10)
    throughput_df_30 = pd.DataFrame(throughput_data_30)

    plot_appetizer(throughput_df_10, throughput_df_30)

    # CSV and JSON plot
    throughput_data_10 = []

    for filepath in glob.glob(os.path.join("measurements", "throughput", "other-10", "*.json")):
        with open(filepath, "r") as f:
            this_data = json.load(f) # each file contains a JSON object
            throughput_data_10.append(this_data)
    
    throughput_df_10 = pd.DataFrame(throughput_data_10)

    plot_csv_json(throughput_df_10)

if __name__ == "__main__":
    os.makedirs("plots", exist_ok=True)
    main()
