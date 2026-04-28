import json
import glob
import os
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
import numpy as np
from matplotlib.ticker import FuncFormatter
from matplotlib.patches import Patch

for font_file in glob.glob("resources/*.ttf"):
    font_manager.fontManager.addfont(font_file)
plt.rcParams["font.family"] = "Inter 18pt"

TEXT_COLOR = "black" # "#3A3A3C"

# --- Plot ---
plt.rcParams.update({
    "grid.linestyle": ":",
    "grid.alpha": 1.0,
    "grid.color": "black",
    "font.size": 12,
    "axes.spines.right": False,
    "axes.spines.left": False,
    "axes.edgecolor": "black",
    "text.color": TEXT_COLOR,
    "axes.labelcolor": TEXT_COLOR,
    "xtick.color": TEXT_COLOR,
    "ytick.color": TEXT_COLOR,
})

COLOR_QUERY = "#5E81AC"
COLOR_DECODE = "#BF616A"
COLOR_FILTERED = "#A3BE8C"

COLOR_CSV = "#D08770"
COLOR_JSON = "#B48EAD"

def _cpu_time_subplot(ax, df, benchmark):
    data = df.copy()
    data = data[data["threads"] == 1]

    if benchmark == "tpch":
        data["query_label"] = data["query"].str.extract(r'q(\d+)\.sql')[0].astype(int).apply(lambda x: f"Q{x}")
    else:
        data["query_label"] = data["query"].str.extract(r'(\d+)\.sql')[0].astype(int).apply(lambda x: f"Q{x}")

    pivot = data.pivot_table(index="query_label", columns="source", values="cpu_time_sec")
    pivot = pivot.loc[sorted(pivot.index, key=lambda s: int(s[1:]))]

    filtered_clamped = pivot["filtered"].clip(upper=pivot["memory"])
    pivot["filter_pct"] = ((pivot["memory"] - filtered_clamped) / pivot["parquet"]) * 100
    pivot["query_pct"] = (filtered_clamped / pivot["parquet"]) * 100
    pivot["decode_pct"] = ((pivot["parquet"] - pivot["memory"]) / pivot["parquet"]) * 100

    # When parquet cpu time < memory, decode_pct is negative; clamp both decode and filter to 0
    # so the remaining query bar fills the full 100%.
    parquet_lt_memory = pivot["decode_pct"] < 0
    pivot.loc[parquet_lt_memory, "decode_pct"] = 0
    pivot.loc[parquet_lt_memory, "filter_pct"] = 0
    # filter_pct can also go negative independently (filtered > memory due to noise); clamp it too.
    pivot.loc[pivot["filter_pct"] < 0, "filter_pct"] = 0

    # Exclude queries where parquet ≈ 0 (clamped rows) from averages — they produce huge query_pct outliers.
    valid = ~parquet_lt_memory

    ax.set_axisbelow(True)
    ax.grid(axis='y')
    ax.grid(axis='x', visible=False)
    ax.set_ylim(0, 100)
    xlim_multiplier = {"tpch": 1.6, "clickbench": 1.3, "tpcds": 2.2}
    ax.set_xlim(-0.4 * xlim_multiplier[benchmark], len(pivot.index) + 1.0 * xlim_multiplier[benchmark])
    ax.tick_params(axis='y', length=0)

    x = np.arange(len(pivot.index))
    width = 0.65

    ax.bar(x, pivot["decode_pct"], width, label="Parquet decoding", color=COLOR_DECODE, edgecolor="black", linewidth=0.5)
    ax.bar(x, pivot["filter_pct"], width, bottom=pivot["decode_pct"], label="Filtering", color=COLOR_FILTERED, edgecolor="black", linewidth=0.5)
    ax.bar(x, [100 for _ in range(len(pivot.index))], width, bottom=pivot["decode_pct"] + pivot["filter_pct"], label="Remaining query", color=COLOR_QUERY, edgecolor="black", linewidth=0.5)

    avg_query = 100.0 - pivot.loc[valid, "query_pct"].mean()
    avg_decode = pivot.loc[valid, "decode_pct"].mean()
    ax.axhline(y=avg_query, color=COLOR_FILTERED, linestyle="--", linewidth=1.5)
    ax.axhline(y=avg_decode, color=COLOR_DECODE, linestyle="--", linewidth=1.5)

    ax.text(len(pivot.index) - 0.5, avg_query + 3, f"{avg_query:.0f}%", ha="left", bbox=dict(facecolor="white", edgecolor="none", pad=1))
    ax.text(len(pivot.index) - 0.5, avg_decode - 10, f"{avg_decode:.0f}%", ha="left", bbox=dict(facecolor="white", edgecolor="none", pad=1))

    ax.set_xticks(x)
    if benchmark == "tpcds":
        labels = [label if i % 2 == 0 else "" for i, label in enumerate(pivot.index)]
    else:
        labels = pivot.index
    ax.set_xticklabels(labels, rotation=90)
    benchmark_labels = {"tpch": "TPC-H", "clickbench": "ClickBench", "tpcds": "TPC-DS"}
    ax.set_xlabel(f"{benchmark_labels.get(benchmark, benchmark)} query", fontweight="bold")


def plot_cpu_time(df_tpch, df_clickbench, df_tpcds):
    fig = plt.figure(figsize=(16, 6))
    gs = fig.add_gridspec(2, 3, hspace=0.45)

    ax_tpch = fig.add_subplot(gs[0, 0])
    ax_cb = fig.add_subplot(gs[0, 1:])
    ax_tpcds = fig.add_subplot(gs[1, :])

    _cpu_time_subplot(ax_tpch, df_tpch, "tpch")
    _cpu_time_subplot(ax_cb, df_clickbench, "clickbench")
    _cpu_time_subplot(ax_tpcds, df_tpcds, "tpcds")

    ax_tpch.set_ylabel("CPU time (%)", fontweight="bold")
    ax_tpcds.set_ylabel("CPU time (%)", fontweight="bold")
    ax_tpch.set_title("(a) TPC-H", fontsize=12)
    ax_cb.set_title("(b) ClickBench", fontsize=12)
    ax_tpcds.set_title("(c) TPC-DS", fontsize=12)

    handles, labels = ax_tpch.get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.98), ncol=3, frameon=False, prop={'weight': 'bold'}, columnspacing=1.2, handletextpad=0.3, handlelength=1.0)

    plt.savefig("plots/cpu_time_stacked.pdf", bbox_inches="tight")

def plot_appetizer(df_10, df_30):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(8, 3.25))
    
    for ax, data, sf, label, streams in [(ax1, df_10.copy(), 10, "a", 3), (ax2, df_30.copy(), 30, "b", 4)]:
        ax.set_axisbelow(True)
        ax.grid(axis='y')
        ax.grid(axis='x')
        ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x/1000:.0f}k" if x > 0 else "0"))
        ax.set_xlim(0, 65)
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
    fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 1.07), ncol=3, frameon=False, prop={'weight': 'bold'}, columnspacing=1.2, handletextpad=0.3, handlelength=1.0)
    
    plt.tight_layout()
    plt.savefig("plots/latency_by_threads.pdf", bbox_inches="tight")

def plot_csv_json(df_10, df_10_unsorted, df_10_sorted):
    _, (ax1, ax2) = plt.subplots(1, 2, figsize=(7, 3.75), gridspec_kw={'width_ratios': [0.4, 0.6]})

    for i, ax, df, title in [(0, ax1, df_10, "(a) CSV & JSON parsing"), (1, ax2, (df_10_unsorted, df_10_sorted), "(b) Parquet input ordering & row group pruning")]:
        ax.set_axisbelow(True)
        ax.grid(axis='y')
        ax.grid(axis='x')

        if i == 0:
            data = df.copy()

            ax.set_ylim(0, 4000)

            ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x/1000:.0f}k" if x > 0 else "0"))
            ax.set_xlim(0, 65)
            factor = 3 * 22 * 3600
            subset = data[(data["source"] == "csv") & (data["streams"] == 3)].sort_values("threads")
            ax.plot(subset["threads"], factor / subset["runtime_sec"], marker="s", color=COLOR_CSV, 
                    label="CSV files", markersize=2)
            
            subset = data[(data["source"] == "json") & (data["streams"] == 3)].sort_values("threads")
            ax.plot(subset["threads"], factor / subset["runtime_sec"], marker="o", color=COLOR_JSON, 
                    label="JSON files", markersize=2)
            
            ax.set_xlabel("Number of threads", fontweight="bold")
            ticks = sorted(data["threads"].unique())
            ticks = [t for t in ticks if t == 1 or t % 8 == 0]
            ax.set_xticks(ticks)
            ax.set_xticklabels(ticks)
            ax.set_ylabel("Queries / h", fontweight="bold")

            ax.legend(loc="upper center", bbox_to_anchor=(0.42, 1.225), ncol=3, frameon=False, prop={'weight': 'bold'}, columnspacing=1.2, handletextpad=0.3, handlelength=1.0)
        else:
            data_unsorted = df[0].copy()
            data_sorted = df[1].copy()
            ax.set_ylim(0, 100)
            ax.grid(axis='x', visible=False)
            
            data_unsorted = data_unsorted[(data_unsorted["source"] == "parquet") & (data_unsorted["threads"] == 1)]
            data_sorted = data_sorted[(data_sorted["source"] == "parquet") & (data_sorted["threads"] == 1)]

            data_unsorted["query_label"] = data_unsorted["query"].str.extract(r'q(\d+)\.sql')[0].astype(int).apply(lambda x: f"Q{x}")
            data_sorted["query_label"] = data_sorted["query"].str.extract(r'q(\d+)\.sql')[0].astype(int).apply(lambda x: f"Q{x}")
    
            keep = ["Q1", "Q2", "Q3", "Q6", "Q7", "Q10", "Q12", "Q14", "Q15", "Q16", "Q20"]

            data_unsorted = data_unsorted[data_unsorted["query_label"].isin(keep)].sort_values("query")
            data_sorted = data_sorted[data_sorted["query_label"].isin(keep)].sort_values("query")

            queries = data_unsorted.sort_values("query")["query_label"].values

            ax.set_xlim(-.5, len(queries) - .5)

            bar_width = 0.3

            for idx, (label, data) in enumerate([("U", data_unsorted), ("Sorted", data_sorted)]):
                data = data.sort_values("query")
                scan = data["operators"].apply(lambda o: o["scan"]).values
                filt = data["operators"].apply(lambda o: o["filter"]).values
                scan_filter = scan + filt
                rest = data["runtime_sec"].values - scan_filter

                if idx == 0:
                    # Compute max runtime per query across both datasets
                    sorted_tmp = data_sorted.sort_values("query")
                    max_runtime = np.maximum(data["runtime_sec"].values, sorted_tmp["runtime_sec"].values)
                # Normalize
                scan_filter = scan_filter / max_runtime * 100
                rest = rest / max_runtime * 100

                x = np.arange(len(queries))
                
                offset = idx * (bar_width + 0.075) - (bar_width + 0.075) / 2

                hatch = '////' if label == "Sorted" else None
                ax.bar(x + offset, scan_filter, bar_width, label=f"Scan", color=COLOR_DECODE, hatch=hatch, edgecolor="black", linewidth=0.5)
                ax.bar(x + offset, rest, bar_width, bottom=scan_filter, label=f"Rest", color=COLOR_QUERY, hatch=hatch, edgecolor="black", linewidth=0.5)

            ax.set_xlabel("TPC-H query", fontweight="bold")
            ax.set_xticks(np.arange(len(queries)))
            ax.set_xticklabels(queries, rotation=90)
            ax.set_ylabel("Relative runtime (%)", fontweight="bold")

            legend_handles = [
                Patch(facecolor=COLOR_DECODE, label='Scan'),
                Patch(facecolor=COLOR_QUERY, label='Rest'),
                Patch(facecolor='white', edgecolor="black", linewidth=1, label='Unsorted'),
                Patch(facecolor='white', edgecolor="black", linewidth=1, hatch='/////', label='Sorted'),
            ]
            ax.legend(handles=legend_handles, loc="upper center", bbox_to_anchor=(0.42, 1.225), ncol=4, frameon=False, prop={'weight': 'bold'}, columnspacing=1.2, handletextpad=0.3, handlelength=1.0)
        ax.set_title(title, fontsize=12, y=1.175, x=0.42)

    plt.tight_layout()
    plt.savefig("plots/csv_json.pdf", bbox_inches="tight")

def main():
    # Queries plots
    tpch_data = []
    tpcds_data = []

    clickbench_data = []

    for filepath in glob.glob(os.path.join("measurements", "queries", "tpch-30", "*.json")):
        with open(filepath, "r") as f:
            tpch_data.extend(json.load(f))
    for filepath in glob.glob(os.path.join("measurements", "queries", "tpcds-30", "*.json")):
        with open(filepath, "r") as f:
            tpcds_data.extend(json.load(f))
    for filepath in glob.glob(os.path.join("measurements", "queries", "clickbench", "*.json")):
        with open(filepath, "r") as f:
            clickbench_data.extend(json.load(f))

    tpch_df = pd.DataFrame(tpch_data)
    tpcds_df = pd.DataFrame(tpcds_data)
    clickbench_df = pd.DataFrame(clickbench_data)

    plot_cpu_time(tpch_df, clickbench_df, tpcds_df)

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
    unsorted_data_10 = []
    sorted_data_10 = []

    for filepath in glob.glob(os.path.join("measurements", "throughput", "tpch-other-10", "*.json")):
        with open(filepath, "r") as f:
            this_data = json.load(f) # each file contains a JSON object
            throughput_data_10.append(this_data)
    for filepath in glob.glob(os.path.join("measurements", "queries", "tpch-30-random", "*.json")):
        with open(filepath, "r") as f:
            this_data = json.load(f) # each file contains a JSON object
            unsorted_data_10.extend(this_data)
    for filepath in glob.glob(os.path.join("measurements", "queries", "tpch-30-sorted", "*.json")):
        with open(filepath, "r") as f:
            this_data = json.load(f) # each file contains a JSON object
            sorted_data_10.extend(this_data)
    
    throughput_df_10 = pd.DataFrame(throughput_data_10)
    unsorted_df_10 = pd.DataFrame(unsorted_data_10)
    sorted_df_10 = pd.DataFrame(sorted_data_10)

    plot_csv_json(throughput_df_10, unsorted_df_10, sorted_df_10)

if __name__ == "__main__":
    os.makedirs("plots", exist_ok=True)
    main()
