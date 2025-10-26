#!/usr/bin/env python3
"""
plot_metrics.py

Reads a metrics CSV (from your consolidator), computes summaries and plots:
- counts/time-series
- bytes/time-series
- avg & max latencies (snap/trade)
- rolling percentiles for a selected latency metric
- scatter of throughput vs latency
- correlation heatmap

Usage:
    python3 plot_metrics.py --metrics metrics.csv --outdir plots_metrics --rolling-sec 60
"""
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import textwrap
from datetime import datetime

def iso_now():
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

def ensure_outdir(p: Path):
    p.mkdir(parents=True, exist_ok=True)
    return p

def safe_col_find(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def to_numeric_series(df, col):
    return pd.to_numeric(df[col], errors="coerce")

def pctile_summary(arr):
    arr = np.asarray(arr.dropna())
    if arr.size == 0:
        return {}
    return {
        "count": int(arr.size),
        "mean": float(np.mean(arr)),
        "median": float(np.median(arr)),
        "p90": float(np.percentile(arr,90)),
        "p95": float(np.percentile(arr,95)),
        "p99": float(np.percentile(arr,99)),
        "max": float(np.max(arr)),
        "min": float(np.min(arr))
    }

def plot_timeseries(df_time, cols, outpath, title, ylabel=None):
    plt.figure(figsize=(12,4))
    for c,label in cols:
        if c in df_time:
            plt.plot(df_time.index, df_time[c], label=label, linewidth=0.9)
    plt.title(title)
    if ylabel:
        plt.ylabel(ylabel)
    plt.legend()
    plt.grid(alpha=0.2)
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()

def plot_scatter(x, y, outpath, title, xlabel=None, ylabel=None):
    plt.figure(figsize=(6,5))
    plt.scatter(x, y, s=6, alpha=0.5)
    if xlabel: plt.xlabel(xlabel)
    if ylabel: plt.ylabel(ylabel)
    plt.title(title)
    plt.grid(alpha=0.2)
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()

def plot_heatmap(corr, outpath, title):
    plt.figure(figsize=(8,6))
    import matplotlib
    cmap = matplotlib.cm.get_cmap("RdBu_r")
    plt.imshow(corr, interpolation='nearest', cmap=cmap, aspect='auto', vmin=-1, vmax=1)
    plt.colorbar(label='corr')
    ticks = list(range(len(corr.columns)))
    plt.xticks(ticks, corr.columns, rotation=90)
    plt.yticks(ticks, corr.columns)
    plt.title(title)
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--metrics", required=True, help="metrics CSV path")
    p.add_argument("--outdir", default="plots_metrics", help="output directory for plots")
    p.add_argument("--rolling-sec", type=int, default=60, help="rolling window seconds for percentiles")
    p.add_argument("--ts-col", default=None, help="timestamp column name (if specified, will be parsed as epoch seconds)")
    args = p.parse_args()

    metrics_path = Path(args.metrics)
    outdir = ensure_outdir(Path(args.outdir))

    if not metrics_path.exists():
        print("Metrics file not found:", metrics_path)
        return

    df = pd.read_csv(metrics_path, low_memory=False)
    # normalize column names
    df.columns = [c.strip() for c in df.columns]

    # Parse timestamp column if present
    ts_col = args.ts_col
    if not ts_col:
        # heuristics
        for cand in ("ts","time","timestamp"):
            if cand in df.columns:
                ts_col = cand
                break
    if ts_col and ts_col in df.columns:
        try:
            # assume epoch seconds or numeric; if very large assume ms
            if pd.api.types.is_numeric_dtype(df[ts_col]):
                vals = pd.to_numeric(df[ts_col], errors='coerce')
                if vals.max() > 1e12:
                    # ms -> convert to seconds for index
                    idx = pd.to_datetime(vals, unit='ms', utc=True)
                elif vals.max() > 1e9:
                    idx = pd.to_datetime(vals, unit='s', utc=True)
                else:
                    # fallback parse
                    idx = pd.to_datetime(vals, unit='s', utc=True)
            else:
                idx = pd.to_datetime(df[ts_col], errors='coerce', utc=True)
            df.index = idx
        except Exception:
            # fallback numeric index
            df.index = pd.RangeIndex(start=0, stop=len(df), step=1)
    else:
        df.index = pd.RangeIndex(start=0, stop=len(df), step=1)

    # Common metrics candidate names
    snap_count_col = safe_col_find(df, ["snap_count","snap_counts","snapshot_count"])
    snap_bytes_col = safe_col_find(df, ["snap_bytes","snapshot_bytes"])
    snap_avg_ex_col = safe_col_find(df, ["snap_avg_ex_recv_ms","snap_avg_ex_ms","snapshot_avg_ex_recv_ms"])
    snap_avg_recvwrite_col = safe_col_find(df, ["snap_avg_recv_write_ms","snap_avg_recv_ms"])
    snap_max_ex_col = safe_col_find(df, ["snap_max_ex_recv_ms","snap_max_ex_ms"])
    snap_max_recvwrite_col = safe_col_find(df, ["snap_max_recv_write_ms","snap_max_recv_ms"])

    trade_count_col = safe_col_find(df, ["trade_count","trade_counts"])
    trade_bytes_col = safe_col_find(df, ["trade_bytes"])
    trade_avg_ex_col = safe_col_find(df, ["trade_avg_ex_recv_ms","trade_avg_ex_ms"])
    trade_avg_recvwrite_col = safe_col_find(df, ["trade_avg_recv_write_ms","trade_avg_recv_ms"])
    trade_max_ex_col = safe_col_find(df, ["trade_max_ex_recv_ms","trade_max_ex_ms"])
    trade_max_recvwrite_col = safe_col_find(df, ["trade_max_recv_write_ms","trade_max_recv_ms"])

    numeric_cols = []
    for c in [snap_count_col, snap_bytes_col, snap_avg_ex_col, snap_avg_recvwrite_col, snap_max_ex_col, snap_max_recvwrite_col,
              trade_count_col, trade_bytes_col, trade_avg_ex_col, trade_avg_recvwrite_col, trade_max_ex_col, trade_max_recvwrite_col]:
        if c and c not in numeric_cols:
            numeric_cols.append(c)

    # Convert to numeric
    for c in numeric_cols:
        df[c] = pd.to_numeric(df.get(c), errors='coerce')

    # Basic summary statistics
    summary_lines = []
    summary_lines.append(f"metrics file: {metrics_path}")
    summary_lines.append(f"rows: {len(df)}")
    summary_lines.append(f"index range: {df.index.min()} -> {df.index.max()}")
    summary_lines.append("\nColumn presence:")
    for c in numeric_cols:
        summary_lines.append(f" - {c}: present, non-null={int(df[c].notna().sum())}")

    # Per-column percentile summary
    summary_lines.append("\nPercentile summary (mean/median/p90/p95/p99/max):")
    for c in numeric_cols:
        arr = df[c].dropna()
        if len(arr) == 0:
            summary_lines.append(f" - {c}: no data")
        else:
            s = pctile_summary(arr)
            summary_lines.append(f" - {c}: count={s['count']} mean={s['mean']:.3f} median={s['median']:.3f} p90={s['p90']:.3f} p95={s['p95']:.3f} p99={s['p99']:.3f} max={s['max']:.3f}")

    # Save summary text
    summary_txt = outdir / "metrics_summary.txt"
    with open(summary_txt, "w") as fh:
        fh.write("\n".join(summary_lines))
    print(f"[{iso_now()}] wrote summary to {summary_txt}")

    # Create time-series plots
    # Counts
    cols_counts = []
    if snap_count_col: cols_counts.append((snap_count_col, "snap_count"))
    if trade_count_col: cols_counts.append((trade_count_col, "trade_count"))
    if cols_counts:
        plot_timeseries(df, cols_counts, outdir / "counts_timeseries.png", "Counts per interval", ylabel="count")
        print(f"[{iso_now()}] wrote counts_timeseries.png")

    # Bytes
    cols_bytes = []
    if snap_bytes_col: cols_bytes.append((snap_bytes_col, "snap_bytes"))
    if trade_bytes_col: cols_bytes.append((trade_bytes_col, "trade_bytes"))
    if cols_bytes:
        plot_timeseries(df, cols_bytes, outdir / "bytes_timeseries.png", "Bytes per interval", ylabel="bytes")
        print(f"[{iso_now()}] wrote bytes_timeseries.png")

    # Avg latencies
    cols_avg = []
    if snap_avg_ex_col: cols_avg.append((snap_avg_ex_col, "snap_avg_ex_recv_ms"))
    if snap_avg_recvwrite_col: cols_avg.append((snap_avg_recvwrite_col, "snap_avg_recv_write_ms"))
    if trade_avg_ex_col: cols_avg.append((trade_avg_ex_col, "trade_avg_ex_recv_ms"))
    if trade_avg_recvwrite_col: cols_avg.append((trade_avg_recvwrite_col, "trade_avg_recv_write_ms"))
    if cols_avg:
        plot_timeseries(df, cols_avg, outdir / "latency_avg_timeseries.png", "Average latencies per interval", ylabel="ms")
        print(f"[{iso_now()}] wrote latency_avg_timeseries.png")

    # Max latencies
    cols_max = []
    if snap_max_ex_col: cols_max.append((snap_max_ex_col, "snap_max_ex_recv_ms"))
    if snap_max_recvwrite_col: cols_max.append((snap_max_recvwrite_col, "snap_max_recv_write_ms"))
    if trade_max_ex_col: cols_max.append((trade_max_ex_col, "trade_max_ex_recv_ms"))
    if trade_max_recvwrite_col: cols_max.append((trade_max_recvwrite_col, "trade_max_recv_write_ms"))
    if cols_max:
        plot_timeseries(df, cols_max, outdir / "latency_max_timeseries.png", "Max latencies per interval", ylabel="ms")
        print(f"[{iso_now()}] wrote latency_max_timeseries.png")

    # Rolling percentiles (for a selected latency metric, default: trade_avg_recv_write_ms)
    chosen_latency = trade_avg_recvwrite_col or trade_avg_ex_col or snap_avg_recvwrite_col or snap_avg_ex_col
    if chosen_latency:
        try:
            window = f"{int(args.rolling_sec)}s"
            # resample to 1s if index is datetime-like
            temp = df[[chosen_latency]].dropna()
            # convert to series indexed by datetime if available
            if isinstance(df.index, pd.DatetimeIndex):
                series = temp[chosen_latency].astype(float)
                # compute rolling percentiles via rolling apply (slow but ok for modest data)
                roll_median = series.rolling(window=window).median()
                roll_p90 = series.rolling(window=window).quantile(0.90)
                roll_p95 = series.rolling(window=window).quantile(0.95)
                roll_p99 = series.rolling(window=window).quantile(0.99)
                plt.figure(figsize=(12,4))
                plt.plot(roll_median.index, roll_median.values, label="p50")
                plt.plot(roll_p90.index, roll_p90.values, label="p90")
                plt.plot(roll_p95.index, roll_p95.values, label="p95")
                plt.plot(roll_p99.index, roll_p99.values, label="p99")
                plt.title(f"Rolling percentiles ({args.rolling_sec}s) for {chosen_latency}")
                plt.legend()
                plt.grid(alpha=0.2)
                plt.tight_layout()
                plt.savefig(outdir / "rolling_percentiles.png")
                plt.close()
                print(f"[{iso_now()}] wrote rolling_percentiles.png")
        except Exception as e:
            print("rolling percentile plot failed:", e)

    # scatter trade_count vs trade_avg_recv_write_ms
    if trade_count_col and trade_avg_recvwrite_col:
        plot_scatter(df[trade_count_col], df[trade_avg_recvwrite_col],
                     outdir / "count_vs_latency_scatter.png",
                     "trade_count vs trade_avg_recv_write_ms",
                     xlabel="trade_count", ylabel="trade_avg_recv_write_ms (ms)")
        print(f"[{iso_now()}] wrote count_vs_latency_scatter.png")

    # correlation heatmap of numeric columns
    if numeric_cols and len(numeric_cols) >= 2:
        corr_df = df[numeric_cols].corr().fillna(0)
        plot_heatmap(corr_df, outdir / "corr_heatmap.png", "Correlation heatmap of metrics")
        print(f"[{iso_now()}] wrote corr_heatmap.png")

    print(f"[{iso_now()}] finished. plots saved to {outdir}")

if __name__ == "__main__":
    main()
