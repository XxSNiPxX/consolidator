#!/usr/bin/env python3
"""
plot_metrics.py

Reads a metrics CSV (headers expected: ts_iso,kind,exchange,asset,recv,parse,enqueue,writer,persist,bytes)
and produces a set of diagnostic plots:

- counts/time-series (per-second intervals)
- bytes/time-series (per-second intervals)
- avg & max hop latencies per-interval
- rolling percentiles for a chosen latency metric
- scatter of throughput (rows/sec or bytes/sec) vs latency
- correlation heatmap of numeric metrics

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
from datetime import datetime, timezone

def iso_now():
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

def ensure_outdir(p: Path):
    p.mkdir(parents=True, exist_ok=True)
    return p

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

def plot_timeseries(index, series_dict, outpath, title, ylabel=None):
    plt.figure(figsize=(12,4))
    for label, series in series_dict.items():
        plt.plot(index, series, label=label, linewidth=0.9)
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
    plt.scatter(x, y, s=8, alpha=0.5)
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
    p.add_argument("--resample-sec", type=int, default=1, help="resample aggregation interval in seconds")
    p.add_argument("--chosen-latency", default="recv_to_writer_ms",
                   help="which latency metric to compute rolling percentiles for (see computed metrics)")
    args = p.parse_args()

    metrics_path = Path(args.metrics)
    outdir = ensure_outdir(Path(args.outdir))

    if not metrics_path.exists():
        print("Metrics file not found:", metrics_path)
        return

    # read CSV
    df = pd.read_csv(metrics_path, low_memory=False)
    df.columns = [c.strip() for c in df.columns]

    # required columns check
    required = {"ts_iso","kind","exchange","asset","recv","parse","enqueue","writer","persist","bytes"}
    missing = required - set(df.columns)
    if missing:
        print("WARNING: metrics file missing columns:", missing)
        # continue but try to proceed with what we have

    # parse ts_iso into datetime index (if present)
    if "ts_iso" in df.columns:
        try:
            df['ts_dt'] = pd.to_datetime(df['ts_iso'], errors='coerce', utc=True)
            # fallback: if many nulls, try numeric epoch
            if df['ts_dt'].isna().mean() > 0.2:
                # try numeric fallback on 'recv' as ms
                if 'recv' in df.columns:
                    df['ts_dt'] = pd.to_datetime(df['recv'], unit='ms', errors='coerce', utc=True)
        except Exception:
            df['ts_dt'] = pd.to_datetime(df['ts_iso'], errors='coerce', utc=True)
        df = df.set_index('ts_dt').sort_index()
    else:
        # fallback to numeric index
        df.index = pd.RangeIndex(start=0, stop=len(df), step=1)

    # convert numeric columns (coerce errors)
    for col in ("recv","parse","enqueue","writer","persist","bytes"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Compute hop latencies (ms)
    # parse_recv = parse - recv
    # enqueue_parse = enqueue - parse
    # writer_enqueue = writer - enqueue
    # persist_writer = persist - writer
    # recv_to_writer = writer - recv
    # recv_to_persist = persist - recv
    df['recv_to_parse_ms'] = (df['parse'] - df['recv']).astype('float64')
    df['parse_to_enqueue_ms'] = (df['enqueue'] - df['parse']).astype('float64')
    df['enqueue_to_writer_ms'] = (df['writer'] - df['enqueue']).astype('float64')
    df['writer_to_persist_ms'] = (df['persist'] - df['writer']).astype('float64')
    df['recv_to_writer_ms'] = (df['writer'] - df['recv']).astype('float64')
    df['recv_to_persist_ms'] = (df['persist'] - df['recv']).astype('float64')

    # per-row success marker
    df['valid_recv'] = df['recv'].notna()
    df['valid_writer'] = df['writer'].notna()
    df['valid_persist'] = df['persist'].notna()

    # Resample/aggregate per-second (or args.resample_sec) to create time-series
    resample_rule = f"{int(args.resample_sec)}S"
    # counts per interval
    counts = df.groupby(['kind']).resample(resample_rule).size().unstack(level=0).fillna(0)
    # but above grouping yields MultiIndex; easier: resample overall and per-kind separately
    overall_counts = df.resample(resample_rule).size().rename("rows")
    counts_by_kind = {}
    for k in sorted(df['kind'].dropna().unique()):
        try:
            k_int = int(k)
        except:
            k_int = k
        counts_by_kind[f"kind_{k_int}"] = df[df['kind']==k].resample(resample_rule).size()

    # bytes per interval
    bytes_by_interval = df['bytes'].resample(resample_rule).sum().fillna(0)

    # latencies aggregated per interval (mean & max)
    def aggr_mean_max(col):
        mean_s = df[col].resample(resample_rule).mean()
        max_s = df[col].resample(resample_rule).max()
        return mean_s, max_s

    mean_recv_to_writer, max_recv_to_writer = aggr_mean_max('recv_to_writer_ms')
    mean_recv_to_persist, max_recv_to_persist = aggr_mean_max('recv_to_persist_ms')
    mean_writer_to_persist, max_writer_to_persist = aggr_mean_max('writer_to_persist_ms')

    # plots: counts
    series_counts = {}
    series_counts['total_rows'] = overall_counts
    for k,label_series in counts_by_kind.items():
        series_counts[k] = label_series

    plot_timeseries(overall_counts.index, series_counts, outdir / "counts_timeseries.png", "Rows per interval", ylabel="rows")
    print(f"[{iso_now()}] wrote counts_timeseries.png")

    # plot bytes/time
    plot_timeseries(bytes_by_interval.index, {'bytes': bytes_by_interval}, outdir / "bytes_timeseries.png", "Bytes per interval", ylabel="bytes")
    print(f"[{iso_now()}] wrote bytes_timeseries.png")

    # plot average & max latencies
    lat_mean_series = {
        "recv_to_writer_mean": mean_recv_to_writer,
        "recv_to_persist_mean": mean_recv_to_persist,
        "writer_to_persist_mean": mean_writer_to_persist
    }
    plot_timeseries(mean_recv_to_writer.index, lat_mean_series, outdir / "latency_mean_timeseries.png", "Mean latencies per interval", ylabel="ms")
    print(f"[{iso_now()}] wrote latency_mean_timeseries.png")

    lat_max_series = {
        "recv_to_writer_max": max_recv_to_writer,
        "recv_to_persist_max": max_recv_to_persist,
        "writer_to_persist_max": max_writer_to_persist
    }
    plot_timeseries(max_recv_to_writer.index, lat_max_series, outdir / "latency_max_timeseries.png", "Max latencies per interval", ylabel="ms")
    print(f"[{iso_now()}] wrote latency_max_timeseries.png")

    # Choose latency metric for rolling percentiles
    chosen = args.chosen_latency
    # allow several common names
    if chosen not in df.columns:
        # fallback sequence
        for cand in ['recv_to_persist_ms','recv_to_writer_ms','writer_to_persist_ms','recv_to_parse_ms']:
            if cand in df.columns:
                chosen = cand
                break
    if chosen in df.columns and isinstance(df.index, pd.DatetimeIndex):
        try:
            window_secs = int(args.rolling_sec)
            window = f"{window_secs}s"
            series = df[chosen].dropna()
            # resample to 1s and forward-fill missing to make rolling stable
            series_s = series.resample('1S').mean().interpolate(limit_direction='both')
            roll_median = series_s.rolling(window=f"{window_secs}s").median()
            roll_p90 = series_s.rolling(window=f"{window_secs}s").quantile(0.90)
            roll_p95 = series_s.rolling(window=f"{window_secs}s").quantile(0.95)
            roll_p99 = series_s.rolling(window=f"{window_secs}s").quantile(0.99)
            plt.figure(figsize=(12,4))
            plt.plot(roll_median.index, roll_median.values, label="p50")
            plt.plot(roll_p90.index, roll_p90.values, label="p90")
            plt.plot(roll_p95.index, roll_p95.values, label="p95")
            plt.plot(roll_p99.index, roll_p99.values, label="p99")
            plt.title(f"Rolling percentiles ({window_secs}s) for {chosen}")
            plt.legend()
            plt.grid(alpha=0.2)
            plt.tight_layout()
            plt.savefig(outdir / "rolling_percentiles.png")
            plt.close()
            print(f"[{iso_now()}] wrote rolling_percentiles.png")
        except Exception as e:
            print("rolling percentile plot failed:", e)

    # scatter: throughput (rows/sec) vs latency (mean recv_to_writer)
    # use aligned index
    try:
        thru = overall_counts.resample(resample_rule).sum() / int(args.resample_sec)  # rows per second approximate
        lat = mean_recv_to_writer.reindex(thru.index)
        valid_mask = (~lat.isna()) & (~thru.isna())
        if valid_mask.any():
            plot_scatter(thru[valid_mask], lat[valid_mask], outdir / "throughput_vs_latency.png", "Throughput vs mean recv->writer latency", xlabel="rows/sec", ylabel="mean recv->writer ms")
            print(f"[{iso_now()}] wrote throughput_vs_latency.png")
    except Exception as e:
        print("throughput vs latency plot failed:", e)

    # correlation heatmap of numeric columns (select computed numeric metrics)
    numeric_fields = [
        'recv_to_parse_ms','parse_to_enqueue_ms','enqueue_to_writer_ms','writer_to_persist_ms',
        'recv_to_writer_ms','recv_to_persist_ms','bytes'
    ]
    available = [c for c in numeric_fields if c in df.columns]
    if len(available) >= 2:
        corr_df = df[available].corr().fillna(0)
        plot_heatmap(corr_df, outdir / "corr_heatmap.png", "Correlation heatmap of metrics")
        print(f"[{iso_now()}] wrote corr_heatmap.png")

    # write a small text summary
    summary_lines = []
    summary_lines.append(f"metrics file: {metrics_path}")
    summary_lines.append(f"rows: {len(df)}")
    summary_lines.append(f"time range: {df.index.min()} -> {df.index.max()}")
    summary_lines.append("")
    # percentile summaries for main metrics
    for col in ['recv_to_writer_ms','recv_to_persist_ms','writer_to_persist_ms']:
        if col in df.columns:
            s = pctile_summary(df[col].dropna())
            if s:
                summary_lines.append(f"{col}: n={s['count']} mean={s['mean']:.3f} median={s['median']:.3f} p95={s['p95']:.3f} max={s['max']:.3f}")
    summary_txt = outdir / "metrics_summary.txt"
    with open(summary_txt, "w") as fh:
        fh.write("\n".join(summary_lines))
    print(f"[{iso_now()}] wrote summary to {summary_txt}")
    print(f"[{iso_now()}] finished. plots saved to {outdir}")

if __name__ == "__main__":
    main()
