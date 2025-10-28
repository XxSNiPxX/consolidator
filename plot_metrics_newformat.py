#!/usr/bin/env python3
"""
plot_metrics_newformat.py (revised)

Reads a metrics CSV with columns:
  ts_iso, kind, exchange, asset, recv, parse, enqueue, writer, persist, bytes

Produces per-asset aggregated CSV + plots:
  - counts & bytes timeseries
  - mean latency timeseries
  - rolling percentiles for chosen latency
  - throughput vs latency scatter
  - latency histogram
  - correlation heatmap

Usage:
  python3 plot_metrics_newformat.py --metrics metrics.csv --outdir plots_metrics --rolling-sec 60 --asset SOLUSDT
"""
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
import os
import math
import textwrap
import sys

def iso_now():
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

def ensure_outdir(p: Path):
    p.mkdir(parents=True, exist_ok=True)
    return p

def safe_find(cols, candidates):
    for c in candidates:
        if c in cols:
            return c
    return None

def parse_time_index(df, ts_candidates):
    ts_col = safe_find(df.columns, ts_candidates)
    if ts_col is None:
        # fallback to first column
        ts_col = df.columns[0]
    # try parse ISO or numeric epochs
    ser = pd.to_datetime(df[ts_col], utc=True, errors='coerce')
    # if many nulls try numeric heuristics
    if ser.isna().sum() > len(ser) * 0.25:
        num = pd.to_numeric(df[ts_col], errors='coerce')
        if num.notna().any():
            mx = num.max()
            if mx > 1e12:
                ser = pd.to_datetime(num, unit='ms', utc=True, errors='coerce')
            elif mx > 1e9:
                ser = pd.to_datetime(num, unit='s', utc=True, errors='coerce')
    df.index = ser
    df = df.sort_index()
    return df, ts_col

def pctile_summary(arr):
    arr = np.asarray(arr.dropna())
    if arr.size == 0:
        return None
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

def plot_timeseries(series_dict, outpath, title, ylabel=None):
    plt.figure(figsize=(12,4))
    plotted = False
    for name, ser in series_dict.items():
        if ser is None or ser.dropna().empty:
            continue
        plt.plot(ser.index, ser.values, label=name, linewidth=0.9)
        plotted = True
    if not plotted:
        plt.figure(figsize=(6,3)); plt.text(0.5,0.5,"no data",ha="center")
    plt.title(title)
    if ylabel: plt.ylabel(ylabel)
    plt.legend()
    plt.grid(alpha=0.2)
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()

def plot_rolling_percentiles(series, outpath, window_sec=60):
    if series is None or series.dropna().empty:
        plt.figure(figsize=(10,3)); plt.text(0.5,0.5,"no data",ha="center"); plt.savefig(outpath); plt.close(); return
    try:
        roll = series.astype(float).rolling(f"{int(window_sec)}s")
        pm = roll.median()
        p90 = roll.quantile(0.9)
        p95 = roll.quantile(0.95)
        p99 = roll.quantile(0.99)
        plt.figure(figsize=(12,4))
        plt.plot(pm.index, pm.values, label="p50")
        plt.plot(p90.index, p90.values, label="p90")
        plt.plot(p95.index, p95.values, label="p95")
        plt.plot(p99.index, p99.values, label="p99")
        plt.title(f"Rolling percentiles ({window_sec}s)")
        plt.legend()
        plt.grid(alpha=0.2)
        plt.tight_layout()
        plt.savefig(outpath)
        plt.close()
    except Exception as e:
        plt.figure(figsize=(10,3)); plt.text(0.5,0.5,f"rolling failed: {e}",ha="center"); plt.savefig(outpath); plt.close()

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

def plot_hist(series, outpath, title, bins=80):
    plt.figure(figsize=(8,3))
    if series is None or series.dropna().empty:
        plt.text(0.5,0.5,"no data",ha="center")
    else:
        plt.hist(series.dropna().values, bins=bins)
    plt.title(title)
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()

def plot_heatmap(corr, outpath, title):
    if corr is None or corr.empty:
        plt.figure(figsize=(6,3)); plt.text(0.5,0.5,"no data",ha="center"); plt.savefig(outpath); plt.close(); return
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

def aggregate_by_interval(df_asset, interval='1S'):
    if df_asset.empty:
        return pd.DataFrame()
    lat_cols = ['recv','parse','enqueue','writer','persist']
    lat_present = [c for c in lat_cols if c in df_asset.columns]
    bytes_col = None
    for cand in ['bytes','byte','size']:
        if cand in df_asset.columns:
            bytes_col = cand; break
    # resample
    res = {}
    agg = df_asset.resample(interval).agg({})  # create index
    agg['count'] = df_asset.resample(interval).size()
    if bytes_col:
        agg['bytes_sum'] = df_asset[bytes_col].astype(float).resample(interval).sum()
    for c in lat_present:
        agg[f'{c}_mean'] = df_asset[c].astype(float).resample(interval).mean()
        agg[f'{c}_median'] = df_asset[c].astype(float).resample(interval).median()
        agg[f'{c}_max'] = df_asset[c].astype(float).resample(interval).max()
    return agg

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--metrics", required=True, help="metrics CSV path")
    p.add_argument("--outdir", default="plots_metrics", help="output directory")
    p.add_argument("--asset", default=None, help="asset to analyze (optional). If omitted, analyze all assets found.")
    p.add_argument("--rolling-sec", type=int, default=60, help="rolling window seconds for percentiles")
    p.add_argument("--interval", default="1S", help="aggregation interval for timeseries (e.g. 1S, 1T, 1min)")
    args = p.parse_args()

    metrics_path = Path(args.metrics)
    outdir = ensure_outdir(Path(args.outdir))

    if not metrics_path.exists():
        print("Metrics file not found:", metrics_path)
        return

    # read CSV
    df = pd.read_csv(metrics_path, low_memory=False)
    if df.empty:
        print("Empty metrics file")
        return
    # normalize headers: strip + lower
    df.columns = [c.strip() for c in df.columns]
    # parse time index robustly
    ts_candidates = ['ts_iso','ts','time','timestamp']
    df, ts_col = parse_time_index(df, ts_candidates)

    # normalize to lower-case colnames for convenience
    df.columns = [c.lower() for c in df.columns]

    # pick asset column
    asset_col = safe_find(df.columns, ['asset','symbol','instrument'])
    if args.asset:
        assets = [args.asset.upper()]
    else:
        if asset_col:
            assets = sorted(df[asset_col].dropna().astype(str).str.upper().unique().tolist())
        else:
            assets = ["ALL"]

    summary_lines = []
    summary_lines.append(f"metrics file: {metrics_path}")
    summary_lines.append(f"rows: {len(df)}")
    summary_lines.append(f"index range: {df.index.min()} -> {df.index.max()}")
    summary_lines.append(f"assets detected: {assets}")

    for asset in assets:
        if asset == "ALL":
            df_asset = df.copy()
        else:
            if asset_col:
                df_asset = df[df[asset_col].astype(str).str.upper() == asset].copy()
            else:
                df_asset = df.copy()
        if df_asset.empty:
            print(f"[{iso_now()}] no rows for asset {asset}, skipping.")
            continue

        # coerce numeric latency columns
        for c in ['recv','parse','enqueue','writer','persist','bytes']:
            if c in df_asset.columns:
                df_asset[c] = pd.to_numeric(df_asset[c], errors='coerce')

        # aggregate
        agg = aggregate_by_interval(df_asset, interval=args.interval)
        if agg.empty:
            print(f"[{iso_now()}] aggregation empty for {asset}")
            continue

        # save aggregated CSV
        out_csv = outdir / f"{asset}_metrics_agg.csv"
        agg.to_csv(out_csv)
        print(f"[{iso_now()}] wrote aggregated CSV: {out_csv}")

        # summary stats for latencies
        lat_cols_present = [c for c in ['recv','parse','enqueue','writer','persist'] if c in df_asset.columns]
        summary_lines.append(f"\nAsset: {asset}")
        summary_lines.append(f"rows: {len(df_asset)}")
        for c in lat_cols_present:
            s = pctile_summary(df_asset[c].dropna())
            if s:
                summary_lines.append(f" - {c}: count={s['count']} mean={s['mean']:.3f} median={s['median']:.3f} p90={s['p90']:.3f} p95={s['p95']:.3f} p99={s['p99']:.3f} max={s['max']:.3f}")
            else:
                summary_lines.append(f" - {c}: no data")

        # plots
        # counts & bytes
        series_counts = {}
        if 'count' in agg.columns:
            series_counts['count'] = agg['count']
        if 'bytes_sum' in agg.columns:
            series_counts['bytes'] = agg['bytes_sum']
        if series_counts:
            plot_timeseries(series_counts, outdir / f"{asset}_counts_bytes_timeseries.png", f"{asset} counts & bytes", ylabel="value")
            print(f"[{iso_now()}] wrote {asset}_counts_bytes_timeseries.png")

        # mean latencies timeseries
        mean_cols = [col for col in agg.columns if col.endswith('_mean')]
        mean_series = {col: agg[col] for col in mean_cols}
        if mean_series:
            plot_timeseries(mean_series, outdir / f"{asset}_latency_means.png", f"{asset} mean latencies (per {args.interval})", ylabel="ms")
            print(f"[{iso_now()}] wrote {asset}_latency_means.png")

        # choose latency for rolling percentiles (persist_mean -> writer_mean -> enqueue_mean -> parse_mean -> recv_mean)
        chosen = None
        for cand in ['persist_mean','writer_mean','enqueue_mean','parse_mean','recv_mean']:
            if cand in agg.columns:
                chosen = cand; break
        if chosen:
            plot_rolling_percentiles(agg[chosen].dropna(), outdir / f"{asset}_rolling_percentiles_{chosen}.png", window_sec=args.rolling_sec)
            print(f"[{iso_now()}] wrote {asset}_rolling_percentiles_{chosen}.png")

        # throughput vs latency scatter
        if 'count' in agg.columns and chosen in agg.columns:
            # align non-null
            x = agg['count']; y = agg[chosen]
            mask = x.notna() & y.notna()
            plot_scatter(x[mask], y[mask], outdir / f"{asset}_throughput_vs_latency.png", f"{asset} throughput vs {chosen}", xlabel="count", ylabel=chosen)
            print(f"[{iso_now()}] wrote {asset}_throughput_vs_latency.png")
        elif 'bytes_sum' in agg.columns and chosen in agg.columns:
            x = agg['bytes_sum']; y = agg[chosen]
            mask = x.notna() & y.notna()
            plot_scatter(x[mask], y[mask], outdir / f"{asset}_bytes_vs_latency.png", f"{asset} bytes vs {chosen}", xlabel="bytes", ylabel=chosen)
            print(f"[{iso_now()}] wrote {asset}_bytes_vs_latency.png")

        # latency histograms
        for c in lat_cols_present:
            plot_hist(df_asset[c], outdir / f"{asset}_hist_{c}.png", f"{asset} latency histogram: {c}")
            print(f"[{iso_now()}] wrote {asset}_hist_{c}.png")

        # correlation heatmap across numeric agg columns
        numeric_cols = [c for c in agg.columns if agg[c].dtype.kind in 'fiu' and not agg[c].isna().all()]
        if len(numeric_cols) >= 2:
            corr_df = agg[numeric_cols].corr().fillna(0)
            plot_heatmap(corr_df, outdir / f"{asset}_corr_heatmap.png", f"{asset} correlation heatmap")
            print(f"[{iso_now()}] wrote {asset}_corr_heatmap.png")

    # write summary
    with open(outdir / "metrics_newformat_summary.txt", "w") as fh:
        fh.write("\n".join(summary_lines))
    print(f"[{iso_now()}] summary written to {outdir}/metrics_newformat_summary.txt")
    print(f"[{iso_now()}] finished. plots and CSVs in {outdir}")

if __name__ == "__main__":
    main()
