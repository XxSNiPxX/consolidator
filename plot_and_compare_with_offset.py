#!/usr/bin/env python3
"""
plot_and_compare_with_offset_csvformat.py

Analyzes CSVs like SOLUSDT.csv containing:
  ts_iso, symbol, source, subsource, kind, seq_or_tradeid, px, sz, bid, ask, mid,
  recv_ts, ws_ts, arrival_ms, price_diff, matched_with, note

Extracts ws/mmap trades, aligns them by timestamp, estimates latency offset,
computes error metrics, and produces diagnostic plots.

Usage:
  python3 plot_and_compare_with_offset_csvformat.py --input out/SOLUSDT.csv --outdir plots --symbol SOLUSDT
"""

from __future__ import annotations
import argparse
import math
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

def iso_now():
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

def ensure_outdir(p: Path):
    p.mkdir(parents=True, exist_ok=True)
    return p

def to_ms(x):
    """Convert timestamps (int/float/iso string) to epoch ms."""
    if pd.isna(x):
        return np.nan
    try:
        x = float(x)
        if x > 1e12:
            return x
        elif x > 1e9:
            return x * 1000
    except Exception:
        pass
    try:
        dt = pd.to_datetime(x, utc=True)
        return dt.value // 1_000_000
    except Exception:
        return np.nan

def estimate_median_offset(ws_ts, mmap_ts, sample_step=50):
    """Estimate clock offset (median ws_ts - mmap_ts)."""
    ws = np.sort(np.asarray(ws_ts.dropna().astype(float)))
    mm = np.sort(np.asarray(mmap_ts.dropna().astype(float)))
    if len(ws) == 0 or len(mm) == 0:
        return 0.0
    diffs = []
    i = 0
    for w in ws[::sample_step]:
        while i + 1 < len(mm) and abs(mm[i + 1] - w) <= abs(mm[i] - w):
            i += 1
        diffs.append(w - mm[i])
    return float(np.median(diffs)) if diffs else 0.0

def nearest_ts_pairs(ws_df, mmap_df, max_dt=500):
    """Find nearest-timestamp pairs between ws and mmap rows."""
    if ws_df.empty or mmap_df.empty:
        return pd.DataFrame()
    ws = ws_df.sort_values("ws_ts_ms").reset_index(drop=True)
    mm = mmap_df.sort_values("recv_ts_ms").reset_index(drop=True)
    i = 0
    pairs = []
    for _, w in ws.iterrows():
        wts = w["ws_ts_ms"]
        while i + 1 < len(mm) and abs(mm.loc[i + 1, "recv_ts_ms"] - wts) <= abs(mm.loc[i, "recv_ts_ms"] - wts):
            i += 1
        m = mm.loc[i]
        dt = abs(float(wts) - float(m["recv_ts_ms"]))
        if dt <= max_dt:
            pairs.append({
                "ws_px": w["px"],
                "mmap_px": m["px"],
                "ws_ts_ms": wts,
                "mmap_ts_ms": m["recv_ts_ms"],
                "dt_ms": dt
            })
    return pd.DataFrame(pairs)

def plot_timeseries(df, path, symbol):
    plt.figure(figsize=(11,4))
    if df.empty:
        plt.text(0.5,0.5,"no data",ha="center")
    else:
        t = pd.to_datetime(df["ws_ts_ms"], unit="ms", utc=True)
        plt.plot(t, df["ws_px"], label="ws_px", linewidth=0.8)
        plt.plot(t, df["mmap_px"], label="mmap_px", linewidth=0.8)
    plt.legend()
    plt.title(f"{symbol} price time-series (ws vs mmap)")
    plt.tight_layout()
    plt.savefig(path)
    plt.close()

def plot_scatter(df, path, symbol):
    plt.figure(figsize=(6,6))
    if df.empty:
        plt.text(0.5,0.5,"no data",ha="center")
    else:
        plt.scatter(df["ws_px"], df["mmap_px"], s=8, alpha=0.5)
        mn, mx = df[["ws_px","mmap_px"]].min().min(), df[["ws_px","mmap_px"]].max().max()
        plt.plot([mn, mx],[mn, mx], "--", c="tab:blue")
    plt.xlabel("ws_px")
    plt.ylabel("mmap_px")
    plt.title(f"{symbol} ws vs mmap scatter")
    plt.tight_layout()
    plt.savefig(path)
    plt.close()

def plot_hist_cdf(df, hist_path, cdf_path, symbol):
    if df.empty:
        plt.figure(); plt.text(0.5,0.5,"no data",ha="center"); plt.savefig(hist_path); plt.close()
        plt.figure(); plt.text(0.5,0.5,"no data",ha="center"); plt.savefig(cdf_path); plt.close()
        return
    diffs = df["mmap_px"] - df["ws_px"]
    plt.figure(figsize=(8,3))
    plt.hist(diffs, bins=60)
    plt.title(f"{symbol} price diff (mmap - ws)")
    plt.tight_layout()
    plt.savefig(hist_path)
    plt.close()
    arr = np.sort(np.abs(diffs.dropna()))
    p = np.linspace(0,1,len(arr))
    plt.figure(figsize=(8,3))
    plt.plot(arr, p)
    plt.title(f"{symbol} |diff| CDF")
    plt.tight_layout()
    plt.savefig(cdf_path)
    plt.close()

def plot_rolling_mae(df, path, symbol, window_sec=30):
    if df.empty:
        plt.figure(); plt.text(0.5,0.5,"no data",ha="center"); plt.savefig(path); plt.close(); return
    t = pd.to_datetime(df["ws_ts_ms"], unit="ms", utc=True)
    s = pd.Series(np.abs(df["mmap_px"] - df["ws_px"]).values, index=t)
    roll = s.rolling(f"{window_sec}s").mean()
    plt.figure(figsize=(10,3))
    roll.plot()
    plt.title(f"{symbol} rolling MAE ({window_sec}s)")
    plt.tight_layout()
    plt.savefig(path)
    plt.close()

def plot_fraction_buckets(df, path, symbol, buckets=(0.001,0.01,0.1)):
    if df.empty:
        plt.figure(); plt.text(0.5,0.5,"no data",ha="center"); plt.savefig(path); plt.close(); return
    diffs = np.abs(df["mmap_px"] - df["ws_px"])
    fracs = [(diffs <= b).mean() for b in buckets]
    plt.figure(figsize=(5,3))
    plt.bar([str(b) for b in buckets], fracs)
    plt.title(f"{symbol} fraction |diff| <= bucket")
    plt.tight_layout()
    plt.savefig(path)
    plt.close()

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--outdir", default="plots")
    p.add_argument("--symbol", default=None)
    p.add_argument("--match-ms", type=int, default=500)
    p.add_argument("--rolling-sec", type=int, default=30)
    args = p.parse_args()

    inf = Path(args.input)
    outdir = ensure_outdir(Path(args.outdir))
    symbol = args.symbol or inf.stem.split(".")[0].upper()

    df = pd.read_csv(inf, low_memory=False)
    if df.empty:
        print("empty file"); return

    # Filter ws and mmap sources
    df["source"] = df["source"].astype(str).str.lower()
    ws_df = df[df["source"].str.contains("ws")].copy()
    mmap_df = df[df["source"].str.contains("mmap")].copy()
    print(f"[{iso_now()}] total={len(df)} ws={len(ws_df)} mmap={len(mmap_df)}")

    # Convert timestamps to ms
    ws_df["ws_ts_ms"] = ws_df["ws_ts"].apply(to_ms)
    mmap_df["recv_ts_ms"] = mmap_df["recv_ts"].apply(to_ms)
    ws_df["px"] = pd.to_numeric(ws_df["px"], errors="coerce")
    mmap_df["px"] = pd.to_numeric(mmap_df["px"], errors="coerce")

    # Estimate offset
    offset_ms = estimate_median_offset(ws_df["ws_ts_ms"], mmap_df["recv_ts_ms"])
    print(f"[{iso_now()}] estimated offset = {offset_ms:.1f} ms")
    ws_df["ws_ts_ms_adj"] = ws_df["ws_ts_ms"] - offset_ms

    # Match nearest timestamps
    ws_work = ws_df.drop(columns=['ws_ts_ms'], errors='ignore').rename(columns={"ws_ts_ms_adj": "ws_ts_ms"})
    ws_work['ws_ts_ms'] = pd.to_numeric(ws_work['ws_ts_ms'], errors='coerce')
    mmap_df['recv_ts_ms'] = pd.to_numeric(mmap_df['recv_ts_ms'], errors='coerce')
    ws_work = ws_work.dropna(subset=['ws_ts_ms'])
    comp = nearest_ts_pairs(ws_work, mmap_df, max_dt=args.match_ms)
    if comp.empty:
        print("no pairs found"); return

    comp["price_diff"] = comp["mmap_px"] - comp["ws_px"]
    comp["abs_diff"] = comp["price_diff"].abs()

    # Write compared CSV
    out_csv = outdir / f"{symbol}_compared.csv"
    comp.to_csv(out_csv, index=False)
    print(f"[{iso_now()}] wrote {out_csv}")

    # Stats
    mae = comp["abs_diff"].mean()
    rmse = math.sqrt((comp["price_diff"] ** 2).mean())
    median = comp["abs_diff"].median()
    p90 = np.percentile(comp["abs_diff"].dropna(), 90)
    p95 = np.percentile(comp["abs_diff"].dropna(), 95)
    print(f"[{iso_now()}] MAE={mae:.6f} RMSE={rmse:.6f} median={median:.6f} p90={p90:.6f} p95={p95:.6f}")
    dups = ws_df.columns[ws_df.columns.duplicated()].unique()
    if len(dups):
        print("duplicate columns:", dups)

    # Plots
    base = outdir / symbol
    plot_timeseries(comp, f"{base}_timeseries.png", symbol)
    plot_scatter(comp, f"{base}_scatter.png", symbol)
    plot_hist_cdf(comp, f"{base}_hist.png", f"{base}_cdf.png", symbol)
    plot_rolling_mae(comp, f"{base}_rolling_mae.png", symbol, window_sec=args.rolling_sec)
    plot_fraction_buckets(comp, f"{base}_buckets.png", symbol)

    # Summary
    with open(outdir / f"{symbol}_summary.txt", "w") as f:
        f.write(f"symbol={symbol}\nrows={len(df)} pairs={len(comp)}\noffset_ms={offset_ms:.1f}\n")
        f.write(f"MAE={mae}\nRMSE={rmse}\nmedian={median}\n90pct={p90}\n95pct={p95}\n")

    print(f"[{iso_now()}] finished, results in {outdir}")

if __name__ == "__main__":
    main()
