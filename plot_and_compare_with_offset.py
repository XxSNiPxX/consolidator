#!/usr/bin/env python3
"""
plot_and_compare_with_offset.py

Load a per-symbol CSV containing 'mmap' and 'ws' rows, estimate + correct clock offset,
match rows (trade-id first, nearest-timestamp next), write *_compared.csv and diagnostic plots.

Outputs:
  <outdir>/<SYMBOL>_compared.csv
  <outdir>/<SYMBOL>_price_timeseries.png
  <outdir>/<SYMBOL>_ws_vs_mmap_scatter.png
  <outdir>/<SYMBOL>_hist.png
  <outdir>/<SYMBOL>_cdf.png
  <outdir>/<SYMBOL>_rolling_mae.png
  <outdir>/<SYMBOL>_fraction_buckets.png
  <outdir>/<SYMBOL>_summary.txt
"""
from __future__ import annotations
import argparse
import math
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def iso_now():
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

def to_ms(x):
    if pd.isna(x):
        return np.nan
    try:
        v = float(x)
        # heuristics: > 1e12 already ms, >1e9 seconds
        if v > 1e12:
            return int(v)
        if v > 1e9:
            return int(v * 1000)
    except Exception:
        pass
    try:
        dt = pd.to_datetime(x, utc=True)
        return int(dt.value // 1_000_000)
    except Exception:
        return np.nan

def ensure_outdir(p: Path):
    p.mkdir(parents=True, exist_ok=True)
    return p

def estimate_median_offset(ws_ts_arr, mmap_ts_arr, sample_step=50):
    """Estimate median(ws_ts - mmap_ts) via sampled nearest neighbors (ms)."""
    ws = np.sort(np.asarray(ws_ts_arr.dropna().astype(float)))
    mm = np.sort(np.asarray(mmap_ts_arr.dropna().astype(float)))
    if len(ws) == 0 or len(mm) == 0:
        return 0.0
    i = 0
    diffs = []
    for wts in ws[::sample_step]:
        # advance i to nearest mm
        while i + 1 < len(mm) and abs(mm[i+1] - wts) <= abs(mm[i] - wts):
            i += 1
        diffs.append(wts - mm[i])
    diffs = np.array(diffs)
    return float(np.median(diffs)) if len(diffs) else 0.0

def nearest_ts_pairs(ws_df, mmap_df, ws_ts_col='ts_ms_adj', mmap_ts_col='ts_ms', max_dt=500):
    """For each ws row find nearest mmap row (two-pointer). Returns DataFrame of pairs."""
    if ws_df.empty or mmap_df.empty:
        return pd.DataFrame()
    w = ws_df[['__idx_ws', ws_ts_col, 'px', 'id_val']].copy().sort_values(ws_ts_col).reset_index(drop=True)
    m = mmap_df[['__idx_mmap', mmap_ts_col, 'px', 'id_val']].copy().sort_values(mmap_ts_col).reset_index(drop=True)
    i = 0
    rows = []
    for _, wrow in w.iterrows():
        wts = wrow[ws_ts_col]
        # skip NaN ts
        if pd.isna(wts):
            continue
        # move pointer to nearest mmap
        while i + 1 < len(m) and abs(m.loc[i+1, mmap_ts_col] - wts) <= abs(m.loc[i, mmap_ts_col] - wts):
            i += 1
        mrow = m.loc[i]
        if pd.isna(mrow[mmap_ts_col]):
            continue
        dt = abs(float(wts) - float(mrow[mmap_ts_col]))
        if dt <= max_dt:
            rows.append({
                'ws_idx': int(wrow['__idx_ws']),
                'mmap_idx': int(mrow['__idx_mmap']),
                'ws_ts_ms': float(wts),
                'mmap_ts_ms': float(mrow[mmap_ts_col]),
                'ws_px': float(wrow['px']) if not pd.isna(wrow['px']) else np.nan,
                'mmap_px': float(mrow['px']) if not pd.isna(mrow['px']) else np.nan,
                'ws_id': wrow.get('id_val', np.nan),
                'mmap_id': mrow.get('id_val', np.nan),
                'dt_ms': dt
            })
    return pd.DataFrame(rows)

def plot_timeseries(comp_df, outpath, symbol):
    plt.figure(figsize=(11,4))
    t = pd.to_datetime(comp_df["ws_ts_ms"], unit="ms", utc=True)
    if "ws_px" in comp_df.columns:
        plt.plot(t, comp_df["ws_px"], label="ws_px", marker='.', linewidth=0.6)
    if "mmap_px" in comp_df.columns:
        plt.plot(t, comp_df["mmap_px"], label="mmap_px", marker='.', linewidth=0.6)
    plt.title(f"{symbol} price timeseries")
    plt.legend()
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()

def plot_scatter(comp_df, outpath, symbol):
    plt.figure(figsize=(6,6))
    x = comp_df["ws_px"].dropna()
    y = comp_df["mmap_px"].dropna()
    # align lengths safely
    m = comp_df.dropna(subset=["ws_px","mmap_px"])
    plt.scatter(m["ws_px"], m["mmap_px"], s=6, alpha=0.5)
    if not m.empty:
        mn = min(m["ws_px"].min(), m["mmap_px"].min())
        mx = max(m["ws_px"].max(), m["mmap_px"].max())
        plt.plot([mn, mx], [mn, mx], linestyle='--', color='tab:blue')
    plt.xlabel("ws_px")
    plt.ylabel("mmap_px")
    plt.title(f"{symbol} ws vs mmap scatter")
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()

def plot_hist_cdf(comp_df, out_hist, out_cdf, symbol):
    diffs = comp_df["price_diff"].dropna().values
    if len(diffs) == 0:
        return
    plt.figure(figsize=(8,3))
    plt.hist(diffs, bins=60)
    plt.title(f"{symbol} price diffs (mmap - ws)")
    plt.tight_layout()
    plt.savefig(out_hist)
    plt.close()
    arr = np.sort(np.abs(diffs))
    p = np.linspace(0,1,len(arr))
    plt.figure(figsize=(8,3))
    plt.plot(arr, p)
    plt.title(f"{symbol} abs diff CDF")
    plt.tight_layout()
    plt.savefig(out_cdf)
    plt.close()

def plot_rolling_mae(comp_df, outpath, symbol, window_sec=30):
    if comp_df.empty or "price_diff" not in comp_df.columns:
        return
    df = comp_df.copy()
    df['ts'] = pd.to_datetime(df['ws_ts_ms'], unit='ms', utc=True)
    df = df.set_index('ts').sort_index()
    window = f"{int(window_sec)}s"
    mae = df['price_diff'].abs().rolling(window=window).mean()
    plt.figure(figsize=(10,3))
    mae.plot()
    plt.title(f"{symbol} rolling MAE ({window_sec}s)")
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()

def plot_fraction_buckets(comp_df, outpath, symbol, buckets=(1,10,100)):
    if comp_df.empty or "price_diff" not in comp_df.columns:
        return
    absd = comp_df["price_diff"].abs().dropna()
    if absd.empty:
        return
    fracs = [(absd <= b).mean() for b in buckets]
    plt.figure(figsize=(5,3))
    plt.bar([str(b) for b in buckets], fracs)
    plt.title(f"{symbol} fraction abs(diff) <= bucket")
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="input CSV (per-symbol), e.g. out/BTCUSDT.csv")
    p.add_argument("--outdir", default="plots", help="output directory")
    p.add_argument("--match-ms", type=int, default=500, help="max ms for nearest-timestamp matching after offset")
    p.add_argument("--rolling-sec", type=int, default=30, help="rolling window seconds for MAE plot")
    p.add_argument("--sample-step", type=int, default=50, help="sampling step for offset estimation")
    args = p.parse_args()

    inf = Path(args.input)
    outdir = ensure_outdir(Path(args.outdir))
    if not inf.exists():
        print("Input not found:", inf); return

    df = pd.read_csv(inf, low_memory=False)
    if df.empty:
        print("Empty input"); return

    # Detect symbol
    sym = inf.stem.upper().split('.')[0]

    # Normalize column names
    df.columns = [c.strip() for c in df.columns]

    # Ensure source present
    if 'source' not in df.columns:
        print("No 'source' column found, aborting."); return

    # Identify WS rows and mmap rows
    ws_df = df[df['source'].astype(str).str.lower().str.contains('ws', na=False)].copy()
    mmap_df = df[df['source'].astype(str).str.lower().str.contains('mmap', na=False)].copy()

    # If none found, try heuristics
    if ws_df.empty and mmap_df.empty:
        print("No ws or mmap rows identified by 'source'. Attempting heuristics...")
        # heuristic: rows with 'binance' or 'trade' likely ws
        ws_df = df[df.apply(lambda r: 'binance' in str(r).lower() or 'trade' in str(r).lower(), axis=1)].copy()
        mmap_df = df.drop(ws_df.index).copy()

    # Prepare ts and px columns
    # Try common timestamp names
    ws_ts_col_candidates = ['ws_ts','ws_ts_ms','ts_ms','ts','exchange_ts_ms','E']
    mmap_ts_col_candidates = ['recv_ts','recv_ts_ms','exchange_ts_ms','ts_ms','ts']
    # choose first present
    def pick_col(cands, frame):
        for c in cands:
            if c in frame.columns:
                return c
        return None

    raw_ws_ts_col = pick_col(ws_ts_col_candidates, ws_df) or pick_col(ws_ts_col_candidates, df)
    raw_mmap_ts_col = pick_col(mmap_ts_col_candidates, mmap_df) or pick_col(mmap_ts_col_candidates, df)

    # Price column
    price_cols = ['px','price','p','mid']
    raw_price_col = pick_col(price_cols, df) or pick_col(price_cols, ws_df) or pick_col(price_cols, mmap_df)

    # ID column (trade id / seq)
    id_cols = ['trade_id','seq_or_tradeid','seq','tradeid','id']
    id_col = None
    for c in id_cols:
        if c in df.columns:
            id_col = c; break

    # create normalized columns
    ws_df = ws_df.reset_index(drop=True)
    mmap_df = mmap_df.reset_index(drop=True)
    ws_df['__idx_ws'] = ws_df.index
    mmap_df['__idx_mmap'] = mmap_df.index

    ws_df['ts_ms_raw'] = ws_df.get(raw_ws_ts_col)
    mmap_df['ts_ms_raw'] = mmap_df.get(raw_mmap_ts_col)
    ws_df['ts_ms'] = ws_df['ts_ms_raw'].apply(to_ms)
    mmap_df['ts_ms'] = mmap_df['ts_ms_raw'].apply(to_ms)

    # price normalization
    if raw_price_col:
        ws_df['px'] = pd.to_numeric(ws_df.get(raw_price_col), errors='coerce')
        mmap_df['px'] = pd.to_numeric(mmap_df.get(raw_price_col), errors='coerce')
    else:
        # try common px per-frame
        for c in ['px','price','p','mid']:
            if c in ws_df.columns:
                ws_df['px'] = pd.to_numeric(ws_df[c], errors='coerce'); break
        for c in ['px','price','p','mid']:
            if c in mmap_df.columns:
                mmap_df['px'] = pd.to_numeric(mmap_df[c], errors='coerce'); break
        ws_df['px'] = ws_df.get('px', np.nan)
        mmap_df['px'] = mmap_df.get('px', np.nan)

    # id normalization
    if id_col:
        ws_df['id_val'] = ws_df.get(id_col)
        mmap_df['id_val'] = mmap_df.get(id_col)
    else:
        ws_df['id_val'] = np.nan
        mmap_df['id_val'] = np.nan

    # quick counts
    print(f"[{iso_now()}] rows: total={len(df)} ws={len(ws_df)} mmap={len(mmap_df)}")

    # Estimate clock offset: median(ws_ts - mmap_ts)
    offset_ms = estimate_median_offset(ws_df['ts_ms'], mmap_df['ts_ms'], sample_step=args.sample_step)
    print(f"[{iso_now()}] estimated median clock offset (ws_ts - mmap_recv_ts) = {offset_ms:.0f} ms")

    # If offset is large relative to match window, apply it to ws timestamps
    ws_df['ts_ms_adj'] = ws_df['ts_ms'] - offset_ms

    # Attempt trade-id exact matching first (if id available and numeric)
    paired_rows = []
    if not ws_df['id_val'].isna().all() and not mmap_df['id_val'].isna().all():
        # coerce numeric where possible
        ws_iddf = ws_df[~ws_df['id_val'].isna()].copy()
        mmap_iddf = mmap_df[~mmap_df['id_val'].isna()].copy()
        # Try string-equality merge first (some trade ids are strings)
        try:
            merged = pd.merge(ws_iddf, mmap_iddf, left_on='id_val', right_on='id_val', suffixes=('_ws','_mmap'))
            if not merged.empty:
                merged_pairs = pd.DataFrame({
                    'ws_idx': merged['__idx_ws'],
                    'mmap_idx': merged['__idx_mmap'],
                    'ws_ts_ms': merged['ts_ms_ws'],
                    'mmap_ts_ms': merged['ts_ms_mmap'],
                    'ws_px': merged['px_ws'],
                    'mmap_px': merged['px_mmap'],
                    'ws_id': merged['id_val'],
                    'mmap_id': merged['id_val'],
                    'dt_ms': (merged['ts_ms_ws'] - merged['ts_ms_mmap']).abs()
                })
                paired_rows.append(merged_pairs)
                print(f"[{iso_now()}] exact id matches found: {len(merged_pairs)}")
        except Exception:
            pass

    # Nearest-timestamp matching (after offset) for remaining rows
    near = nearest_ts_pairs(ws_df, mmap_df, ws_ts_col='ts_ms_adj', mmap_ts_col='ts_ms', max_dt=args.match_ms)
    print(f"[{iso_now()}] nearest-timestamp pairs found (within {args.match_ms}ms): {len(near)}")

    # Combine id-pairs and nearest pairs (concatenate, then dedupe by mmap_idx+ws_idx)
    if paired_rows:
        comp = pd.concat(paired_rows + [near], ignore_index=True, sort=False).drop_duplicates(subset=['ws_idx','mmap_idx'])
    else:
        comp = near.copy()
    if comp.empty:
        print("No matched pairs found after id & timestamp matching. Consider increasing --match-ms or check clocks.")
        return

    # compute price_diff
    comp['price_diff'] = comp['mmap_px'] - comp['ws_px']
    comp['abs_diff'] = comp['price_diff'].abs()

    # Save compared CSV (enrich with original rows for context)
    # attach original raw JSON-ish rows if desired; here we will include indexes and basic fields
    comp_out = comp.copy()
    comp_out = comp_out[['ws_idx','mmap_idx','ws_id','mmap_id','ws_ts_ms','mmap_ts_ms','dt_ms','ws_px','mmap_px','price_diff','abs_diff']]
    symname = sym
    out_csv = outdir / f"{symname}_compared.csv"
    comp_out.to_csv(out_csv, index=False)
    print(f"[{iso_now()}] wrote compared CSV: {out_csv} (pairs={len(comp_out)})")

    # Statistics
    n = len(comp_out)
    mae = comp_out['abs_diff'].mean()
    rmse = math.sqrt((comp_out['price_diff'] ** 2).mean())
    median = comp_out['abs_diff'].median()
    p90 = np.percentile(comp_out['abs_diff'].dropna(), 90)
    p95 = np.percentile(comp_out['abs_diff'].dropna(), 95)
    within1 = (comp_out['abs_diff'] <= 1).mean()
    within10 = (comp_out['abs_diff'] <= 10).mean()
    within100 = (comp_out['abs_diff'] <= 100).mean()

    print(f"[{iso_now()}] pairs={n} MAE={mae:.3f} RMSE={rmse:.3f} median_abs={median:.3f}")
    print(f"[{iso_now()}] within: <=1: {within1:.3f}, <=10: {within10:.3f}, <=100: {within100:.3f}")

    # Prepare comp_df for plotting (canonical columns)
    comp_df = comp.copy()
    comp_df = comp_df.rename(columns={'ws_ts_ms':'ws_ts_ms','mmap_ts_ms':'mmap_ts_ms','ws_px':'ws_px','mmap_px':'mmap_px'})
    # Plotting
    base = outdir / f"{symname}"
    plot_timeseries(comp_df, str(base) + "_price_timeseries.png", symname)
    plot_scatter(comp_df, str(base) + "_ws_vs_mmap_scatter.png", symname)
    plot_hist_cdf(comp_df, str(base) + "_hist.png", str(base) + "_cdf.png", symname)
    plot_rolling_mae(comp_df, str(base) + "_rolling_mae.png", symname, window_sec=args.rolling_sec)
    plot_fraction_buckets(comp_df, str(base) + "_fraction_buckets.png", symname)

    # Write plain text summary
    with open(outdir / f"{symname}_summary.txt", "w") as fh:
        fh.write(f"symbol: {symname}\n")
        fh.write(f"input rows: total={len(df)} ws={len(ws_df)} mmap={len(mmap_df)}\n")
        fh.write(f"estimated_offset_ms (ws - mmap): {offset_ms:.0f}\n")
        fh.write(f"pairs: {n}\n")
        fh.write(f"MAE: {mae}\nRMSE: {rmse}\nmedian_abs: {median}\n90pct: {p90}\n95pct: {p95}\n")
        fh.write(f"fraction within 1/10/100: {within1}/{within10}/{within100}\n")

    print(f"[{iso_now()}] finished. plots and summary in {outdir}")

if __name__ == "__main__":
    import argparse
    main()
