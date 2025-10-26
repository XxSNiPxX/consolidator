#!/usr/bin/env python3
"""
plot_and_compare.py

Read per-symbol CSV(s) that contain mmap and websocket rows (and possibly compare rows),
clean & align the data (match by trade_id when available, otherwise nearest timestamp within match-ms),
produce a paired comparison CSV and a set of diagnostic plots.

Outputs: <outdir>/<SYMBOL>_compared.csv and several PNGs per symbol.
"""
from __future__ import annotations
import argparse
import os
import math
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# ---------------------------
# Helpers
# ---------------------------
def iso_now():
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

def to_ms(x):
    """Coerce numeric timestamp (ms) or ISO string to integer ms since epoch, or NaN."""
    if pd.isna(x):
        return np.nan
    try:
        # already numeric (ms)
        vx = float(x)
        if vx > 1e12:  # already ms
            return int(vx)
        if vx > 1e9:   # seconds
            return int(vx * 1000)
    except Exception:
        pass
    # try parse ISO
    try:
        dt = pd.to_datetime(x, utc=True)
        return int(dt.value // 1_000_000)
    except Exception:
        return np.nan

def safe_float_col(s):
    return pd.to_numeric(s, errors="coerce")

def ensure_outdir(d: Path):
    d.mkdir(parents=True, exist_ok=True)
    return d

# ---------------------------
# Matching logic
# ---------------------------
def match_by_trade_id(ws_df: pd.DataFrame, mmap_df: pd.DataFrame, id_col_ws="seq_or_tradeid", id_col_mmap="seq_or_tradeid"):
    """
    Return paired DataFrame for rows where trade id (or seq) matches exactly.
    The caller should ensure relevant columns exist.
    """
    # rename ids to common key
    w = ws_df.copy(); m = mmap_df.copy()
    w = w.rename(columns={id_col_ws: "match_id"})
    m = m.rename(columns={id_col_mmap: "match_id"})
    # drop NaN ids
    w = w[~w["match_id"].isna()].copy()
    m = m[~m["match_id"].isna()].copy()
    if w.empty or m.empty:
        return pd.DataFrame()
    # coerce numeric
    w["match_id"] = pd.to_numeric(w["match_id"], errors="coerce")
    m["match_id"] = pd.to_numeric(m["match_id"], errors="coerce")
    merged = pd.merge(w, m, on="match_id", suffixes=("_ws","_mmap"))
    return merged

def nearest_ts_join(ws_df: pd.DataFrame, mmap_df: pd.DataFrame, max_dt_ms=500):
    """
    For each ws row find the nearest mmap row by timestamp (ws_ts vs recv_ts).
    Returns DataFrame of pairs and the dt (abs difference in ms).
    This is O(n+m) by two-pointer after sorting.
    """
    if ws_df.empty or mmap_df.empty:
        return pd.DataFrame()
    w = ws_df.copy().sort_values("ts_ms").reset_index(drop=True)
    m = mmap_df.copy().sort_values("ts_ms").reset_index(drop=True)
    i = 0
    rows = []
    # For each ws row, advance mmap pointer to best candidate
    for _, wrow in w.iterrows():
        wts = wrow["ts_ms"]
        # move i while next mmap is closer
        while i + 1 < len(m) and abs(m.loc[i+1,"ts_ms"] - wts) <= abs(m.loc[i,"ts_ms"] - wts):
            i += 1
        mrow = m.loc[i]
        dt = abs(int(wts) - int(mrow["ts_ms"]))
        if dt <= max_dt_ms:
            # produce merged-style row combining fields; keep dt
            combined = {}
            # prefix source columns with ws_ and mmap_
            for c in wrow.index:
                combined[f"ws_{c}"] = wrow[c]
            for c in mrow.index:
                combined[f"mmap_{c}"] = mrow[c]
            combined["dt_ms"] = dt
            rows.append(combined)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)

# ---------------------------
# plotting functions
# ---------------------------
def plot_timeseries(comp_df, outpath, symbol):
    plt.figure(figsize=(10,4))
    ts = pd.to_datetime(comp_df["ws_ts_ms"], unit="ms", utc=True)
    # when ws exists
    if "ws_px" in comp_df.columns:
        plt.plot(ts, comp_df["ws_px"], label="ws px", linestyle='-', marker='.', markersize=2)
    if "mmap_px" in comp_df.columns:
        plt.plot(ts, comp_df["mmap_px"], label="mmap px", linestyle='-', marker='.', markersize=2)
    plt.title(f"{symbol} prices: mmap vs websocket")
    plt.xlabel("ts")
    plt.ylabel("price")
    plt.legend()
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()

def plot_scatter(comp_df, outpath, symbol):
    plt.figure(figsize=(8,4))
    x = comp_df["ws_px"]
    y = comp_df["mmap_px"]
    plt.scatter(x, y, s=10, alpha=0.6)
    # plot y=x
    mn = min(x.min(), y.min()); mx = max(x.max(), y.max())
    plt.plot([mn, mx], [mn, mx], linestyle='--', color='tab:blue')
    plt.title(f"{symbol} ws_px vs mmap_px (scatter)")
    plt.xlabel("ws_px")
    plt.ylabel("mmap_px")
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()

def plot_hist_cdf(comp_df, outpath_hist, outpath_cdf, symbol):
    diffs = comp_df["price_diff"].dropna().abs()
    if diffs.empty:
        return
    plt.figure(figsize=(8,4))
    plt.hist(comp_df["price_diff"].dropna(), bins=50)
    plt.title(f"{symbol} price difference (mmap - ws) histogram")
    plt.xlabel("price difference")
    plt.tight_layout()
    plt.savefig(outpath_hist)
    plt.close()

    # CDF
    arr = np.sort(diffs.values)
    p = np.linspace(0,1,len(arr))
    plt.figure(figsize=(8,4))
    plt.plot(arr, p)
    plt.title(f"{symbol} absolute price diff CDF")
    plt.xlabel("abs price difference")
    plt.ylabel("CDF")
    plt.tight_layout()
    plt.savefig(outpath_cdf)
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
    plt.title(f"{symbol} rolling MAE (window ~{window_sec}s)")
    plt.xlabel("ts")
    plt.ylabel("MAE")
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()

def plot_fraction_buckets(comp_df, outpath, symbol, buckets=(1,10,100)):
    if comp_df.empty or "price_diff" not in comp_df.columns:
        return
    absd = comp_df["price_diff"].abs().dropna()
    total = len(absd)
    fractions = {b: (absd <= b).mean() for b in buckets}
    plt.figure(figsize=(5,3))
    plt.bar([str(b) for b in buckets], [fractions[b] for b in buckets])
    plt.title(f"{symbol} fraction abs(diff) <= bucket")
    plt.ylabel("fraction")
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()

# ---------------------------
# main processing for one symbol CSV
# ---------------------------
def process_file(fn: Path, outdir: Path, match_ms=500, rolling_sec=30):
    print(f"[{iso_now()}] processing {fn}")
    df = pd.read_csv(fn, low_memory=False)
    if df.empty:
        print(" empty file; skipping")
        return

    # normalize lowercase column names
    df.columns = [c.strip() for c in df.columns]

    # Try to detect columns: source, subsource, seq/tradeid, px, recv_ts, ws_ts
    # Sources expected: 'mmap' and 'ws'; 'compare' maybe present
    # Normalize columns expected by the engine:
    # - seq_or_tradeid: seq/trade_id column name may vary; accept 'seq','trade_id','seq_or_tradeid'
    possible_id_cols = ["seq_or_tradeid","seq","trade_id","tradeid"]
    id_col = None
    for c in possible_id_cols:
        if c in df.columns:
            id_col = c
            break

    # unify numeric price columns
    # candidate price columns: px, price, mid, p
    price_cols = [c for c in ["px","price","mid","p"] if c in df.columns]
    # prefer px then mid then price/p
    price_col = price_cols[0] if price_cols else None

    # identify source-specific timestamp columns
    # possible: recv_ts, exchange_ts_ms, arrival_ms, ws_ts
    # We'll create ts_ms column for ws and mmap
    # find ws timestamp column
    ws_ts_candidates = [c for c in ["ws_ts","ws_ts_ms","ws_ts_ms","E","exchange_ts_ms","recv_ts_ms"] if c in df.columns]
    # but safer: detect numeric columns that look like ms since epoch
    def find_ts_candidates(df):
        cand = []
        for c in df.columns:
            if c.lower().endswith("ts") or c.lower().endswith("ts_ms") or c.lower().endswith("_ms"):
                cand.append(c)
        return cand

    ts_cands = find_ts_candidates(df)
    # We'll construct df_ws and df_mmap from rows by source column if available
    if "source" in df.columns:
        df_ws = df[df["source"].str.lower().str.contains("ws", na=False)].copy()
        df_mmap = df[df["source"].str.lower().str.contains("mmap", na=False)].copy()
        # if some rows are "compare" drop here (we'll regenerate)
        df_ws = df_ws[~(df_ws["source"].str.lower().str.contains("compare", na=False))]
    else:
        # fallback heuristics: rows with 'binance' or 'trade' in subsource -> ws
        df_ws = df[df.apply(lambda r: ("binance" in str(r).lower()) or ("@trade" in str(r).lower()) or ("ws" in str(r).lower()), axis=1)].copy()
        df_mmap = df.drop(df_ws.index).copy()

    # prepare columns for ws and mmap frames
    # create ts_ms column for both frames
    def extract_ts_ms_rowwise(frame, prefer_cols=None):
        prefer_cols = prefer_cols or []
        ts_ms = []
        for _, row in frame.iterrows():
            v = None
            for c in prefer_cols:
                if c in frame.columns and not pd.isna(row.get(c)):
                    v = row.get(c); break
            if v is None:
                # try common names
                for c in ["recv_ts","recv_ts_ms","exchange_ts_ms","ws_ts","ws_ts_ms","arrival_ms","ts","ts_ms"]:
                    if c in frame.columns and not pd.isna(row.get(c)):
                        v = row.get(c); break
            ts_ms.append(to_ms(v))
        frame = frame.copy()
        frame["ts_ms"] = ts_ms
        return frame

    df_ws = extract_ts_ms_rowwise(df_ws)
    df_mmap = extract_ts_ms_rowwise(df_mmap)

    # price extraction
    # ws_px: try px, price, p
    def extract_px(frame):
        for c in ["px","price","p","mid","bid","ask"]:
            if c in frame.columns:
                frame["px_val"] = pd.to_numeric(frame[c], errors="coerce")
                return frame
        # fallback: attempt any numeric-like column
        for c in frame.columns:
            if frame[c].dtype.kind in "fi":
                frame["px_val"] = pd.to_numeric(frame[c], errors="coerce")
                return frame
        frame["px_val"] = np.nan
        return frame

    df_ws = extract_px(df_ws.rename(columns={price_col: "px"}))
    df_mmap = extract_px(df_mmap.rename(columns={price_col: "px"}))

    # normalize id column into 'id'
    id_candidates = [c for c in ["trade_id","seq","seq_or_tradeid","id","tradeid"] if c in df_ws.columns or c in df_mmap.columns]
    id_col_use = id_candidates[0] if id_candidates else None
    if id_col_use:
        df_ws["id_val"] = df_ws.get(id_col_use)
        df_mmap["id_val"] = df_mmap.get(id_col_use)
    else:
        df_ws["id_val"] = np.nan
        df_mmap["id_val"] = np.nan

    # attempt matching: first by id if available
    paired = pd.DataFrame()
    if id_col_use:
        # only keep rows where id_val notna
        ws_with_id = df_ws[~df_ws["id_val"].isna()].copy()
        mmap_with_id = df_mmap[~df_mmap["id_val"].isna()].copy()
        if not ws_with_id.empty and not mmap_with_id.empty:
            # coerce numeric textual ids
            ws_with_id["id_val"] = pd.to_numeric(ws_with_id["id_val"], errors="coerce")
            mmap_with_id["id_val"] = pd.to_numeric(mmap_with_id["id_val"], errors="coerce")
            if not ws_with_id["id_val"].dropna().empty and not mmap_with_id["id_val"].dropna().empty:
                paired_id = pd.merge(ws_with_id, mmap_with_id, left_on="id_val", right_on="id_val", suffixes=("_ws","_mmap"))
                if not paired_id.empty:
                    # Map columns to canonical names
                    paired = paired_id.copy()
                    paired["ws_px"] = paired.get("px_val_ws", paired.get("px_val"))
                    paired["mmap_px"] = paired.get("px_val_mmap", paired.get("px_val"))
                    paired["ws_ts_ms"] = paired.get("ts_ms_ws", paired.get("ts_ms"))
                    paired["mmap_ts_ms"] = paired.get("ts_ms_mmap", paired.get("ts_ms"))
                    paired["price_diff"] = paired["mmap_px"] - paired["ws_px"]
    # Next, use nearest timestamp join for remaining unmatched rows
    # Build ws-only and mmap-only sets excluding those already paired by id (by index)
    matched_ws_idx = set()
    matched_mmap_idx = set()
    if not paired.empty:
        # indices in original frames are preserved as columns when merging? they might not be.
        # we will simple perform timestamp join on original frames for all rows and then dedupe by id matches later
        pass

    # Build simple ws and mmap frames for timestamp join
    ws_ts_frame = df_ws[["ts_ms","px_val","id_val"]].rename(columns={"px_val":"px"}).dropna(subset=["ts_ms"])
    mmap_ts_frame = df_mmap[["ts_ms","px_val","id_val"]].rename(columns={"px_val":"px"}).dropna(subset=["ts_ms"])
    ws_ts_frame = ws_ts_frame.reset_index(drop=True)
    mmap_ts_frame = mmap_ts_frame.reset_index(drop=True)

    # perform nearest timestamp join
    near = nearest_ts_join(ws_ts_frame.rename(columns={"px":"ws_px"}), mmap_ts_frame.rename(columns={"px":"mmap_px"}), max_dt_ms=match_ms)

    # If we had id-paired results, combine them preferentially
    if paired.empty:
        comp = near.copy()
        # normalize column names
        if not comp.empty:
            comp["ws_ts_ms"] = comp["ws_ts"]
            comp["mmap_ts_ms"] = comp["mmap_ts"]
            comp["ws_px"] = comp["ws_px"]
            comp["mmap_px"] = comp["mmap_px"]
            comp["price_diff"] = comp["mmap_px"] - comp["ws_px"]
    else:
        # map paired id frame into same structure if available
        # normalize paired to columns: ws_px, mmap_px, ws_ts_ms, mmap_ts_ms, price_diff
        p = paired.copy()
        if "ws_px" not in p.columns:
            p["ws_px"] = p.get("px_val_ws", np.nan)
        if "mmap_px" not in p.columns:
            p["mmap_px"] = p.get("px_val_mmap", np.nan)
        if "ws_ts_ms" not in p.columns:
            p["ws_ts_ms"] = p.get("ts_ms_ws", np.nan)
        if "mmap_ts_ms" not in p.columns:
            p["mmap_ts_ms"] = p.get("ts_ms_mmap", np.nan)
        p["price_diff"] = p["mmap_px"] - p["ws_px"]
        # union with near (avoid duplicates by ws_ts && mmap_ts)
        comp = p[["ws_ts_ms","mmap_ts_ms","ws_px","mmap_px","price_diff","dt_ms"]].copy() if "dt_ms" in p.columns else p[["ws_ts_ms","mmap_ts_ms","ws_px","mmap_px","price_diff"]].copy()
        # append near rows that are not covered by id matching (basic)
        if not near.empty:
            # Try to avoid duplicates by excluding rows with near equal (ws_ts,mmap_ts) already in comp
            existing = set()
            for _, r in comp.iterrows():
                existing.add((int(r["ws_ts_ms"]) if not pd.isna(r["ws_ts_ms"]) else None, int(r["mmap_ts_ms"]) if not pd.isna(r["mmap_ts_ms"]) else None))
            extras = []
            for _, r in near.iterrows():
                key = (int(r["ws_ts"]), int(r["mmap_ts"]))
                if key not in existing:
                    extras.append(r)
            if extras:
                extra_df = pd.DataFrame(extras)
                extra_df = extra_df.rename(columns={"ws_ts":"ws_ts_ms","mmap_ts":"mmap_ts_ms"})
                extra_df["price_diff"] = extra_df["mmap_px"] - extra_df["ws_px"]
                comp = pd.concat([comp, extra_df[["ws_ts_ms","mmap_ts_ms","ws_px","mmap_px","price_diff","dt_ms"] if "dt_ms" in extra_df.columns else ["ws_ts_ms","mmap_ts_ms","ws_px","mmap_px","price_diff"]]], ignore_index=True)

    if comp.empty:
        print("No matched pairs found (by id or nearest-timestamp). Nothing to plot.")
        return

    # canonicalize columns
    comp = comp.rename(columns=lambda c: c.strip())
    # ensure numeric columns exist
    for c in ["ws_px","mmap_px","price_diff","ws_ts_ms","mmap_ts_ms"]:
        if c not in comp.columns:
            comp[c] = np.nan
    comp["ws_ts_ms"] = pd.to_numeric(comp["ws_ts_ms"], errors="coerce")
    comp["mmap_ts_ms"] = pd.to_numeric(comp["mmap_ts_ms"], errors="coerce")
    comp["price_diff"] = pd.to_numeric(comp["price_diff"], errors="coerce")

    # add absolute diff
    comp["abs_diff"] = comp["price_diff"].abs()

    # Save compared CSV
    sym = fn.stem.replace("_compared","")
    out_csv = outdir / f"{sym}_compared.csv"
    comp.to_csv(out_csv, index=False)
    print(f" wrote compared CSV {out_csv}")

    # Stats
    n = len(comp)
    mae = comp["abs_diff"].mean()
    rmse = math.sqrt((comp["price_diff"] ** 2).mean())
    median = comp["abs_diff"].median()
    p90 = np.percentile(comp["abs_diff"].dropna(), 90)
    p95 = np.percentile(comp["abs_diff"].dropna(), 95)
    within1 = (comp["abs_diff"] <= 1).mean()
    within10 = (comp["abs_diff"] <= 10).mean()
    within100 = (comp["abs_diff"] <= 100).mean()

    print(f" pairs: {n}  MAE={mae:.3f} RMSE={rmse:.3f} median={median:.3f}")
    print(f" pct within: 1={within1:.3f}, 10={within10:.3f}, 100={within100:.3f}")

    # prepare plotting inputs: ensure ws_ts_ms exists as canonical time axis
    if "ws_ts_ms" not in comp.columns or comp["ws_ts_ms"].isna().all():
        # fallback to mmap ts
        comp["ws_ts_ms"] = comp["mmap_ts_ms"]
    comp = comp.sort_values("ws_ts_ms").reset_index(drop=True)

    # ensure px columns numeric
    comp["ws_px"] = pd.to_numeric(comp["ws_px"], errors="coerce")
    comp["mmap_px"] = pd.to_numeric(comp["mmap_px"], errors="coerce")
    comp["price_diff"] = pd.to_numeric(comp["price_diff"], errors="coerce")

    # generate plots
    ensure_outdir(outdir)
    base = outdir / f"{sym}"
    plot_timeseries(comp, str(base) + "_price_timeseries.png", sym)
    plot_scatter(comp, str(base) + "_ws_vs_mmap_scatter.png", sym)
    plot_hist_cdf(comp, str(base) + "_hist.png", str(base) + "_cdf.png", sym)
    plot_rolling_mae(comp, str(base) + "_rolling_mae.png", sym, window_sec=rolling_sec)
    plot_fraction_buckets(comp, str(base) + "_fraction_buckets.png", sym)

    # also save a small textual summary
    with open(outdir / f"{sym}_summary.txt", "w") as fh:
        fh.write(f"symbol: {sym}\n")
        fh.write(f"pairs: {n}\n")
        fh.write(f"MAE: {mae}\n")
        fh.write(f"RMSE: {rmse}\n")
        fh.write(f"median abs: {median}\n")
        fh.write(f"90pct abs: {p90}\n")
        fh.write(f"95pct abs: {p95}\n")
        fh.write(f"fraction within 1: {within1}\n")
        fh.write(f"fraction within 10: {within10}\n")
        fh.write(f"fraction within 100: {within100}\n")
    print(f"[{iso_now()}] plots & summary written for {sym}")

# ---------------------------
# CLI
# ---------------------------
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", help="input CSV file for a symbol (mutually exclusive with --input-dir)")
    p.add_argument("--input-dir", help="directory containing per-symbol CSVs to process")
    p.add_argument("--outdir", default="plots", help="output directory for plots and compared csvs")
    p.add_argument("--match-ms", type=int, default=500, help="max ms for nearest-timestamp matching")
    p.add_argument("--rolling-sec", type=int, default=30, help="rolling window for MAE (seconds)")
    args = p.parse_args()

    outdir = Path(args.outdir)
    ensure_outdir(outdir)

    files = []
    if args.input:
        files = [Path(args.input)]
    elif args.input_dir:
        files = sorted(Path(args.input_dir).glob("*.csv"))
    else:
        p.print_help()
        return

    for f in files:
        try:
            process_file(f, outdir, match_ms=args.match_ms, rolling_sec=args.rolling_sec)
        except Exception as e:
            print(f"Error processing {f}: {e}")

if __name__ == "__main__":
    main()
