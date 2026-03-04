"""
fetch_binance_funding.py
------------------------
Fetches full historical ETH perpetual funding rates from Binance
(data available from 2019-11-27), saves to CSV, applies the same
±0.01% signal thresholds used on the Hyperliquid dataset, and prints
a side-by-side signal count comparison.

Endpoint : GET https://fapi.binance.com/fapi/v1/fundingRate
Params   : symbol=ETHUSDT, limit=1000, startTime, endTime
Fields   : symbol, fundingTime (ms), fundingRate, markPrice

Binance settles funding every 8 hours (same as Hyperliquid).
No API key required.
"""

import csv
import sys
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

# ── Paths & config ─────────────────────────────────────────────────────────────

CACHE_DIR    = Path(__file__).parent / "cache"
CSV_PATH     = CACHE_DIR / "ETH-binance-funding-rates.csv"
HL_CSV_PATH  = CACHE_DIR / "ETH-funding-rates.csv"       # Hyperliquid data

API_URL      = "https://fapi.binance.com/fapi/v1/fundingRate"
SYMBOL       = "ETHUSDT"
PAGE_SIZE    = 1000                                        # Binance max per request

# Start from 2020-01-01 as requested (data exists from 2019-11-27)
START_MS     = 1_577_836_800_000    # 2020-01-01 00:00:00 UTC

# Signal thresholds — same as signal.py
UPPER_THRESHOLD =  0.0001    # > +0.01% → signal -1 (short)
LOWER_THRESHOLD = -0.0001    # < -0.01% → signal +1 (long)

# ── Fetch ──────────────────────────────────────────────────────────────────────

def fetch_page(start_ms: int, end_ms: int) -> list[dict]:
    """Fetch one page of funding rate records from Binance."""
    resp = requests.get(API_URL, params={
        "symbol":    SYMBOL,
        "limit":     PAGE_SIZE,
        "startTime": start_ms,
        "endTime":   end_ms,
    }, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_all(start_ms: int) -> list[dict]:
    """
    Paginate forward from start_ms to now in PAGE_SIZE chunks.
    Advances cursor using the last record's fundingTime + 1ms.
    Stops when the page is shorter than PAGE_SIZE (end of history).
    """
    import time
    now_ms  = int(time.time() * 1000)
    records = []
    cursor  = start_ms

    while cursor < now_ms:
        end = min(cursor + PAGE_SIZE * 8 * 3_600_000, now_ms)  # 8h per record
        print(f"  Fetching {datetime.fromtimestamp(cursor/1000, tz=timezone.utc).date()} "
              f"-> {datetime.fromtimestamp(end/1000, tz=timezone.utc).date()} ...",
              end=" ", flush=True)

        page = fetch_page(cursor, end)
        print(f"{len(page)} records")
        records.extend(page)

        if len(page) < PAGE_SIZE:
            break

        cursor = page[-1]["fundingTime"] + 1

    return records


# ── Save ───────────────────────────────────────────────────────────────────────

def save_csv(records: list[dict], path: Path) -> None:
    """Write Binance funding rate records to CSV."""
    fields = ["fundingTime_ms", "datetime", "symbol", "fundingRate", "markPrice"]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for r in records:
            dt = datetime.fromtimestamp(r["fundingTime"] / 1000, tz=timezone.utc)
            writer.writerow({
                "fundingTime_ms": r["fundingTime"],
                "datetime":       dt.strftime("%Y-%m-%d %H:%M:%S"),
                "symbol":         r["symbol"],
                "fundingRate":    r["fundingRate"],
                "markPrice":      r.get("markPrice", ""),
            })


# ── Signal ─────────────────────────────────────────────────────────────────────

def apply_signal(df: pd.DataFrame, rate_col: str) -> pd.DataFrame:
    """Apply the ±0.01% threshold signal to a funding rate dataframe."""
    df = df.copy()
    df["signal"] = 0
    df.loc[df[rate_col].astype(float) < LOWER_THRESHOLD, "signal"] =  1
    df.loc[df[rate_col].astype(float) > UPPER_THRESHOLD, "signal"] = -1
    return df


# ── Comparison ─────────────────────────────────────────────────────────────────

def compare_signals(binance_df: pd.DataFrame) -> None:
    """Load Hyperliquid data and print a side-by-side signal comparison."""

    if not HL_CSV_PATH.exists():
        print(f"\nHyperliquid CSV not found at {HL_CSV_PATH} — skipping comparison.")
        return

    hl_df = pd.read_csv(HL_CSV_PATH)
    hl_df = apply_signal(hl_df, "fundingRate")

    def signal_stats(df, label, rate_col):
        rates  = df[rate_col].astype(float)
        long_n  = (df["signal"] ==  1).sum()
        short_n = (df["signal"] == -1).sum()
        flat_n  = (df["signal"] ==  0).sum()
        total   = len(df)
        return {
            "label":   label,
            "total":   total,
            "long":    long_n,
            "short":   short_n,
            "flat":    flat_n,
            "mean_fr": rates.mean(),
            "max_fr":  rates.max(),
            "min_fr":  rates.min(),
        }

    b_stats = signal_stats(binance_df, "Binance (2020-now)", "fundingRate")
    h_stats = signal_stats(hl_df,     "Hyperliquid (2023-now)", "fundingRate")

    w = 30
    print("\n" + "=" * 70)
    print("  SIGNAL COMPARISON — Binance vs Hyperliquid")
    print("  Thresholds: >+0.01% short  |  <-0.01% long")
    print("=" * 70)
    print(f"  {'Metric':<22} {'Binance':>20} {'Hyperliquid':>20}")
    print("-" * 70)

    def row(label, b_val, h_val):
        print(f"  {label:<22} {b_val:>20} {h_val:>20}")

    row("Period start",       "2020-01-01",           "2023-05-12")
    row("Total records",      f"{b_stats['total']:,}", f"{h_stats['total']:,}")
    row("Mean funding rate",
        f"{b_stats['mean_fr']*100:.5f}%",
        f"{h_stats['mean_fr']*100:.5f}%")
    row("Max funding rate",
        f"{b_stats['max_fr']*100:.4f}%",
        f"{h_stats['max_fr']*100:.4f}%")
    row("Min funding rate",
        f"{b_stats['min_fr']*100:.4f}%",
        f"{h_stats['min_fr']*100:.4f}%")
    print("-" * 70)
    row("Long signals (+1)",
        f"{b_stats['long']:,}  ({b_stats['long']/b_stats['total']*100:.2f}%)",
        f"{h_stats['long']:,}  ({h_stats['long']/h_stats['total']*100:.2f}%)")
    row("Short signals (-1)",
        f"{b_stats['short']:,}  ({b_stats['short']/b_stats['total']*100:.2f}%)",
        f"{h_stats['short']:,}  ({h_stats['short']/h_stats['total']*100:.2f}%)")
    row("No signal (0)",
        f"{b_stats['flat']:,}  ({b_stats['flat']/b_stats['total']*100:.2f}%)",
        f"{h_stats['flat']:,}  ({h_stats['flat']/h_stats['total']*100:.2f}%)")
    print("=" * 70)


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    print(f"Symbol    : {SYMBOL}")
    print(f"Start     : {datetime.fromtimestamp(START_MS/1000, tz=timezone.utc).date()}")
    print(f"Endpoint  : {API_URL}\n")

    # 1. Fetch all Binance funding rates
    print("Fetching Binance funding rate history...")
    records = fetch_all(START_MS)

    # 2. Save to CSV
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    save_csv(records, CSV_PATH)
    print(f"\nTotal records : {len(records):,}")
    print(f"Saved to      : {CSV_PATH}")

    # 3. Load into DataFrame for signal analysis
    df = pd.read_csv(CSV_PATH)
    df = apply_signal(df, "fundingRate")

    # 4. Print first 5 rows
    print(f"\nFirst 5 rows:")
    preview = df[["datetime", "fundingRate", "markPrice", "signal"]].head(5).copy()
    df["fundingRate"] = df["fundingRate"].astype(float)
    preview["fundingRate"] = preview["fundingRate"].astype(float).apply(lambda x: f"{x:.8f}")
    print(preview.to_string(index=False))

    # 5. Compare with Hyperliquid
    compare_signals(df)


if __name__ == "__main__":
    main()
