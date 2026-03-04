"""
fetch_eth_prices_extended.py
-----------------------------
Backfills ETH/USDT hourly Binance klines from 2020-01-01 up to
the start of the existing price cache, then merges and saves the
combined file back to ETH-prices-1h.csv.

Endpoint : GET https://api.binance.com/api/v3/klines
Params   : symbol=ETHUSDT, interval=1h, startTime, endTime, limit=1000
"""

import time
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

# ── Config ───────────────────────────────────────────────────────────────────

BINANCE_URL  = "https://api.binance.com/api/v3/klines"
SYMBOL       = "ETHUSDT"
INTERVAL     = "1h"
LIMIT        = 1000
MS_PER_HOUR  = 3_600_000

# Fetch from 2020-01-01 00:00 UTC
START_MS     = 1_577_836_800_000   # 2020-01-01 00:00:00 UTC

CACHE_DIR    = Path(__file__).parent / "cache"
PRICE_CACHE  = CACHE_DIR / "ETH-prices-1h.csv"

# ── Fetch ────────────────────────────────────────────────────────────────────

def fetch_candles(start_ms: int, end_ms: int) -> list:
    resp = requests.get(BINANCE_URL, params={
        "symbol":    SYMBOL,
        "interval":  INTERVAL,
        "startTime": start_ms,
        "endTime":   end_ms,
        "limit":     LIMIT,
    }, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_range(start_ms: int, end_ms: int) -> pd.DataFrame:
    """
    Paginate forward from start_ms to end_ms in LIMIT-candle chunks.
    Returns a DataFrame indexed by timestamp_ms with close_price column.
    """
    rows   = []
    cursor = start_ms

    while cursor < end_ms:
        chunk_end = min(cursor + LIMIT * MS_PER_HOUR, end_ms)
        print(f"  {datetime.fromtimestamp(cursor/1000, tz=timezone.utc).date()} "
              f"-> {datetime.fromtimestamp(chunk_end/1000, tz=timezone.utc).date()} ...",
              end=" ", flush=True)

        candles = fetch_candles(cursor, chunk_end)
        print(f"{len(candles)} candles")

        for c in candles:
            rows.append({"timestamp_ms": int(c[0]), "close_price": float(c[4])})

        if not candles:
            break

        cursor = int(candles[-1][0]) + MS_PER_HOUR
        time.sleep(0.12)   # stay well within Binance rate limit

    if not rows:
        return pd.DataFrame(columns=["timestamp_ms", "close_price"]).set_index("timestamp_ms")

    df = pd.DataFrame(rows)
    df = df.drop_duplicates("timestamp_ms").sort_values("timestamp_ms")
    df = df.set_index("timestamp_ms")
    return df


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    # Load existing cache
    existing = pd.read_csv(PRICE_CACHE, index_col="timestamp_ms")
    cache_start_ms = int(existing.index.min())
    print(f"Existing cache : {len(existing):,} candles  "
          f"({datetime.fromtimestamp(existing.index.min()/1000, tz=timezone.utc).date()} "
          f"-> {datetime.fromtimestamp(existing.index.max()/1000, tz=timezone.utc).date()})")

    if START_MS >= cache_start_ms:
        print("Nothing to backfill — cache already starts at or before target date.")
        return

    print(f"\nBackfilling from {datetime.fromtimestamp(START_MS/1000, tz=timezone.utc).date()} "
          f"to {datetime.fromtimestamp(cache_start_ms/1000, tz=timezone.utc).date()} ...")

    new_df = fetch_range(START_MS, cache_start_ms)
    print(f"\nFetched {len(new_df):,} new candles")

    # Merge: new (older) + existing (newer), drop duplicates
    combined = pd.concat([new_df, existing])
    combined = combined[~combined.index.duplicated(keep="last")]
    combined = combined.sort_index()

    combined.to_csv(PRICE_CACHE)
    print(f"\nMerged cache saved: {PRICE_CACHE}")
    print(f"  Total candles : {len(combined):,}")
    print(f"  Date range    : "
          f"{datetime.fromtimestamp(combined.index.min()/1000, tz=timezone.utc).date()} "
          f"-> {datetime.fromtimestamp(combined.index.max()/1000, tz=timezone.utc).date()}")


if __name__ == "__main__":
    main()
