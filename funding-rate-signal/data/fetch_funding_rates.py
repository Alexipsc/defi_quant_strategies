"""
fetch_funding_rates.py
----------------------
Fetches historical hourly funding rates for ETH from the Hyperliquid
public API and saves them as a CSV file.

Endpoint : POST https://api.hyperliquid.xyz/info
Payload  : {"type": "fundingHistory", "coin": "ETH", "startTime": <ms>}

Hyperliquid returns at most 500 records per request, so we paginate by
advancing startTime to (last_record_time + 1) until we receive fewer
than 500 records, which signals the end of available history.

No API key is required — this is a fully public endpoint.
"""

import csv
import requests
from pathlib import Path
from datetime import datetime, timezone

# ── Configuration ──────────────────────────────────────────────────────────────

COIN       = "ETH"
START_TIME = 1640995200000        # Jan 1 2022 00:00:00 UTC in milliseconds
PAGE_SIZE  = 500                  # Hyperliquid returns max 500 records per call

API_URL    = "https://api.hyperliquid.xyz/info"

CACHE_DIR  = Path(__file__).parent / "cache"
CSV_PATH   = CACHE_DIR / f"{COIN}-funding-rates.csv"

# ── Fetch ──────────────────────────────────────────────────────────────────────

def fetch_page(start_time_ms: int) -> list[dict]:
    """
    Fetch one page of funding rate records starting from start_time_ms.
    Returns a list of records, each with: coin, fundingRate, premium, time.
    """
    payload = {
        "type":      "fundingHistory",
        "coin":      COIN,
        "startTime": start_time_ms,
    }
    response = requests.post(API_URL, json=payload, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_all(start_time_ms: int) -> list[dict]:
    """
    Paginate through all available funding rate history.
    Advances the startTime cursor using the last record's timestamp + 1ms.
    Stops when a page returns fewer records than PAGE_SIZE.
    """
    all_records = []
    cursor = start_time_ms

    while True:
        print(f"  Fetching page (startTime={cursor})...", end=" ", flush=True)
        page = fetch_page(cursor)
        print(f"{len(page)} records")

        all_records.extend(page)

        # Fewer than PAGE_SIZE means we've reached the end
        if len(page) < PAGE_SIZE:
            break

        # Advance cursor past the last record's timestamp to avoid duplicates
        cursor = page[-1]["time"] + 1

    return all_records


# ── Save ───────────────────────────────────────────────────────────────────────

def save_csv(records: list[dict], path: Path) -> None:
    """Write funding rate records to CSV with a human-readable datetime column."""
    fields = ["time_ms", "datetime", "coin", "fundingRate", "premium"]

    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()

        for r in records:
            dt = datetime.fromtimestamp(r["time"] / 1000, tz=timezone.utc)
            writer.writerow({
                "time_ms":     r["time"],
                "datetime":    dt.strftime("%Y-%m-%d %H:%M:%S"),
                "coin":        r["coin"],
                "fundingRate": r["fundingRate"],
                "premium":     r["premium"],
            })


# ── Preview ────────────────────────────────────────────────────────────────────

def print_preview(records: list[dict], n: int = 5) -> None:
    """Print the first n records as a sanity check."""
    print(f"\nFirst {n} records:")
    header = f"{'Datetime (UTC)':<22} {'Coin':<6} {'Funding Rate':>14} {'Premium':>14}"
    print(header)
    print("-" * len(header))

    for r in records[:n]:
        dt = datetime.fromtimestamp(r["time"] / 1000, tz=timezone.utc)
        print(
            f"{dt.strftime('%Y-%m-%d %H:%M:%S'):<22} "
            f"{r['coin']:<6} "
            f"{float(r['fundingRate']):>14.8f} "
            f"{float(r['premium']):>14.8f}"
        )


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"Coin      : {COIN}")
    print(f"Start     : {datetime.fromtimestamp(START_TIME / 1000, tz=timezone.utc).date()}")
    print(f"Endpoint  : {API_URL}\n")

    print("Fetching funding rate history from Hyperliquid...")
    records = fetch_all(START_TIME)

    print_preview(records)

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    save_csv(records, CSV_PATH)

    print(f"\nTotal records fetched : {len(records):,}")
    print(f"Date range            : {datetime.fromtimestamp(records[0]['time'] / 1000, tz=timezone.utc).date()} "
          f"-> {datetime.fromtimestamp(records[-1]['time'] / 1000, tz=timezone.utc).date()}")
    print(f"Saved to              : {CSV_PATH}")
