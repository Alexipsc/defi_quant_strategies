"""
fetch_thegraph.py
-----------------
Fetches historical daily data for the USDC/WETH 0.05% Uniswap v3 pool
from The Graph's decentralized network and saves it as a CSV.

Pool:     USDC/WETH 0.05% (token0=USDC, token1=WETH)
Address:  0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640
Fields:   date, token0Price, volumeUSD, feesUSD, tvlUSD

NOTE: The Graph's free hosted service was shut down in 2024.
The decentralized network requires a free API key:
  1. Sign up at https://thegraph.com/studio/
  2. Go to https://thegraph.com/studio/apikeys/ and create a key
  3. Add THEGRAPH_API_KEY=<your_key> to the .env file at the project root

The free tier provides 1,000 queries/month at no cost.
"""

import os
import csv
import requests
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

# ── Configuration ──────────────────────────────────────────────────────────────

POOL_ADDRESS = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
POOL_LABEL   = "USDC-WETH-005"     # used in the output CSV filename
PAGE_SIZE    = 1000                 # max rows per request (TheGraph hard limit)

# Uniswap v3 subgraph on The Graph decentralised network (Ethereum mainnet)
# Explorer: https://thegraph.com/explorer/subgraphs/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV
SUBGRAPH_ID  = "5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"
GATEWAY_URL  = "https://gateway.thegraph.com/api/{key}/subgraphs/id/{id}"

# CSV goes into the cache folder next to this file
CACHE_DIR    = Path(__file__).parent / "cache"

# ── GraphQL query ──────────────────────────────────────────────────────────────
#
# Pagination strategy: instead of `skip` (limited to 5000 by TheGraph),
# we use `date_gt: <last_seen_date>` to cursor-paginate through all rows.

QUERY = """
query($pool: String!, $lastDate: Int!, $pageSize: Int!) {
  poolDayDatas(
    first: $pageSize
    orderBy: date
    orderDirection: asc
    where: {
      pool: $pool
      date_gt: $lastDate
    }
  ) {
    date
    token0Price
    volumeUSD
    feesUSD
    tvlUSD
  }
}
"""

# ── Helpers ────────────────────────────────────────────────────────────────────

def load_api_key() -> str:
    """
    Load THEGRAPH_API_KEY from .env file at the project root,
    falling back to the shell environment.
    Raises a clear error if the key is missing or is the placeholder value.
    """
    # Load .env two levels up from this file (project root)
    env_path = Path(__file__).parents[2] / ".env"
    load_dotenv(dotenv_path=env_path)

    key = os.environ.get("THEGRAPH_API_KEY", "").strip()

    if not key or key == "your_api_key_here":
        raise EnvironmentError(
            "\nTHEGRAPH_API_KEY not set or still using the placeholder value.\n"
            "Steps to fix:\n"
            "  1. Go to https://thegraph.com/studio/apikeys/\n"
            "  2. Create a free API key (1,000 free queries/month)\n"
            "  3. Add it to .env:  THEGRAPH_API_KEY=<your_key>\n"
        )
    return key


def fetch_page(url: str, last_date: int) -> list[dict]:
    """Send one GraphQL request and return the list of poolDayData rows."""
    payload = {
        "query": QUERY,
        "variables": {
            "pool": POOL_ADDRESS,
            "lastDate": last_date,
            "pageSize": PAGE_SIZE,
        },
    }
    response = requests.post(url, json=payload, timeout=30)
    response.raise_for_status()

    body = response.json()

    # Surface GraphQL-level errors (distinct from HTTP errors)
    if "errors" in body:
        raise RuntimeError(f"GraphQL error: {body['errors']}")

    return body["data"]["poolDayDatas"]


def fetch_all_days(url: str) -> list[dict]:
    """
    Paginate through all available poolDayData rows by cursor.
    Stops when a page returns fewer rows than PAGE_SIZE.
    """
    all_rows = []
    last_date = 0   # Unix timestamp cursor; 0 = fetch from the beginning

    while True:
        print(f"  Fetching page (cursor date={last_date})...", end=" ", flush=True)
        page = fetch_page(url, last_date)
        print(f"{len(page)} rows")

        all_rows.extend(page)

        # If we got a full page there may be more; otherwise we're done
        if len(page) < PAGE_SIZE:
            break

        # Advance cursor to the last date we received
        last_date = page[-1]["date"]

    return all_rows


def save_csv(rows: list[dict], out_path: Path) -> None:
    """Write the fetched rows to a CSV file."""
    fields = ["date_unix", "date", "token0Price", "volumeUSD", "feesUSD", "tvlUSD"]

    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()

        for row in rows:
            # Convert Unix timestamp to a human-readable date string
            dt = datetime.fromtimestamp(int(row["date"]), tz=timezone.utc)
            writer.writerow({
                "date_unix":   row["date"],
                "date":        dt.strftime("%Y-%m-%d"),
                "token0Price": row["token0Price"],
                "volumeUSD":   row["volumeUSD"],
                "feesUSD":     row["feesUSD"],
                "tvlUSD":      row["tvlUSD"],
            })


def print_preview(rows: list[dict], n: int = 5) -> None:
    """Print the first n rows as a quick sanity check."""
    print(f"\nFirst {n} rows:")
    header = f"{'Date':<12} {'token0Price':>14} {'volumeUSD':>16} {'feesUSD':>12} {'tvlUSD':>16}"
    print(header)
    print("-" * len(header))
    for row in rows[:n]:
        dt = datetime.fromtimestamp(int(row["date"]), tz=timezone.utc).strftime("%Y-%m-%d")
        print(
            f"{dt:<12} "
            f"{float(row['token0Price']):>14.6f} "
            f"${float(row['volumeUSD']):>15,.0f} "
            f"${float(row['feesUSD']):>11,.0f} "
            f"${float(row['tvlUSD']):>15,.0f}"
        )


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # 1. Load API key from .env
    api_key = load_api_key()
    url = GATEWAY_URL.format(key=api_key, id=SUBGRAPH_ID)

    print(f"Pool:     {POOL_LABEL}  ({POOL_ADDRESS})")
    print(f"Endpoint: {GATEWAY_URL.format(key='<hidden>', id=SUBGRAPH_ID)}\n")

    # 2. Fetch all days with cursor-based pagination
    print("Fetching data from The Graph...")
    rows = fetch_all_days(url)

    # 3. Preview first 5 rows
    print_preview(rows, n=5)

    # 4. Save to CSV
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    out_path = CACHE_DIR / f"{POOL_LABEL}-pool-day-data.csv"
    save_csv(rows, out_path)

    # 5. Report
    print(f"\nDone. Fetched {len(rows)} days of data.")
    print(f"Saved to: {out_path}")
