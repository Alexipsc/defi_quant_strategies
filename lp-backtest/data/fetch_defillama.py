"""
fetch_defillama.py
------------------
Fetches all Uniswap v3 pools from the DefiLlama yields API and prints
the top 10 by TVL (USD).

API docs: https://yieldsapi.llama.fi
Endpoint: GET https://yields.llama.fi/pools
"""

import requests

DEFILLAMA_YIELDS_URL = "https://yields.llama.fi/pools"
PROJECT_FILTER = "uniswap-v3"
TOP_N = 10


def fetch_pools(url: str) -> list[dict]:
    """Fetch the full pool list from the DefiLlama yields endpoint."""
    response = requests.get(url, timeout=30)
    response.raise_for_status()          # raises HTTPError for 4xx/5xx
    return response.json()["data"]       # "data" key holds the pool list


def filter_uniswap_v3(pools: list[dict]) -> list[dict]:
    """Keep only Uniswap v3 pools."""
    return [p for p in pools if p.get("project") == PROJECT_FILTER]


def top_by_tvl(pools: list[dict], n: int) -> list[dict]:
    """Return the top-n pools sorted by TVL descending."""
    return sorted(pools, key=lambda p: p.get("tvlUsd", 0), reverse=True)[:n]


def print_table(pools: list[dict]) -> None:
    """Print a formatted table of pool data."""
    header = f"{'#':<4} {'Symbol':<25} {'Chain':<12} {'TVL (USD)':>16} {'APY (%)':>10}"
    print(header)
    print("-" * len(header))

    for i, pool in enumerate(pools, start=1):
        symbol  = pool.get("symbol", "N/A")[:24]   # trim long symbols
        chain   = pool.get("chain", "N/A")
        tvl     = pool.get("tvlUsd", 0)
        apy     = pool.get("apy", 0) or 0

        print(f"{i:<4} {symbol:<25} {chain:<12} ${tvl:>15,.0f} {apy:>9.2f}%")


if __name__ == "__main__":
    print(f"Fetching Uniswap v3 pools from DefiLlama...\n")

    pools     = fetch_pools(DEFILLAMA_YIELDS_URL)
    v3_pools  = filter_uniswap_v3(pools)
    top_pools = top_by_tvl(v3_pools, TOP_N)

    print(f"Found {len(v3_pools)} Uniswap v3 pools. Top {TOP_N} by TVL:\n")
    print_table(top_pools)
