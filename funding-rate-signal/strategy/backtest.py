"""
backtest.py
-----------
Backtests the funding rate mean-reversion signal defined in signal.py
using actual ETH price changes over the 24-hour hold window.

Strategy rules
--------------
  When signal = -1 (short) : enter short at signal time, exit 24h later
  When signal = +1 (long)  : enter long  at signal time, exit 24h later

Return measure
--------------
  trade_return = signal × (exit_price / entry_price − 1)

  - entry_price : ETH close price at the hour the signal fires
  - exit_price  : ETH close price exactly 24 hours later
  - signal direction multiplies so that a correct directional bet is positive

Price data
----------
Hourly ETH/USDT candles are fetched from the Binance public REST API
(no API key required) and cached locally in data/cache/.
The API returns max 1,000 candles per request, so we paginate forward
in 1,000-hour chunks from the start of the funding rate history to now.

Each signal triggers an independent 24-hour trade.
Overlapping trades are treated independently.

Outputs
-------
  - Console summary : total return, win rate, Sharpe ratio
  - PNG chart       : cumulative PnL over time saved to funding-rate-signal/outputs/
"""

import math
import time
import requests
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path
from datetime import datetime, timezone

# Import signal logic from signal.py (same directory)
import sys
sys.path.insert(0, str(Path(__file__).parent))
from signal import generate_signal, CSV_PATH, UPPER_THRESHOLD, LOWER_THRESHOLD

# ── Paths ──────────────────────────────────────────────────────────────────────

SCRIPT_DIR  = Path(__file__).parent
CACHE_DIR   = SCRIPT_DIR.parent / "data" / "cache"
PRICE_CACHE = CACHE_DIR / "ETH-prices-1h.csv"
OUTPUTS_DIR = SCRIPT_DIR.parent / "outputs"
CHART_PATH  = OUTPUTS_DIR / "ETH-funding-backtest-pnl.png"

# ── Constants ──────────────────────────────────────────────────────────────────

BINANCE_URL    = "https://api.binance.com/api/v3/klines"
CANDLE_LIMIT   = 1_000          # max candles Binance returns per request
HOLD_HOURS     = 24             # hold duration in hours
MS_PER_HOUR    = 3_600_000      # milliseconds in one hour

# ── Price fetching ─────────────────────────────────────────────────────────────

def fetch_candles(start_ms: int, end_ms: int) -> list:
    """
    Fetch up to 1,000 hourly ETH/USDT candles from Binance between
    start_ms and end_ms. Returns raw list of kline arrays.
    Binance kline format: [open_time, open, high, low, close, volume, ...]
    """
    resp = requests.get(BINANCE_URL, params={
        "symbol":    "ETHUSDT",
        "interval":  "1h",
        "startTime": start_ms,
        "endTime":   end_ms,
        "limit":     CANDLE_LIMIT,
    }, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_all_prices(start_ms: int) -> pd.DataFrame:
    """
    Paginate through hourly ETH/USDT candles from start_ms to now in
    1,000-hour chunks using Binance. Returns a DataFrame indexed by
    timestamp_ms with a close_price column.
    """
    now_ms   = int(time.time() * 1000)
    all_rows = []
    cursor   = start_ms

    while cursor < now_ms:
        end = min(cursor + CANDLE_LIMIT * MS_PER_HOUR, now_ms)
        print(f"  Fetching candles {datetime.fromtimestamp(cursor/1000, tz=timezone.utc).date()} "
              f"-> {datetime.fromtimestamp(end/1000, tz=timezone.utc).date()} ...",
              end=" ", flush=True)

        candles = fetch_candles(cursor, end)
        print(f"{len(candles)} candles")

        for c in candles:
            # c[0] = open_time_ms, c[4] = close price
            all_rows.append({
                "timestamp_ms": int(c[0]),
                "close_price":  float(c[4]),
            })

        if not candles:
            break

        # Advance cursor past the last returned candle
        cursor = int(candles[-1][0]) + MS_PER_HOUR

    df = pd.DataFrame(all_rows).drop_duplicates("timestamp_ms").sort_values("timestamp_ms")
    df = df.set_index("timestamp_ms")
    return df


def load_prices() -> pd.DataFrame:
    """
    Load ETH hourly prices from local cache if it exists,
    otherwise fetch from Hyperliquid and save to cache.
    """
    if PRICE_CACHE.exists():
        print(f"Loading cached prices: {PRICE_CACHE}")
        df = pd.read_csv(PRICE_CACHE, index_col="timestamp_ms")
        return df

    # Fetch from the start of the funding rate history
    funding_start_ms = 1_683_849_600_000   # 2023-05-12 00:00 UTC
    print("Fetching ETH hourly prices from Hyperliquid...")
    df = fetch_all_prices(funding_start_ms)

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(PRICE_CACHE)
    print(f"Prices cached: {PRICE_CACHE}  ({len(df):,} candles)\n")
    return df


# ── Return calculation ─────────────────────────────────────────────────────────

def lookup_price(prices: pd.DataFrame, signal_ms: int) -> float | None:
    """
    Return the closest available close price at or just after signal_ms.
    Returns None if no price is found within a 2-hour tolerance.
    """
    tolerance_ms = 2 * MS_PER_HOUR
    # Find the nearest timestamp in the price index
    available = prices.index[(prices.index >= signal_ms) &
                              (prices.index <= signal_ms + tolerance_ms)]
    if available.empty:
        return None
    return float(prices.at[available[0], "close_price"])


def run_backtest(signal_df: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    """
    For each signal event, look up the ETH price at entry and 24h later.
    trade_return = signal × (exit_price / entry_price − 1)

    Trades where price data is unavailable at entry or exit are skipped.
    """
    trades = []

    for _, row in signal_df[signal_df["signal"] != 0].iterrows():
        sig        = row["signal"]
        entry_ms   = int(row["datetime"].timestamp() * 1000)
        exit_ms    = entry_ms + HOLD_HOURS * MS_PER_HOUR

        entry_price = lookup_price(prices, entry_ms)
        exit_price  = lookup_price(prices, exit_ms)

        if entry_price is None or exit_price is None:
            continue    # skip if price data is missing at either leg

        price_return  = (exit_price / entry_price) - 1
        trade_return  = sig * price_return  # positive when direction is correct

        trades.append({
            "entry_time":   row["datetime"],
            "signal":       sig,
            "entry_price":  entry_price,
            "exit_price":   exit_price,
            "price_return": price_return * 100,   # store as %
            "trade_return": trade_return * 100,   # store as %
            "win":          trade_return > 0,
        })

    return pd.DataFrame(trades)


# ── Metrics ────────────────────────────────────────────────────────────────────

def sharpe_ratio(returns: pd.Series, trades_per_year: float) -> float:
    """Annualised Sharpe ratio on per-trade returns (Rf = 0%)."""
    if returns.std() == 0:
        return float("nan")
    return (returns.mean() / returns.std()) * math.sqrt(trades_per_year)


def print_summary(trades: pd.DataFrame, signal_df: pd.DataFrame) -> None:
    """Print a formatted performance summary."""
    n            = len(trades)
    wins         = trades["win"].sum()
    win_rate     = wins / n * 100
    total_ret    = trades["trade_return"].sum()
    mean_ret     = trades["trade_return"].mean()
    best         = trades["trade_return"].max()
    worst        = trades["trade_return"].min()

    total_days      = (signal_df["datetime"].iloc[-1] - signal_df["datetime"].iloc[0]).days
    trades_per_year = n / (total_days / 365)
    sr              = sharpe_ratio(trades["trade_return"], trades_per_year)

    long_t  = trades[trades["signal"] ==  1]
    short_t = trades[trades["signal"] == -1]

    # Breakdown by direction
    def dir_summary(t):
        if t.empty:
            return "n/a"
        wr = t["win"].mean() * 100
        tr = t["trade_return"].sum()
        return f"{len(t)} trades | win rate {wr:.1f}% | total {tr:+.3f}%"

    print("=" * 60)
    print("  BACKTEST RESULTS - ETH Funding Rate Signal")
    print("  (actual 24h ETH price returns)")
    print("=" * 60)
    print(f"  Strategy     : Mean-reversion on extreme funding rates")
    print(f"  Hold period  : {HOLD_HOURS}h  |  Return: actual ETH price change")
    print(f"  Thresholds   : >{UPPER_THRESHOLD*100:.2f}% short | <{LOWER_THRESHOLD*100:.2f}% long")
    print("-" * 60)
    print(f"  Total trades : {n}")
    print(f"    Long  (+1) : {dir_summary(long_t)}")
    print(f"    Short (-1) : {dir_summary(short_t)}")
    print("-" * 60)
    print(f"  Total return : {total_ret:>+9.3f}%  (sum of all trade returns)")
    print(f"  Mean / trade : {mean_ret:>+9.3f}%")
    print(f"  Best trade   : {best:>+9.3f}%")
    print(f"  Worst trade  : {worst:>+9.3f}%")
    print("-" * 60)
    print(f"  Win rate     : {win_rate:>8.1f}%  ({int(wins)}/{n} trades profitable)")
    print(f"  Sharpe ratio : {sr:>8.3f}   (annualised, Rf = 0%)")
    print("=" * 60)


# ── Chart ──────────────────────────────────────────────────────────────────────

def plot_pnl(trades: pd.DataFrame) -> None:
    """Cumulative PnL chart with long/short markers."""
    trades = trades.sort_values("entry_time").copy()
    trades["cum_pnl"] = trades["trade_return"].cumsum()

    long_t  = trades[trades["signal"] ==  1]
    short_t = trades[trades["signal"] == -1]

    fig, ax = plt.subplots(figsize=(13, 5))

    ax.plot(trades["entry_time"], trades["cum_pnl"],
            color="#3b82f6", linewidth=1.4, label="Cumulative PnL")
    ax.axhline(0, color="grey", linewidth=0.7, linestyle="--")

    ax.scatter(long_t["entry_time"],  long_t["cum_pnl"],
               color="#22c55e", s=35, zorder=5, label=f"Long trade ({len(long_t)})")
    ax.scatter(short_t["entry_time"], short_t["cum_pnl"],
               color="#ef4444", s=15, zorder=5, label=f"Short trade ({len(short_t)})")

    ax.fill_between(trades["entry_time"], trades["cum_pnl"], 0,
                    where=trades["cum_pnl"] >= 0, color="#22c55e", alpha=0.08)
    ax.fill_between(trades["entry_time"], trades["cum_pnl"], 0,
                    where=trades["cum_pnl"] < 0,  color="#ef4444", alpha=0.08)

    ax.set_title("ETH Funding Rate Signal — Cumulative PnL (24h hold, actual price returns)",
                 fontsize=12, fontweight="bold")
    ax.set_ylabel("Cumulative Return (%)", fontsize=10)
    ax.set_xlabel("Trade entry date", fontsize=10)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.1f}%"))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    plt.xticks(rotation=30, ha="right")
    ax.legend(fontsize=9)
    ax.grid(axis="y", linestyle=":", alpha=0.4)

    plt.tight_layout()
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(CHART_PATH, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"\nChart saved: {CHART_PATH}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    # 1. Load funding rates and generate signal
    print(f"Reading funding rates: {CSV_PATH}")
    signal_df = pd.read_csv(CSV_PATH, parse_dates=["datetime"])
    signal_df["fundingRate"] = signal_df["fundingRate"].astype(float)
    signal_df = signal_df.sort_values("datetime").reset_index(drop=True)
    signal_df = generate_signal(signal_df)
    print(f"Signal fires: {(signal_df['signal'] != 0).sum()} times\n")

    # 2. Load (or fetch) ETH hourly prices
    prices = load_prices()

    # 3. Run backtest using actual price returns
    print("Running backtest...")
    trades = run_backtest(signal_df, prices)
    print(f"Completed: {len(trades)} trades executed\n")

    # 4. Print summary
    print_summary(trades, signal_df)

    # 5. Save chart
    plot_pnl(trades)


if __name__ == "__main__":
    main()
