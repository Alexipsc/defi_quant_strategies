"""
compare_backtest.py
-------------------
Runs the 24h ETH price-return backtest on two datasets with different
threshold settings and prints a side-by-side comparison table.

  Dataset A : Hyperliquid  (2023-05-12 onwards)  thresholds ±0.01%
  Dataset B : Binance      (2020-01-01 onwards)  thresholds ±0.05%

Both datasets share the same cached ETH hourly price file
(ETH-prices-1h.csv) which was fetched from Binance spot klines.
"""

import math
import pandas as pd
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent))

# ── Paths ───────────────────────────────────────────────────────────────────

SCRIPT_DIR  = Path(__file__).parent
CACHE_DIR   = SCRIPT_DIR.parent / "data" / "cache"

HL_CSV      = CACHE_DIR / "ETH-funding-rates.csv"        # Hyperliquid
BN_CSV      = CACHE_DIR / "ETH-binance-funding-rates.csv" # Binance

PRICE_CACHE = CACHE_DIR / "ETH-prices-1h.csv"

# ── Constants ────────────────────────────────────────────────────────────────

HOLD_HOURS   = 24
MS_PER_HOUR  = 3_600_000

# ── Signal ───────────────────────────────────────────────────────────────────

def apply_signal(df: pd.DataFrame, rate_col: str,
                 upper: float, lower: float) -> pd.DataFrame:
    df = df.copy()
    df["signal"] = 0
    df.loc[df[rate_col].astype(float) > upper, "signal"] = -1
    df.loc[df[rate_col].astype(float) < lower, "signal"] =  1
    return df


# ── Price lookup ─────────────────────────────────────────────────────────────

def load_prices() -> pd.DataFrame:
    df = pd.read_csv(PRICE_CACHE, index_col="timestamp_ms")
    return df


def lookup_price(prices: pd.DataFrame, signal_ms: int):
    tol = 2 * MS_PER_HOUR
    avail = prices.index[(prices.index >= signal_ms) &
                         (prices.index <= signal_ms + tol)]
    if avail.empty:
        return None
    return float(prices.at[avail[0], "close_price"])


# ── Backtest ─────────────────────────────────────────────────────────────────

def run_backtest(signal_df: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    trades = []
    for _, row in signal_df[signal_df["signal"] != 0].iterrows():
        sig      = row["signal"]
        entry_ms = int(row["datetime"].timestamp() * 1000)
        exit_ms  = entry_ms + HOLD_HOURS * MS_PER_HOUR

        ep = lookup_price(prices, entry_ms)
        xp = lookup_price(prices, exit_ms)
        if ep is None or xp is None:
            continue

        pr = (xp / ep) - 1
        tr = sig * pr

        trades.append({
            "entry_time":   row["datetime"],
            "signal":       sig,
            "entry_price":  ep,
            "exit_price":   xp,
            "trade_return": tr * 100,
            "win":          tr > 0,
        })
    return pd.DataFrame(trades)


# ── Metrics ──────────────────────────────────────────────────────────────────

def calc_metrics(trades: pd.DataFrame, signal_df: pd.DataFrame) -> dict:
    if trades.empty:
        return {}
    n            = len(trades)
    long_t       = trades[trades["signal"] ==  1]
    short_t      = trades[trades["signal"] == -1]
    wins         = trades["win"].sum()
    total_ret    = trades["trade_return"].sum()
    total_days   = (signal_df["datetime"].iloc[-1] - signal_df["datetime"].iloc[0]).days
    tpy          = n / (total_days / 365) if total_days else 0
    std          = trades["trade_return"].std()
    sr           = (trades["trade_return"].mean() / std * math.sqrt(tpy)
                    if std != 0 else float("nan"))

    def dir_stats(t):
        if t.empty:
            return {"n": 0, "win_rate": float("nan"), "total_ret": 0.0}
        return {
            "n":         len(t),
            "win_rate":  t["win"].mean() * 100,
            "total_ret": t["trade_return"].sum(),
        }

    return {
        "total_trades": n,
        "long":         dir_stats(long_t),
        "short":        dir_stats(short_t),
        "win_rate":     wins / n * 100,
        "total_ret":    total_ret,
        "sharpe":       sr,
    }


# ── Comparison table ─────────────────────────────────────────────────────────

def print_table(hl: dict, bn: dict,
                hl_sig_df: pd.DataFrame, bn_sig_df: pd.DataFrame) -> None:

    def sig_counts(df):
        return {
            "long":  (df["signal"] ==  1).sum(),
            "short": (df["signal"] == -1).sum(),
            "flat":  (df["signal"] ==  0).sum(),
            "total": len(df),
        }

    hc = sig_counts(hl_sig_df)
    bc = sig_counts(bn_sig_df)

    def fmt_dir(d):
        if d["n"] == 0:
            return "0 trades | n/a"
        return (f"{d['n']} trades | "
                f"win {d['win_rate']:.1f}% | "
                f"total {d['total_ret']:+.2f}%")

    W = 34
    print()
    print("=" * 78)
    print("  BACKTEST COMPARISON")
    print("  Hyperliquid (2023-05-12+, thresholds +-0.01%)  vs  "
          "Binance (2020-01-01+, thresholds +-0.05%)")
    print("=" * 78)
    print(f"  {'Metric':<28} {'Hyperliquid':>{W}} {'Binance':>{W}}")
    print("-" * 78)

    def row(label, hv, bv):
        print(f"  {label:<28} {hv:>{W}} {bv:>{W}}")

    row("Period start",
        "2023-05-12", "2020-01-01")
    row("Total funding records",
        f"{hc['total']:,}", f"{bc['total']:,}")
    row("Upper threshold (short)",
        "+0.01%", "+0.05%")
    row("Lower threshold (long)",
        "-0.01%", "-0.05%")
    print("-" * 78)
    row("Long signals (+1)",
        f"{hc['long']:,}  ({hc['long']/hc['total']*100:.2f}%)",
        f"{bc['long']:,}  ({bc['long']/bc['total']*100:.2f}%)")
    row("Short signals (-1)",
        f"{hc['short']:,}  ({hc['short']/hc['total']*100:.2f}%)",
        f"{bc['short']:,}  ({bc['short']/bc['total']*100:.2f}%)")
    row("Flat (no trade)",
        f"{hc['flat']:,}  ({hc['flat']/hc['total']*100:.2f}%)",
        f"{bc['flat']:,}  ({bc['flat']/bc['total']*100:.2f}%)")
    print("-" * 78)
    row("Total trades executed",
        f"{hl['total_trades']}", f"{bn['total_trades']}")
    row("Long trades",
        fmt_dir(hl["long"]), fmt_dir(bn["long"]))
    row("Short trades",
        fmt_dir(hl["short"]), fmt_dir(bn["short"]))
    print("-" * 78)
    row("Overall win rate",
        f"{hl['win_rate']:.1f}%", f"{bn['win_rate']:.1f}%")
    row("Total return (sum)",
        f"{hl['total_ret']:+.2f}%", f"{bn['total_ret']:+.2f}%")
    row("Sharpe ratio (ann.)",
        f"{hl['sharpe']:.3f}", f"{bn['sharpe']:.3f}")
    print("=" * 78)


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    prices = load_prices()
    print(f"Price cache loaded: {len(prices):,} hourly candles")

    # ── Hyperliquid: ±0.01% thresholds ──────────────────────────────────────
    print("\nLoading Hyperliquid funding rates...")
    hl_raw = pd.read_csv(HL_CSV, parse_dates=["datetime"])
    hl_raw["fundingRate"] = hl_raw["fundingRate"].astype(float)
    hl_raw = hl_raw.sort_values("datetime").reset_index(drop=True)
    hl_sig = apply_signal(hl_raw, "fundingRate",  upper=0.0001, lower=-0.0001)
    print(f"  Records : {len(hl_sig):,}  |  "
          f"Long: {(hl_sig['signal']==1).sum()}  "
          f"Short: {(hl_sig['signal']==-1).sum()}")

    print("  Running Hyperliquid backtest...")
    hl_trades = run_backtest(hl_sig, prices)
    print(f"  Trades executed: {len(hl_trades)}")
    hl_metrics = calc_metrics(hl_trades, hl_sig)

    # ── Binance: ±0.05% thresholds ──────────────────────────────────────────
    print("\nLoading Binance funding rates...")
    bn_raw = pd.read_csv(BN_CSV, parse_dates=["datetime"])
    bn_raw["fundingRate"] = bn_raw["fundingRate"].astype(float)
    bn_raw = bn_raw.sort_values("datetime").reset_index(drop=True)
    bn_sig = apply_signal(bn_raw, "fundingRate", upper=0.0005, lower=-0.0005)
    print(f"  Records : {len(bn_sig):,}  |  "
          f"Long: {(bn_sig['signal']==1).sum()}  "
          f"Short: {(bn_sig['signal']==-1).sum()}")

    print("  Running Binance backtest...")
    bn_trades = run_backtest(bn_sig, prices)
    print(f"  Trades executed: {len(bn_trades)}")
    bn_metrics = calc_metrics(bn_trades, bn_sig)

    # ── Print comparison ─────────────────────────────────────────────────────
    print_table(hl_metrics, bn_metrics, hl_sig, bn_sig)


if __name__ == "__main__":
    main()
