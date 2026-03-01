"""
lp_simulate.py
--------------
Simulates providing $10,000 of liquidity to the USDC/WETH 0.05% Uniswap v3
pool from inception, tracking pool value, fee earnings, and HODL comparison.

Math overview
-------------
We model the position as a full-range (v2-equivalent) LP, which gives exact
closed-form expressions for pool value and IL.

Deposit at price P0, capital C = $10,000, split 50/50:
  x0  = C / (2 * P0)   — ETH held
  y0  = C / 2           — USDC held
  k   = x0 * y0        — constant product

At price Pt (price_ratio r = Pt / P0):
  pool_value = C * sqrt(r)            (derived from the constant-product curve)
  hodl_value = C/2 * (r + 1)         (just holding the original tokens)
  IL         = pool_value / hodl_value - 1   = 2*sqrt(r)/(r+1) - 1  ✓

Fee accrual (daily):
  our_share      = C / tvlUSD         (our fraction of pool liquidity by value)
  our_daily_fee  = feesUSD * our_share

This is a simplification: in practice v3 concentrated positions earn fees
proportional to their in-range liquidity share, not total TVL share.  For a
full-range position it is a reasonable first approximation.
"""

import pandas as pd
import matplotlib
matplotlib.use("Agg")          # non-interactive backend — no display needed
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path

# ── Config ─────────────────────────────────────────────────────────────────────

INITIAL_CAPITAL = 10_000        # USD deposited on day 0

SCRIPT_DIR = Path(__file__).parent
CACHE_DIR  = SCRIPT_DIR.parent / "data" / "cache"
CSV_PATH   = CACHE_DIR / "USDC-WETH-005-pool-day-data.csv"
CHART_PATH = CACHE_DIR / "USDC-WETH-005-lp-simulation.png"

# ── Simulation ─────────────────────────────────────────────────────────────────

def run_simulation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add simulation columns to the dataframe and return it.

    New columns:
      our_fee_usd    — fee income earned on this day
      cum_fees       — cumulative fees earned so far
      pool_value     — current market value of the LP position (ex-fees)
      hodl_value     — value of simply holding the initial tokens
      net_value      — pool_value + cum_fees  (what we can actually withdraw)
      net_pnl        — net_value - INITIAL_CAPITAL
    """
    C  = INITIAL_CAPITAL
    r  = df["price_ratio"]          # Pt / P0 for each day

    # Pool value after IL (constant-product formula)
    df["pool_value"] = C * r.pow(0.5)

    # HODL value: hold x0 ETH + y0 USDC through price changes
    df["hodl_value"] = (C / 2) * (r + 1)

    # Daily fee share — our pro-rata slice of the pool's fee revenue
    df["our_fee_usd"] = df["feesUSD"] * (C / df["tvlUSD"])

    # Cumulative fees (running total)
    df["cum_fees"] = df["our_fee_usd"].cumsum()

    # Net withdrawable value = LP position + all fees collected
    df["net_value"] = df["pool_value"] + df["cum_fees"]

    # Net PnL vs initial deposit
    df["net_pnl"] = df["net_value"] - C

    return df


# ── Chart ──────────────────────────────────────────────────────────────────────

def plot_simulation(df: pd.DataFrame) -> None:
    """
    Three-line chart:
      - Net LP value (pool value + cumulative fees)  — blue
      - HODL value                                   — orange
      - Initial investment flat line                 — grey dashed
    """
    fig, ax = plt.subplots(figsize=(13, 6))

    ax.plot(df["date"], df["net_value"],  color="#3b82f6", linewidth=1.4,
            label="LP value (pool + fees)")
    ax.plot(df["date"], df["hodl_value"], color="#f97316", linewidth=1.4,
            label="HODL value")
    ax.axhline(INITIAL_CAPITAL, color="grey", linewidth=0.9, linestyle="--",
               label=f"Initial investment ${INITIAL_CAPITAL:,}")

    # Shade area between LP and HODL to make out/under-performance visible
    ax.fill_between(
        df["date"],
        df["net_value"],
        df["hodl_value"],
        where=df["net_value"] >= df["hodl_value"],
        alpha=0.15, color="#3b82f6", label="LP outperforms HODL",
    )
    ax.fill_between(
        df["date"],
        df["net_value"],
        df["hodl_value"],
        where=df["net_value"] < df["hodl_value"],
        alpha=0.15, color="#f97316", label="HODL outperforms LP",
    )

    ax.set_title("USDC/WETH 0.05% LP Simulation — $10,000 entry at inception",
                 fontsize=13, fontweight="bold")
    ax.set_ylabel("Portfolio value (USD)", fontsize=10)
    ax.set_xlabel("Date", fontsize=10)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.xaxis.set_minor_locator(mdates.MonthLocator(bymonth=[4, 7, 10]))
    ax.legend(fontsize=9)
    ax.grid(axis="y", linestyle=":", alpha=0.5)

    plt.tight_layout()
    fig.savefig(CHART_PATH, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Chart saved: {CHART_PATH}")


# ── Summary ────────────────────────────────────────────────────────────────────

def print_summary(df: pd.DataFrame) -> None:
    """Print a final performance summary to the console."""
    C = INITIAL_CAPITAL

    first = df.iloc[0]
    last  = df.iloc[-1]

    total_fees    = last["cum_fees"]
    final_pool    = last["pool_value"]
    final_net     = last["net_value"]
    final_hodl    = last["hodl_value"]
    il_impact     = final_pool - final_hodl  + (final_hodl - C) * 0  # see note below
    # IL impact = what IL cost us vs a HODL position (pool value vs hodl, before fees)
    il_impact     = final_pool - final_hodl
    net_return_pct  = (final_net  - C) / C * 100
    hodl_return_pct = (final_hodl - C) / C * 100
    lp_vs_hodl      = final_net - final_hodl

    print("\n" + "=" * 55)
    print(f"  LP SIMULATION SUMMARY - USDC/WETH 0.05%")
    print("=" * 55)
    print(f"  Period          : {first['date'].date()}  ->  {last['date'].date()}")
    print(f"  Days simulated  : {len(df)}")
    print(f"  Initial capital : ${C:>10,.2f}")
    print("-" * 55)
    print(f"  Final pool value: ${final_pool:>10,.2f}  (ex-fees)")
    print(f"  Total fees earned: ${total_fees:>9,.2f}")
    print(f"  Net LP value    : ${final_net:>10,.2f}  (pool + fees)")
    print("-" * 55)
    print(f"  HODL value      : ${final_hodl:>10,.2f}")
    print("-" * 55)
    print(f"  IL impact       : ${il_impact:>10,.2f}  (pool value vs HODL)")
    print(f"  Fees offset IL  : ${total_fees + il_impact:>10,.2f}")
    print("-" * 55)
    print(f"  Net LP return   : {net_return_pct:>+10.2f}%")
    print(f"  HODL return     : {hodl_return_pct:>+10.2f}%")
    print(f"  LP vs HODL      : ${lp_vs_hodl:>+10,.2f}  ({'LP wins' if lp_vs_hodl >= 0 else 'HODL wins'})")
    print("=" * 55)


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    # 1. Load data ----------------------------------------------------------------
    print(f"Reading: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH, parse_dates=["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # 2. Run simulation -----------------------------------------------------------
    df = run_simulation(df)

    # 3. Print summary ------------------------------------------------------------
    print_summary(df)

    # 4. Save chart ---------------------------------------------------------------
    plot_simulation(df)


if __name__ == "__main__":
    main()
