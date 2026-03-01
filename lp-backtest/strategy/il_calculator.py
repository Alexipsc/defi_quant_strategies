"""
il_calculator.py
----------------
Reads the USDC/WETH 0.05% pool daily data from cache, calculates
Impermanent Loss (IL) for each day relative to the pool's first day,
and saves a two-panel chart (price + IL) as a PNG.

IL formula
----------
Given:  price_ratio = P_t / P_0   (current price / initial price)

    IL = (2 * sqrt(price_ratio) / (1 + price_ratio)) - 1

IL == 0  → no divergence from the initial price; no impermanent loss.
IL == -1 → maximum theoretical loss (one token goes to zero).
The result is expressed as a percentage (multiplied by 100).

Note: token0Price in the CSV is USDC per WETH (i.e. the ETH/USD price).
"""

import math
import pandas as pd
import matplotlib
matplotlib.use("Agg")          # non-interactive backend — no display needed
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────

# Resolve paths relative to this file so the script works from any cwd
SCRIPT_DIR = Path(__file__).parent
CACHE_DIR  = SCRIPT_DIR.parent / "data" / "cache"
CSV_PATH   = CACHE_DIR / "USDC-WETH-005-pool-day-data.csv"
CHART_PATH = CACHE_DIR / "USDC-WETH-005-il-chart.png"

# ── IL calculation ─────────────────────────────────────────────────────────────

def calc_il(price_ratio: float) -> float:
    """
    Return the impermanent loss as a decimal for a given price ratio.

    IL = (2 * sqrt(r) / (1 + r)) - 1

    Always <= 0 (a loss relative to simply holding the two tokens).
    """
    return (2 * math.sqrt(price_ratio) / (1 + price_ratio)) - 1


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    # 1. Load CSV -----------------------------------------------------------------
    print(f"Reading: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH, parse_dates=["date"])

    # token0Price = USDC per WETH = ETH/USD price
    df = df.sort_values("date").reset_index(drop=True)

    # 2. Calculate IL -------------------------------------------------------------
    initial_price = df["token0Price"].iloc[0]
    print(f"Initial price (day 0, {df['date'].iloc[0].date()}): ${initial_price:,.2f} USDC/WETH")

    df["price_ratio"] = df["token0Price"] / initial_price
    df["il_pct"]      = df["price_ratio"].apply(calc_il) * 100   # express as %

    # Quick summary
    worst_il  = df["il_pct"].min()
    worst_day = df.loc[df["il_pct"].idxmin(), "date"].date()
    print(f"Worst IL: {worst_il:.2f}% on {worst_day}")
    print(f"Current IL ({df['date'].iloc[-1].date()}): {df['il_pct'].iloc[-1]:.2f}%")

    # 3. Plot ---------------------------------------------------------------------
    fig, (ax_price, ax_il) = plt.subplots(
        nrows=2,
        ncols=1,
        figsize=(13, 7),
        sharex=True,                  # both panels share the same x-axis
        gridspec_kw={"height_ratios": [2, 1]},
    )
    fig.suptitle("USDC/WETH 0.05% — Price & Impermanent Loss", fontsize=14, fontweight="bold")

    # ── Panel 1: ETH/USD price ────────────────────────────────────────────────
    ax_price.plot(df["date"], df["token0Price"], color="#3b82f6", linewidth=1.2, label="ETH/USD")
    ax_price.axhline(initial_price, color="grey", linewidth=0.8, linestyle="--", label=f"Entry price ${initial_price:,.0f}")
    ax_price.set_ylabel("Price (USDC / WETH)", fontsize=10)
    ax_price.legend(fontsize=9)
    ax_price.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax_price.grid(axis="y", linestyle=":", alpha=0.5)

    # ── Panel 2: Impermanent Loss % ───────────────────────────────────────────
    ax_il.fill_between(df["date"], df["il_pct"], 0, color="#ef4444", alpha=0.35, label="IL %")
    ax_il.plot(df["date"], df["il_pct"], color="#ef4444", linewidth=1.0)
    ax_il.axhline(0, color="grey", linewidth=0.8, linestyle="--")
    ax_il.set_ylabel("Impermanent Loss (%)", fontsize=10)
    ax_il.set_xlabel("Date", fontsize=10)
    ax_il.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.1f}%"))
    ax_il.grid(axis="y", linestyle=":", alpha=0.5)

    # Format x-axis dates nicely on the bottom panel
    ax_il.xaxis.set_major_locator(mdates.YearLocator())
    ax_il.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax_il.xaxis.set_minor_locator(mdates.MonthLocator(bymonth=[4, 7, 10]))

    plt.tight_layout()

    # 4. Save chart ---------------------------------------------------------------
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(CHART_PATH, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Chart saved: {CHART_PATH}")


if __name__ == "__main__":
    main()
