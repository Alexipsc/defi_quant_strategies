"""
metrics.py
----------
Reads the USDC/WETH 0.05% pool CSV, runs the LP simulation via lp_simulate,
and calculates three risk/return metrics:

  1. Sharpe Ratio   — annualised, risk-free rate = 0%
  2. Max Drawdown   — worst peak-to-trough drop in net LP value (pool + fees)
  3. Fee APY        — annualised fee yield on the initial capital

Imports run_simulation() directly from lp_simulate so no logic is duplicated.
"""

import math
import pandas as pd
from pathlib import Path

# Add the strategy folder to sys.path so the import works regardless of cwd
import sys
sys.path.insert(0, str(Path(__file__).parent))
from lp_simulate import run_simulation, INITIAL_CAPITAL, CSV_PATH

# ── Metric calculations ────────────────────────────────────────────────────────

def sharpe_ratio(net_values: pd.Series) -> float:
    """
    Annualised Sharpe ratio using daily LP net value (pool + fees).
    Risk-free rate assumed to be 0%.

    daily_return = (V_t - V_{t-1}) / V_{t-1}
    Sharpe       = mean(daily_return) * sqrt(365) / std(daily_return)

    Multiplying mean by 365 and std by sqrt(365) both scale linearly,
    so the ratio simplifies to mean/std * sqrt(365).
    """
    daily_returns = net_values.pct_change().dropna()
    mean_r = daily_returns.mean()
    std_r  = daily_returns.std()

    if std_r == 0:
        return float("nan")

    return (mean_r / std_r) * math.sqrt(365)


def max_drawdown(net_values: pd.Series) -> float:
    """
    Maximum drawdown: the largest peak-to-trough decline in net LP value,
    expressed as a percentage.

    rolling_peak  = highest net_value seen up to day t
    drawdown_t    = (net_value_t - rolling_peak_t) / rolling_peak_t
    max_drawdown  = min(drawdown_t)   ← most negative value
    """
    rolling_peak = net_values.cummax()          # running all-time high
    drawdown     = (net_values - rolling_peak) / rolling_peak
    return drawdown.min() * 100                 # return as %


def fee_apy(df: pd.DataFrame) -> float:
    """
    Annualised fee yield on the initial capital.

    total_fees  = sum of all daily fee income earned by our position
    num_days    = number of days in the simulation
    fee_apy     = (total_fees / INITIAL_CAPITAL) * (365 / num_days) * 100

    This answers: "if the fee rate were constant, what annual % return
    would fees alone provide on the original $10,000 deposit?"
    """
    total_fees = df["cum_fees"].iloc[-1]
    num_days   = len(df)
    return (total_fees / INITIAL_CAPITAL) * (365 / num_days) * 100


# ── Helpers ────────────────────────────────────────────────────────────────────

def print_metrics(df: pd.DataFrame) -> None:
    """Calculate and print a formatted summary table of all three metrics."""

    sr  = sharpe_ratio(df["net_value"])
    mdd = max_drawdown(df["net_value"])
    apy = fee_apy(df)

    # Additional context values for the table
    total_fees   = df["cum_fees"].iloc[-1]
    net_return   = (df["net_value"].iloc[-1] - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100
    hodl_return  = (df["hodl_value"].iloc[-1] - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100
    il_impact    = df["pool_value"].iloc[-1] - df["hodl_value"].iloc[-1]
    num_days     = len(df)
    start        = df["date"].iloc[0].date()
    end          = df["date"].iloc[-1].date()

    print("\n" + "=" * 52)
    print("  PERFORMANCE METRICS - USDC/WETH 0.05% LP")
    print("=" * 52)
    print(f"  Period        : {start}  ->  {end}")
    print(f"  Days          : {num_days}  ({num_days / 365:.1f} years)")
    print(f"  Capital       : ${INITIAL_CAPITAL:,.0f}")
    print("-" * 52)

    # ── Return metrics ─────────────────────────────────────────────────────
    print(f"  Net LP return : {net_return:>+8.2f}%  (pool value + fees)")
    print(f"  HODL return   : {hodl_return:>+8.2f}%  (hold initial tokens)")
    print(f"  IL impact     : ${il_impact:>+9,.2f}  (pool value vs HODL)")
    print(f"  Total fees    : ${total_fees:>9,.2f}")
    print("-" * 52)

    # ── Risk metrics ───────────────────────────────────────────────────────
    print(f"  Sharpe ratio  : {sr:>8.3f}   (annualised, Rf = 0%)")
    print(f"  Max drawdown  : {mdd:>+8.2f}%  (peak-to-trough, net value)")
    print(f"  Fee APY       : {apy:>8.2f}%  (annualised fee yield)")
    print("=" * 52)


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    # 1. Load CSV
    print(f"Reading: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH, parse_dates=["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # 2. Run simulation to get pool_value, hodl_value, cum_fees, net_value
    df = run_simulation(df)

    # 3. Print metrics table
    print_metrics(df)


if __name__ == "__main__":
    main()
