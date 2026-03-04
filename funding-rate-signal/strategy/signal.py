"""
signal.py
---------
Reads the Hyperliquid ETH hourly funding rate CSV and generates a
mean-reversion signal based on extreme funding rate levels.

Signal logic
------------
  +1  (go long)   — funding rate < -0.01%  (shorts are paying longs;
                     market is overly short, expect mean reversion up)
  -1  (go short)  — funding rate > +0.01%  (longs are paying shorts;
                     market is overly long, expect mean reversion down)
   0  (no trade)  — funding rate is within the ±0.01% neutral band

The idea: extreme funding rates reflect crowded positioning. When
everyone is short (negative funding), the contrarian bet is to go long,
and vice versa.
"""

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────

SCRIPT_DIR  = Path(__file__).parent
DATA_DIR    = SCRIPT_DIR.parent / "data" / "cache"
CSV_PATH    = DATA_DIR / "ETH-funding-rates.csv"
OUTPUTS_DIR = SCRIPT_DIR.parent / "outputs"
CHART_PATH  = OUTPUTS_DIR / "ETH-funding-rate-signal.png"

# ── Thresholds ─────────────────────────────────────────────────────────────────

UPPER_THRESHOLD =  0.0001    # +0.01% — above this → signal = -1 (short)
LOWER_THRESHOLD = -0.0001    # -0.01% — below this → signal = +1 (long)

# ── Signal generation ──────────────────────────────────────────────────────────

def generate_signal(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a signal column to the dataframe.

      signal = +1  if fundingRate < LOWER_THRESHOLD
             = -1  if fundingRate > UPPER_THRESHOLD
             =  0  otherwise
    """
    df["signal"] = 0
    df.loc[df["fundingRate"] < LOWER_THRESHOLD, "signal"] = 1
    df.loc[df["fundingRate"] > UPPER_THRESHOLD, "signal"] = -1
    return df


# ── Summary ────────────────────────────────────────────────────────────────────

def print_summary(df: pd.DataFrame) -> None:
    """Print a breakdown of how often each signal fires."""
    total   = len(df)
    counts  = df["signal"].value_counts().sort_index()

    long_n  = counts.get( 1, 0)
    short_n = counts.get(-1, 0)
    flat_n  = counts.get( 0, 0)

    print("=" * 52)
    print("  FUNDING RATE SIGNAL SUMMARY — ETH (Hyperliquid)")
    print("=" * 52)
    print(f"  Total periods   : {total:,}  (8h intervals)")
    print(f"  Date range      : {df['datetime'].iloc[0]}  ->  {df['datetime'].iloc[-1]}")
    print(f"  Upper threshold : >{UPPER_THRESHOLD*100:.2f}%  (short signal)")
    print(f"  Lower threshold : <{LOWER_THRESHOLD*100:.2f}%  (long signal)")
    print("-" * 52)
    print(f"  Signal  +1 (long)  : {long_n:>5,} fires  ({long_n/total*100:.2f}% of periods)")
    print(f"  Signal  -1 (short) : {short_n:>5,} fires  ({short_n/total*100:.2f}% of periods)")
    print(f"  Signal   0 (flat)  : {flat_n:>5,} periods ({flat_n/total*100:.2f}% of periods)")
    print("=" * 52)


# ── Chart ──────────────────────────────────────────────────────────────────────

def plot_signal(df: pd.DataFrame) -> None:
    """
    Plot funding rate over time with signal events overlaid:
      - Blue line     : raw funding rate
      - Dashed lines  : ±0.01% thresholds
      - Green dots    : long signal (+1)
      - Red dots      : short signal (-1)
    """
    # Separate signal subsets for scatter overlay
    long_df  = df[df["signal"] ==  1]
    short_df = df[df["signal"] == -1]

    fig, ax = plt.subplots(figsize=(14, 5))

    # ── Funding rate line ──────────────────────────────────────────────────────
    ax.plot(df["datetime"], df["fundingRate"] * 100,
            color="#94a3b8", linewidth=0.6, alpha=0.8, label="Funding rate")

    # ── Threshold bands ────────────────────────────────────────────────────────
    ax.axhline( UPPER_THRESHOLD * 100, color="#ef4444", linewidth=0.9,
                linestyle="--", label=f"+{UPPER_THRESHOLD*100:.2f}% short threshold")
    ax.axhline( LOWER_THRESHOLD * 100, color="#22c55e", linewidth=0.9,
                linestyle="--", label=f"{LOWER_THRESHOLD*100:.2f}% long threshold")
    ax.axhline(0, color="grey", linewidth=0.5, linestyle=":")

    # ── Signal dots ───────────────────────────────────────────────────────────
    ax.scatter(long_df["datetime"],  long_df["fundingRate"]  * 100,
               color="#22c55e", s=18, zorder=5, label=f"Long signal ({len(long_df)})")
    ax.scatter(short_df["datetime"], short_df["fundingRate"] * 100,
               color="#ef4444", s=18, zorder=5, label=f"Short signal ({len(short_df)})")

    # ── Formatting ─────────────────────────────────────────────────────────────
    ax.set_title("ETH 8h Funding Rate — Mean Reversion Signal (Hyperliquid)",
                 fontsize=13, fontweight="bold")
    ax.set_ylabel("Funding Rate (%)", fontsize=10)
    ax.set_xlabel("Date", fontsize=10)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.3f}%"))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    plt.xticks(rotation=30, ha="right")
    ax.legend(fontsize=9, loc="upper right")
    ax.grid(axis="y", linestyle=":", alpha=0.4)

    plt.tight_layout()
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(CHART_PATH, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Chart saved: {CHART_PATH}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    # 1. Load CSV
    print(f"Reading: {CSV_PATH}\n")
    df = pd.read_csv(CSV_PATH, parse_dates=["datetime"])
    df["fundingRate"] = df["fundingRate"].astype(float)
    df = df.sort_values("datetime").reset_index(drop=True)

    # 2. Generate signal
    df = generate_signal(df)

    # 3. Print summary
    print_summary(df)

    # 4. Plot and save chart
    plot_signal(df)


if __name__ == "__main__":
    main()
