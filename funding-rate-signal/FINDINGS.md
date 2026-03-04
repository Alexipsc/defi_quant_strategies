# Funding Rate Signal — ETH Perpetual

## Strategy

Mean-reversion on extreme ETH perpetual funding rates. The hypothesis: when
funding is abnormally high (longs paying shorts), the market is crowded long
and likely to revert lower — and vice versa. A threshold breach generates a
directional signal; the trade is held for 24 hours using actual spot price
returns.

**Signal logic**

| Condition | Signal | Rationale |
|---|---|---|
| fundingRate > +threshold | −1 (short) | Crowded long; expect reversion down |
| fundingRate < −threshold | +1 (long) | Crowded short; expect reversion up |
| Otherwise | 0 (flat) | No trade |

**Return measure:** `trade_return = signal × (exit_price / entry_price − 1)`
Entry and exit prices are hourly ETH/USDT close prices from Binance spot
klines (`/api/v3/klines`), with a 2-hour lookup tolerance.

---

## Data

| Source | Exchange | Period | Records | Interval | Threshold tested |
|---|---|---|---|---|---|
| Hyperliquid public API | Hyperliquid perp | 2023-05-12 to 2026-03-04 | 24,069 | 8h | ±0.01% |
| Binance fAPI | Binance perp | 2020-01-01 to 2026-03-04 | 5,999 | 8h | ±0.05% |

Price data: 54,081 hourly ETH/USDT candles (Binance spot, 2020-01-01 to
2026-03-04), cached locally. No API key required for any data source.

---

## Results

| Metric | Hyperliquid (±0.01%) | Binance (±0.05%) |
|---|---|---|
| Period | 2023-05-12 → 2026-03-04 | 2020-01-01 → 2026-03-04 |
| Long signals | 40 (0.17%) | 11 (0.18%) |
| Short signals | 526 (2.19%) | 448 (7.47%) |
| Trades executed | 566 | 459 |
| Long trades | 40 \| win 47.5% \| total +3.6% | 11 \| win 63.6% \| total +16.5% |
| Short trades | 526 \| win 50.6% \| total −215.6% | 448 \| win 48.0% \| total −233.9% |
| Overall win rate | 50.4% | 48.4% |
| **Total return (sum)** | **−211.95%** | **−217.37%** |
| **Sharpe ratio (ann.)** | **−1.734** | **−0.767** |

---

## Findings

**Short signals carry no edge.** Despite testing two exchanges, two threshold
levels, and a combined 6-year window, shorting after extreme positive funding
delivers ~48–51% win rates with a large negative tail. ETH price over 24 hours
does not reliably revert after elevated funding — momentum, macro events, and
leverage cascades all introduce noise that overwhelms the signal.

**Long signals show directional promise but are statistically insufficient.**
The 11 Binance long trades (funding < −0.05%, concentrated in 2020–2022) won
63.6% of the time for a combined +16.5% return. These episodes — deeply
negative funding driven by crowded short sellers — did tend to resolve upward.
However, 11 observations are not enough to derive statistical confidence or
to trade a live strategy.

**Threshold level matters.** Raising the threshold from ±0.01% to ±0.05%
halved the Sharpe drawdown (−1.734 → −0.767) by filtering out noise-level
signals, confirming that only truly extreme funding carries information.

---

## Conclusion

The simple funding rate mean-reversion strategy is **not viable in its current
form**. The short signal, which drives 94% of all trades, shows no directional
edge over a 24-hour hold. The long signal warrants further investigation but
requires a longer live history (Hyperliquid has produced only 40 long signals
in ~3 years at ±0.01%).

Possible extensions that may improve performance:

- **Trade long signals only** — filter out the short side entirely.
- **Add a trend filter** — only enter short signals when price is below a
  moving average, avoiding shorting into uptrends.
- **Shorten the hold window** — funding mean-reversion, if it exists, may
  resolve within 4–8 hours rather than 24.
- **Position sizing by magnitude** — scale size proportionally to how far
  funding exceeds the threshold rather than using a fixed binary signal.
