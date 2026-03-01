# LP Backtest — USDC/WETH 0.05% (Uniswap v3)

## Strategy

Full-range liquidity provision on the USDC/WETH 0.05% fee-tier pool on Uniswap v3 (Ethereum mainnet).
A $10,000 deposit is modelled at pool inception, split 50/50 between USDC and WETH, and held without rebalancing or range adjustment for the full period.

| Parameter | Value |
|---|---|
| Pool address | `0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640` |
| Fee tier | 0.05% |
| Initial capital | $10,000 |
| Entry price | $3,496 USDC/WETH |
| Position type | Full-range (v2-equivalent) |

## Data Source

- **Pool daily data:** Uniswap v3 subgraph via The Graph decentralised network
- **Fields used:** `token0Price`, `volumeUSD`, `feesUSD`, `tvlUSD` (daily aggregates)
- **Date range:** 2021-05-05 → 2026-02-28 (1,761 days / ~4.8 years)

## Results

| Metric | Value |
|---|---|
| Net LP return | **+94.34%** |
| HODL return | −23.28% |
| LP vs HODL | +$11,762 |
| Total fees earned | $12,124 |
| Final pool value (ex-fees) | $7,310 |
| IL impact | −$362 |
| **Sharpe ratio** (annualised, Rf = 0%) | **0.733** |
| **Max drawdown** (net LP value) | **−25.47%** |
| **Fee APY** (annualised) | **25.13%** |

## Interpretation

Fee income was the dominant performance driver: the 0.05% pool generated ~$12,124 in cumulative fees against a $10,000 deposit, more than compensating for a −23% decline in ETH price over the period.
Impermanent loss was surprisingly contained at −$362 because ETH fell significantly from the entry price — when both assets decline together, the LP rebalancing mechanism works in the LP's favour relative to a directional HODL position.
A Sharpe ratio of 0.733 reflects a reasonable risk-adjusted outcome for a passive, unmanaged on-chain strategy, though the −25.47% max drawdown (concentrated in the 2022 bear market) highlights that fee income alone does not fully hedge against sharp ETH price dislocations.

## Caveats

- Pool value is modelled using the full-range constant-product formula; actual v3 concentrated positions would earn higher fees but carry out-of-range risk.
- Fee share is approximated as `$10,000 / daily tvlUSD`; this ignores liquidity concentration effects in v3.
- Gas costs, slippage on entry/exit, and reinvestment of fees are not modelled.
