# Portable Strategy Research (2026-02-17)

This note records external strategy patterns reviewed for direct DB-hosted Python portability.

## Opportunity Strategy Patterns (Firehose -> Executable Opportunity)

### Integrated now
- Flash crash reversion
  - Source pattern: abrupt short-window drop detection + rebound target.
  - Repo reference: https://github.com/discountry/polymarket-trading-bot (flash crash + price tracker patterns).
  - Homerun strategy: `flash_crash_reversion`.
- Tail-end carry
  - Source pattern: high-probability near-expiry filtering with tight risk gating.
  - Repo reference: https://github.com/Anmoldureha/polymarket-trading-bot-strategies (`tail_end_strategy`).
  - Homerun strategy: `tail_end_carry`.
- Spread dislocation
  - Source pattern: wide bid/ask dislocation filtering and partial spread-capture targets.
  - Repo reference: https://github.com/Anmoldureha/polymarket-trading-bot-strategies (`spread_scalping`, `micro_spreads`).
  - Homerun strategy: `spread_dislocation`.

### Reviewed but not directly ported
- Multi-leg execution bots that tightly couple to exchange order APIs/state machines (high adaptation cost).
- Low-quality/noisy repos with broken arbitrage math or invalid return paths.

## Recommended Next Ports

- Market-maker quote skew + inventory guards
  - Candidate: https://github.com/lorine93s/polymarket-market-maker-bot
  - Fit: opportunity-side spread/inventory opportunity filters + autotrader maker execution heuristics.
- Cross-venue strike parity checker for BTC event contracts
  - Candidate: https://github.com/CarlosIbCu/polymarket-kalshi-btc-arbitrage-bot
  - Fit: scanner-side cross-platform candidate generation with strict symbol/strike normalization.
- Generic alpha signal bus adapters
  - Candidate: https://github.com/chainstacklabs/polymarket-alpha-bot
  - Fit: autotrader-side source adapters and multi-signal confidence aggregation.

## Trust / Hygiene Notes

- Many repos in this space are no-license (`NOASSERTION`) forks with minimal validation.
- Community and exchange warning signals were observed for some “arbitrage bot” distributions:
  - Reddit community warning thread: https://www.reddit.com/r/Polymarket/comments/1ll6kkb/malware_warning_for_unofficial_polymarket/
  - KuCoin scam warning article: https://www.kucoin.com/news/articles/alert-warning-scam-polymarket-arbitrage-bot-drains-wallets

## AutoTrader Strategy Patterns (Signal -> Selected/Skipped/Blocked)

### Integrated now
- Scanner flash-reversion execution
  - Homerun strategy: `opportunity_flash_reversion`.
  - Uses live 5m move alignment + risk/liquidity gates.
- Scanner tail-carry execution
  - Homerun strategy: `opportunity_tail_carry`.
  - Uses entry-probability and resolution-window gating.
- Crypto spike-reversion execution
  - Homerun strategy: `crypto_spike_reversion`.
  - Uses live 5m/30m/2h shape checks inspired by spike bots.

## Sizing Policy (Not Strategy Type)

- Kelly/adaptive sizing integrated as a policy helper in trader strategy sizing:
  - `backend/services/trader_orchestrator/strategies/sizing.py`
- This keeps Kelly as **position sizing logic**, not a standalone strategy.

## Primary external repositories reviewed
- https://github.com/discountry/polymarket-trading-bot
- https://github.com/Anmoldureha/polymarket-trading-bot-strategies
- https://github.com/dexorynlabs/polymarket-copy-trading-bot-v2.0
- https://github.com/ChurchE2CB/polymarket-arbitrage-bot
- https://github.com/CarlosIbCu/polymarket-kalshi-btc-arbitrage-bot
