# Polymarket Crypto ML Pipeline (Offline)

This workflow is fully offline from Homerun runtime. It downloads Polymarket crypto market NDJSON data, processes bars for `5m`, `15m`, `1h`, `4h`, and trains standalone ML artifacts.

## Prerequisite

Install Python dependencies (at minimum `numpy`):

```bash
python -m pip install -r backend/requirements.txt
```

## Script

- `scripts/build_polymarket_crypto_ml.py`

## Inputs

Default remote source:

- `trentmkelly/polymarket_crypto_derivatives` (Hugging Face)

Default assets:

- `btc,eth,sol,xrp`

## Outputs

Default output directory:

- `output/ml/polymarket_crypto`

Generated files:

- `output/ml/polymarket_crypto/download_manifest.json`
- `output/ml/polymarket_crypto/processed/minute_bars.csv.gz`
- `output/ml/polymarket_crypto/processed/bars_5m.csv.gz`
- `output/ml/polymarket_crypto/processed/bars_15m.csv.gz`
- `output/ml/polymarket_crypto/processed/bars_1h.csv.gz`
- `output/ml/polymarket_crypto/processed/bars_4h.csv.gz`
- `output/ml/polymarket_crypto/polymarket_crypto_ml_bundle.json`
- `output/ml/polymarket_crypto/run_summary.json`

## Run

From repo root:

```bash
python scripts/build_polymarket_crypto_ml.py --mode run
```

This does all three stages:

1. discover+download
2. process bars
3. train ML model bundle

## Download Only

```bash
python scripts/build_polymarket_crypto_ml.py --mode download
```

## Build Only (from local raw files)

```bash
python scripts/build_polymarket_crypto_ml.py --mode build
```

## Useful Flags

Limit file count while validating quickly:

```bash
python scripts/build_polymarket_crypto_ml.py --mode run --max-files-per-asset 25
```

Custom output location:

```bash
python scripts/build_polymarket_crypto_ml.py --mode run --output-dir output/ml/my_crypto_bundle
```

Custom datasets/assets/timeframes:

```bash
python scripts/build_polymarket_crypto_ml.py \
  --mode run \
  --dataset-ids trentmkelly/polymarket_crypto_derivatives \
  --assets btc,eth,sol,xrp \
  --timeframes 5,15,60,240
```

## Model Artifact Format

`polymarket_crypto_ml_bundle.json` stores:

- per-timeframe model weights (`5m`, `15m`, `1h`, `4h`)
- feature names
- scaler mean/std
- train/test metrics
- train/test date ranges

The artifact is independent of Homerun runtime and intended for later strategy integration.
