# tradedesk-dukascopy

Utilities to export Dukascopy tick data into backtest-ready datasets.

## Install

```bash
pip install tradedesk-dukascopy
```

## CLI

Export candles (default):

```bash
tradedesk-dc-export --symbol EURUSD --from 2024-01-01 --to 2024-01-31 --resample 5min --out eurusd_5m.csv
```

Export ticks:

```bash
tradedesk-dc-export --symbol EURUSD --from 2024-01-01 --to 2024-01-02 --format ticks --out eurusd_ticks.csv
```

## Output contract (v1)

- Timestamps are ISO-8601 UTC.
- Prices are scaled exactly once at export time via `--price-divisor` (default `1.0`).
- A sidecar metadata file is always written: `<output>.meta.json`.

## License
Licensed under the Apache License, Version 2.0
