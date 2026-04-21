# tradedesk-dukascopy

![CI Build](https://github.com/radiusred/tradedesk-dukascopy/actions/workflows/ci.yml/badge.svg)
[![PyPI Version](https://img.shields.io/pypi/v/tradedesk-dukascopy?label=PyPI)](https://pypi.python.org/pypi/tradedesk-dukascopy)

Dukascopy tick downloader and candle exporter for use in backtesting your trading strategies.

![loop](https://i.ibb.co/BSz1JSH/tradedesk-dukascopy.gif)

This tool downloads raw tick data from Dukascopy, converts it into clean,
deterministic CSV candle files, and writes a metadata sidecar describing exactly
how the data was produced.

It is designed to be run once per dataset, not repeatedly during backtests.


---

## Quick start

Install:

```bash
pip install tradedesk-dukascopy
```

Export 5-minute candles for EURUSD:

```bash
tradedesk-dc-export --symbols EURUSD \
  --from 2025-01-01 --to 2025-01-31 \
  --resample 5min \
  --out data \
  --cache-dir /paperclip/tradedesk/marketdata \
  --price-divisor 1000 \
  --workers 1
```

This produces:

```text
data/
  EURUSD_5MIN_bid.csv
  EURUSD_5MIN_bid.csv.meta.json
  EURUSD_5MIN_ask.csv
  EURUSD_5MIN_ask.csv.meta.json
```

You can now point your backtest engine at the bid or ask CSV directly, depending
on which price side you want to replay.

---

## Price scaling (`--price-divisor`)

Dukascopy tick prices are stored as integers or scaled values depending on the
instrument.

This tool applies **price scaling once, at export time**, using `--price-divisor`.

Examples:

| Instrument | Typical divisor |
|----------|-----------------|
| EURUSD   | `1000` |
| USDJPY  | `100000` |
| Indices | `1` or `10` |

If unsure, use probe mode:

```bash
tradedesk-dc-export --symbols GBPSEK \
  --from 2025-07-01 --to 2025-07-01 \
  --probe
```

Probe mode prints sample ticks at different divisors without writing files.

```text
GBPSEK: detected tick price format = int
GBPSEK @ 2025-07-01T00:00:00+00:00 (int): first 10 ticks
first tick raw: 2025-07-01T00:00:00.326000+00:00 bid_i 1297675 ask_i 1298619 vol 1.149999976158142
  divisor      1: bid 1297675.000000 ask 1298619.000000
  divisor     10: bid 129767.500000 ask 129861.900000
  divisor    100: bid 12976.750000 ask 12986.190000
  divisor   1000: bid 1297.675000 ask 1298.619000
  divisor  10000: bid 129.767500 ask 129.861900
  divisor 100000: bid 12.976750 ask 12.986190
using --price-divisor 1.0:
2025-07-01T00:00:00.326000+00:00 bid 1297675.0 ask 1298619.0 bid_vol 1.149999976158142
2025-07-01T00:00:01.128000+00:00 bid 1297800.0 ask 1298661.0 bid_vol 0.9200000166893005
2025-07-01T00:00:01.329000+00:00 bid 1297796.0 ask 1298621.0 bid_vol 0.9200000166893005
2025-07-01T00:00:03.335000+00:00 bid 1297796.0 ask 1298591.0 bid_vol 0.9200000166893005
2025-07-01T00:00:03.737000+00:00 bid 1297842.0 ask 1298695.0 bid_vol 1.149999976158142
2025-07-01T00:00:05.340000+00:00 bid 1297850.0 ask 1298655.0 bid_vol 0.9200000166893005
2025-07-01T00:00:06.542000+00:00 bid 1297862.0 ask 1298709.0 bid_vol 0.9200000166893005
2025-07-01T00:00:08.546000+00:00 bid 1297874.0 ask 1298709.0 bid_vol 0.9200000166893005
2025-07-01T00:00:10.556000+00:00 bid 1297877.0 ask 1298724.0 bid_vol 0.9200000166893005
2025-07-01T00:00:12.562000+00:00 bid 1297839.0 ask 1298684.0 bid_vol 1.149999976158142
```

---

## Intended workflow

This tool is intended to be used as a **data preparation step**, not as part of
your backtest runtime loop:

1. Download and export historical data once
2. Commit or archive the output CSV + metadata if applicable
3. Run fast, deterministic backtests against local files

---

## Output files and `--cache-dir`

When run, the tool will fetch new or missing raw data files from Dukascopy for the instrument(s) and periods that you specify. These are always compressed, hourly files. Once fetched, the files are converted to CSV format tick files and aggregated into daily files. When all 24 hour periods are available and the daily CSV file is written to the cache, the raw native files are discarded.

Dukascopy downloads are notoriously slow and unreliable due to rate limiting and limited resources available for their service. This tool has multiple strategies to address and work around those limitations, including retaining the raw files until a full daily file of CSV data can be written. Re-running the same `tradedesk-dc-export` is both safe and efficient - it will only attempt to fill in gaps and will finish very quickly where downloads or conversions are already cached.

For this to work well though, you should treat the cache directory as a permanent, not a transient store of local market data that can be added to over time. Best practice is to **always** specify a `--cache-dir` that points to your common market data trove wherever you use the tool from.

### Concurrency and Dukascopy reliability

Each symbol export uses up to two downloader threads internally. `--workers`
controls how many symbols are exported concurrently, so the total request
concurrency can grow quickly.

Dukascopy becomes unreliable when too many requests are in flight. If you want
to stay near the safest limit of two concurrent download threads, keep
`--workers 1`. Re-running the same command is idempotent and is the intended way
to fill cache gaps caused by failed hours.

### Resampled CSV using `--out`

If you resample to an `--out` location, the tool writes separate bid and ask
OHLCV CSV files with UTC timestamps that include an explicit `+00:00` offset:

```text
timestamp,open,high,low,close,volume
2025-01-01 00:00:00+00:00,1.10342,1.10361,1.10311,1.10355,1234.0
```

- Timestamps are always **UTC**
- Prices are floats **after applying the price divisor**
- Volume is derived from tick volume

#### Metadata sidecar (`.meta.json`)

Every output CSV is accompanied by a metadata file describing how it was generated:

```json
{
  "data_type": "candles",
  "generated_at": "2026-03-06T16:58:50.397630Z",
  "params": {
    "date_from": "2026-01-05",
    "date_to": "2026-01-06",
    "price_side": "bid",
    "resample": "15MIN"
  },
  "price_divisor": 10.0,
  "schema_version": "1",
  "source": "dukascopy",
  "symbol": "GBPUSD",
  "timestamp_format": "iso8601_utc"
}
```

This ensures datasets are **self-describing and reproducible**, even months later.

`--resample` requires `--out`. If you run the tool without `--resample`, it will
populate the `--cache-dir` with the cached source data and daily candles but it
will not emit the final range-level output CSVs in `--out`.

---

## Requirements

- Python 3.11+
- Internet access to Dukascopy datafeed

## Credentials and Release Automation

Normal exporter usage, local development, and CI do not require repository
secrets or broker credentials.

Maintainers running `.github/workflows/prepare-release.yml` need these
repository secrets configured:

- `RELEASE_APP_ID`
- `RELEASE_APP_PRIVATE_KEY`

The release workflow uses those secrets to mint a GitHub App token for
checkout, version bumping, pushing the release commit, and creating the GitHub
release. `.github/workflows/publish.yml` uses PyPI trusted publishing via
GitHub OIDC (`id-token: write`), so no PyPI API token secret is expected in
this repository.

---

## License

Licensed under the Apache License, Version 2.0.
See: https://www.apache.org/licenses/LICENSE-2.0

Copyright 2026 [Radius Red Ltd.](https://github.com/radiusred)

## Contributing

See CONTRIBUTING.md for guidelines on contributing to tradedesk-dukascopy.
