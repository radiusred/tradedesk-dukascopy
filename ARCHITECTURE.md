# Architecture

Overview
- tradedesk-dukascopy is a data preparation and backtesting utility that sits alongside the main tradedesk framework.
- Its focus is on converting Dukascopy data into deterministic CSV candles with metadata, plus versioned cache management and compression.

Core components
- Dukascopy downloader: fetches hourly or daily raw tick data from Dukascopy.
- Candles exporter: converts raw ticks into standardized CSV candles and writes metadata sidecars.
- Cache manager: coordinates data paths, compression, and consolidation of daily outputs.
- CLI entry points: provide the user-facing tools to export data and manage the cache.

Data flow
- Data is downloaded from Dukascopy, converted to CSV candles, a metadata file is emitted, and the daily file is placed into a cache directory.
- Optional compression is applied to daily CSVs to save disk space (per compression brief in docs/briefs/01-compression.md).

Live vs Backtest mode
- This repository is primarily a data-generation and backtest preparation utility. It is designed to be used in tandem with tradedesk backtesting workflows.

Notes
- This document is public-facing; avoid exposing internal data sources or credentials.
- See CLAUDE.md for local initialization and agent guidelines when extending functionality.

---

## License

Licensed under the Apache License, Version 2.0.
See: https://www.apache.org/licenses/LICENSE-2.0

Copyright 2026 [Radius Red Ltd.](https://github.com/radiusred) | [Contact](mailto:opensource@radiusred.uk)
