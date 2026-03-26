# Contributing

## Scope

This repository is intentionally standalone and does not depend on the `tradedesk` framework.

## Conventions

- All timestamps are UTC and written as ISO-8601 strings.
- Exported datasets are canonical and self-describing via `<output>.meta.json`.
- `--price-divisor` is applied once at export-time; downstream code must not rescale.
- Keep Dukascopy concurrency conservative. One symbol export already uses four
  downloader threads internally, so `--workers 1` is the safest default when
  reproducing failures or filling gaps.

## Development

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e .
pip install -U pytest ruff
pytest
ruff check .
```
