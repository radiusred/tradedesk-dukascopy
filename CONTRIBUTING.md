# Contributing

## Scope

This repository is intentionally standalone and does not depend on the `tradedesk` framework.

## Conventions

- All timestamps are UTC and written as ISO-8601 strings.
- Exported datasets are canonical and self-describing via `<output>.meta.json`.
- `--price-divisor` is applied once at export-time; downstream code must not rescale.

## Development

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e .
pip install -U pytest ruff
pytest
ruff check .
```
