# Contributing to tradedesk-dukascopy

We welcome contributions to `tradedesk-dukascopy`. This guide covers the
standards and workflow you need to contribute successfully.

## Scope

This repository is intentionally standalone and does not depend on the
`tradedesk` framework.

## Getting Started

1. Fork the repository and create a feature branch from `main`.
2. Clone locally and install the dev dependencies:

```bash
pip install -e '.[dev]'
```

Or with [`uv`](https://docs.astral.sh/uv/):

```bash
uv sync --extra dev
```

## Code Standards

### Python Version

Python 3.11+ is required.

### Style and Linting

We use [ruff](https://docs.astral.sh/ruff/) for linting and formatting:

```bash
ruff check .
ruff format --check .
```

Rules enforced: `E`, `F`, `I` (import sorting), `B` (bugbear), `UP` (pyupgrade).
Line length is 100 characters.

### Type Checking

We use [mypy](https://mypy.readthedocs.io/):

```bash
mypy .
```

All contributions must pass with zero type errors.

## Conventions

- All timestamps are UTC and written as ISO-8601 strings.
- Exported datasets are canonical and self-describing via `<output>.meta.json`.
- `--price-divisor` is applied once at export time; downstream code must not
  rescale.
- Keep Dukascopy concurrency conservative. One symbol export already uses two
  downloader threads internally, so `--workers 1` is the safest default when
  reproducing failures or filling gaps.

## Testing

We use [pytest](https://docs.pytest.org/):

```bash
pytest --cov=tradedesk_dukascopy --cov-fail-under=75
```

- All new and existing tests must pass following any code change.
- Code coverage must remain at or above 75%.
- Include tests for new functionality or bug fixes.

## Pull Requests

- Keep changes small, well-scoped, and documented in PR descriptions.
- Open PRs against `main`.
- Rebase your branch onto `main` before submitting — we do not use merge commits.
- Provide a brief rationale and expected impact in the PR description.
- Do not include secrets or internal data.

## Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(cli): add parquet output format
fix(export): handle missing tick hours gracefully
docs: update cache directory guidance
```

## Reporting Issues

If you find a bug or have a feature request, open an issue describing the goal
and expected behaviour. Include steps to reproduce for bugs.

If unsure about scope or approach, open an issue to discuss before implementing.

---

## License

Licensed under the Apache License, Version 2.0.
See: https://www.apache.org/licenses/LICENSE-2.0

Copyright 2026 [Radius Red Ltd.](https://github.com/radiusred)
