## Project Overview

**tradedesk-dukascopy** is a data acquisition and normalisation project that feeds high-quality historical market data into *tradedesk*. Its primary role is to source, clean, and persist Dukascopy data in a form suitable for research and backtesting.

This project is **data infrastructure**, not strategy logic.

---

## Primary Responsibilities

Agents should treat this project as responsible for:

* Fetching historical tick and candle data from Dukascopy
* Handling retries, gaps, and provider instability
* Normalising timestamps, sessions, and formats
* Persisting data in a consistent, research-friendly layout
* Making data quality issues visible, not hidden

Downstream consumers should not need to understand Dukascopy quirks.

---

## Current Objectives

Typical work includes:

* Improving robustness of data retrieval
* Detecting and handling missing or partial data
* Ensuring timestamp and timezone correctness
* Verifying consistency across instruments and periods
* Keeping outputs compatible with *tradedesk* expectations

Performance is secondary to correctness and transparency.

---

## Preferred Way of Working

### Defensive and Explicit

* Assume upstream data is unreliable
* Prefer explicit checks over silent fixes
* Fail loudly when invariants are broken

### Small, Verifiable Changes

* One improvement at a time
* Validate changes against real data samples
* Avoid refactors that obscure data flow

### Minimal Abstraction

* This is plumbing, not a framework
* Keep control flow easy to follow
* Avoid indirection unless it removes duplication

---

## Coding Expectations

* Use latest stable libraries
* Follow Dukascopy protocol and data format realities
* No `from __future__ import ...`
* Prioritise readability and traceability
* Code should meet `ruff check` and `mypy --strict` requirements
* Create commit messages for git following "Conventional Commits" and the current style of the project's git log

When suggesting code:

* Provide complete snippets
* No diffs
* Assume direct insertion into the codebase

---

## Domain Assumptions

Agents should understand:

* Tick vs candle data trade-offs
* Timezones, DST, and session boundaries
* How data gaps affect backtests
* Why “clean-looking” data can still be wrong

If data quality is uncertain, surface it explicitly.

---

## What to Avoid

* Strategy or trading logic
* Silent interpolation or smoothing
* Hiding provider errors
* Over-abstracted ingestion pipelines

---

## Success Criteria

An agent is succeeding if it:

* Improves data reliability or transparency
* Makes failures easier to diagnose
* Prevents subtle downstream research errors
* Keeps the project narrowly focused on data quality
