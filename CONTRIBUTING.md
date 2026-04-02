# Contributing

We welcome contributions to tradedesk-dukascopy. This repository is designed to be standalone but aligns with Radius Red's open-source practices.

## Getting started
- Create a Python virtual environment and install in editable mode:
```
python -m venv .venv
source .venv/bin/activate
pip install -e .
```
- Install development tools (optional but recommended):
```
pip install -U pytest ruff
```

## Testing
- Run the test suite:
```
pytest
```
- Run the linter:
```
ruff check .
```
- If you have type hints, run static type checks as applicable for the repo.

## Contributing workflow
- Create a small, focused branch (e.g. feat/your-change).
- Write tests or update existing ones to cover the change.
- Update docs if the change affects user-facing behavior.
- Open a PR with a concise description that explains the rationale and the impact.
- Ensure CI passes before requesting a review.
- Do not commit secrets, credentials, or internal configuration.

## Documentation alignment
- Update any relevant README or docs to reflect the change and to keep public-facing behavior clear.

If you are unsure about the right approach, open an issue describing the goal and ask for guidance before implementing.
