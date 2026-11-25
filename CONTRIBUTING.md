# Contributing to Keel

Thanks for helping. Keel is research-first: a change is judged by the evidence it produces, not by how much it adds.

## Ground rules

- **Read-only until the gate.** Nothing that holds keys, signs, places orders or calls Hyperliquid's exchange endpoint is accepted outside the reviewed execution milestone. `tests/test_safety_invariants.py` enforces this.
- **No promotion by backtest.** Simulation, census and historical-trade evidence can only reject a hypothesis. Strategy, risk-limit and execution changes need the maintainer's approval and go through the research registry (`docs/research.md`).
- **Registered thresholds don't move.** A change to a running test's thresholds is a new hypothesis version, never an edit.
- **No profitability claims.** Reports state what was measured, over what span, with what uncertainty.
- **No planning material in the repo.** PRDs, internal reports and plans stay local (`.gitignore` covers them).

## Development setup

```bash
git clone https://github.com/gsoffmarket/keel.git && cd keel
python3 -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"          # add ,parquet for columnar tables
python -m pytest
```

The runtime has no third-party dependencies; keep it that way unless there is a strong reason (pyarrow stays optional).

## Making a change

1. Open an issue first for anything beyond a small fix, so scope is agreed.
2. Branch from `main`: `feat/…`, `fix/…`, `docs/…`.
3. Write tests with the change. Logic gets unit tests; research code also gets a test on constructed data with a known answer, and where possible a check in `keel research validate`.
4. Keep commits focused, one logical change each, with [Conventional Commit](https://www.conventionalcommits.org/) messages: `feat(recorder): …`, `fix(refs): …`, `docs: …`, `test: …`, `ci: …`.
5. Update the docs for any change to commands, config keys, report fields or methods.
6. Open a pull request and complete the template. CI must be green.

## Style

- Python 3.9 compatible; type hints on public functions.
- Money and prices are fixed-point integers (`_uusdc`, `_px7`, `_sz6`); times are integer nanoseconds (`_ns`). Never use floats for stored quantities.
- Raw data is append-only and verbatim; derived tables are rebuilt from raw, never edited.
- Reports are deterministic: same snapshot and version, same bytes.
