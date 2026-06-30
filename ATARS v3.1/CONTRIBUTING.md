# Contributing to ATARS

Thanks for your interest in improving ATARS. Contributions of all kinds are welcome —
bug reports, documentation fixes, new tests, and new features.

## Reporting issues

Open an issue at https://github.com/Priyanshu-ux712/ATARS/issues and include:

- what you ran (the exact command and ATARS version),
- what you expected vs. what happened,
- a minimal sample of input data (or its shape/columns) if relevant,
- your OS and Python version, and the full traceback.

## Asking for support

For usage questions, open a **Discussion** or an issue labelled `question`. Please
check the README and the `--help` output first.

## Development setup

```bash
git clone https://github.com/Priyanshu-ux712/ATARS.git
cd ATARS
pip install -r requirements.txt
pip install pytest
pytest -q          # or: python tests/test_atars.py
```

## Pull requests

1. Fork the repo and create a feature branch (`git checkout -b feature/my-change`).
2. Keep changes focused; match the existing code style and the equation-numbering
   convention in the source comments.
3. **Add or update tests** in `tests/test_atars.py` for any behaviour change. New
   statistical operators should be validated against an analytically-known value.
4. Ensure the full suite passes and the pipeline still runs end-to-end on a small
   dataset (`--no-llm`).
5. Update the README / docstrings if behaviour or CLI flags change.
6. Open the PR with a clear description of the motivation and the change.

## Design principles (please preserve)

- **Determinism & reproducibility** — fixed seeds, `temperature = 0`, and
  canonical-JSON SHA-256 hashing must continue to hold; don't introduce
  nondeterminism into the analysis path.
- **LLM isolation** — the model receives only the aggregated JSON contract `J`;
  never feed raw records or ML extrapolations into the narrative.
- **Offline-first** — the only permitted network call is the optional local LLM
  (and optional Open-Meteo weather); no telemetry.
- **Additive ML** — machine-learning modules must not modify the formal operators
  or enter `J`.

## Code of conduct

Be respectful and constructive. By participating you agree to keep the project a
welcoming space for everyone.

## License

By contributing, you agree that your contributions are licensed under the MIT License.
