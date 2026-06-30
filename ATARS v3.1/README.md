# ATARS v3.1 — Automated Time-Series Analysis and Reporting System

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
![Python](https://img.shields.io/badge/python-3.9%2B-blue)
![Status](https://img.shields.io/badge/release-v3.1.0-green)

**ATARS** is an open-source, single-file Python pipeline that turns a raw urban
air-quality dataset (hourly pollutant readings, e.g. from India's CPCB network)
into a complete, reproducible, and **independently verified** analytical report —
from one command, fully offline.

Its distinguishing contribution is a **nine-module verification suite** that checks
any AI-written narrative against the data along nine independent axes (numerical
grounding, temporal, causal, anomaly-explanation, drift, uncertainty,
cross-variable consistency, specificity, and benchmark comparison), condensed into
a single **Master Novelty Score (MNS)** and a reproducible grounding rate `G_rate`.

> This repository contains the **open-source pipeline** (`atars_v3_final.py`).
> The hosted no-code web app is a separate, closed-source project.

---

## Why ATARS?

Air-quality analyses are rarely reproducible, almost never documented with
equation-level precision, and — increasingly — written by LLMs that can *hallucinate*
numbers no one checks. ATARS closes that loop end-to-end: clean → analyse →
contextualise → **verify**, with every output reproducible from a SHA-256-hashed
data contract.

## Pipeline (four phases)

1. **Data pipeline** — ingestion (40+ column synonyms, 4 encodings) + an **18-step
   automated data-quality engine** (duplicate/gap handling, physical-bounds clipping,
   3×IQR winsorization, flatline/spike/negative nulling, four-strategy imputation,
   diurnal flagging, null-streak analysis), scored on a seven-dimension composite.
2. **Statistical analysis** — 16 equation-numbered operators + formal inference
   (Mann–Kendall, decomposition) + Isolation Forest + Holt–Winters → typed JSON
   contract `J`.
3. **Contextual enrichment** — Open-Meteo weather (free, key-less), Pasquill–Gifford
   stability proxy, an eight-type pollution-episode classifier, and a seven-day risk
   forecast.
4. **Verification** — the nine-module suite + Master Novelty Score, writing
   machine-readable verification artifacts.

## Outputs

An 18-section Word report · 15 charts (160 DPI) · a 15-slide executive PowerPoint ·
the JSON contract `J` · `grounding_verification.json` · daily snapshot JSONs ·
a SHA-256 audit log · a Windows Task Scheduler `.bat`.

## Installation

```bash
git clone https://github.com/Priyanshu-ux712/ATARS.git
cd ATARS
pip install -r requirements.txt
```

Python 3.9+ is required. The optional LLM narrative uses a **local** [Ollama](https://ollama.com)
server; it is disabled with `--no-llm`, and ATARS runs fully offline without it.

## Usage

```bash
# Fully offline, no LLM:
python atars_v3_final.py --data your_data.csv --city "Gurugram" --no-llm

# With live Open-Meteo weather context:
python atars_v3_final.py --data your_data.csv --city "Delhi" --weather open_meteo

# Quick test mode:
python atars_v3_final.py --data your_data.csv --city "Mumbai" --mode test
```

Input is a CSV/Excel of hourly pollutant readings (PM10, NO2, SO2, Ozone, etc.) with
a timestamp column; column names are auto-normalised from 40+ synonyms.

## Tests

```bash
pip install pytest
pytest -q                      # or: python tests/test_atars.py
```

The suite validates the formal operators, the quality engine, Mann–Kendall, the
Isolation Forest (incl. determinism), and Holt–Winters against analytically-known
synthetic data.

## Reproducibility

Fixed seed (42), LLM `temperature = 0`, and canonical-JSON **SHA-256** hashing of the
data contract make every run reproducible on the same architecture. Re-running on the
same input yields an identical `SHA-256(J)`.

## Citing ATARS

If you use ATARS in academic work, please cite the archived release (Zenodo DOI for
v2.0: [10.5281/zenodo.18866062](https://doi.org/10.5281/zenodo.18866062); a v3.1 DOI
will be minted at release) and, once published, the JOSS paper.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Issues and pull requests are welcome.

## License

MIT — see [LICENSE](LICENSE). © 2026 Priyanshu Kumar.
