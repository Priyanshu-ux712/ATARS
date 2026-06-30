---
title: 'ATARS: An open-source, reproducible pipeline for automated time-series analysis, forecasting, and multi-dimensional verification of LLM-generated air-quality reports'
tags:
  - Python
  - air quality
  - time series
  - data quality
  - anomaly detection
  - forecasting
  - large language models
  - hallucination
  - trustworthy AI
  - reproducibility
authors:
  - name: Priyanshu Kumar
    orcid: 0009-0005-9916-9627
    affiliation: 1
affiliations:
  - name: Global Institute of Technology and Management (Affiliated to Gurugram University), Haryana, India
    index: 1
date: 30 June 2026
bibliography: paper.bib
---

# Summary

`ATARS` v3.1 (Automated Time-Series Analysis and Reporting System) is an open-source, single-file Python pipeline that takes a raw urban air-quality dataset (hourly pollutant readings, e.g. from India's CPCB monitoring network) and produces a complete, reproducible, and *independently verified* analytical report from one command, with no manual intervention. The pipeline runs in four phases. **(1) A data pipeline** ingests the file (40+ column-name synonyms, four encodings) and cleans it with an **18-step automated data-quality engine** — duplicate and datetime-gap handling, physical-bounds clipping, 3×IQR winsorization, flatline/spike/negative nulling, four-strategy imputation, diurnal flagging, and null-streak (downtime vs. dropout) analysis — scored on a seven-dimension quality composite and logged step-by-step for audit. **(2) A statistical phase** computes sixteen formally numbered operators (daily aggregation, rolling-baseline z-score anomalies, confidence intervals, autocorrelation, correlation, OLS) plus a formal-inference layer (Mann–Kendall trend test, time-series decomposition) and two additive machine-learning modules (Isolation-Forest multivariate anomaly detection and Holt–Winters forecasting), assembling a typed JSON data contract `J`. **(3) A contextual-enrichment phase** adds live Open-Meteo weather, a Pasquill–Gifford atmospheric-stability proxy, an eight-type pollution-episode classifier, and a seven-day pollution-risk forecast. **(4) A verification phase** — the distinguishing contribution — checks any optional LLM-written narrative against `J` along **nine independent dimensions** (numerical grounding, temporal, causal, anomaly-explanation, report-drift, uncertainty, cross-variable consistency, narrative specificity, and benchmark comparison), unified by a single composite **Master Novelty Score (MNS)**. The system emits an 18-section Word report, 15 publication-quality charts, a 15-slide executive PowerPoint, daily snapshot JSONs, machine-readable verification artifacts, and a cryptographically-hashed audit log. ATARS runs fully offline (the LLM is optional and local), is deterministic and reproducible (fixed seeds, `temperature = 0`, and a SHA-256 audit hash), and is released under the MIT License.

# Statement of need

National air-quality networks generate hundreds of millions of sensor records per year, yet the analyses built on them are rarely reproducible: pipelines are seldom released as runnable code, almost never documented with equation-level precision, and essentially never accompanied by an audit trail linking computed statistics to report text [@carslaw2012openair; @who2021]. The recent use of LLMs to write such data-driven reports adds a second, qualitatively different risk — *hallucination*: a model can state exceedance counts, correlations, anomaly dates, or causal claims that are absent from or unsupported by the data, which is unacceptable in an environmental or regulatory context where such numbers may inform policy [@brown2020; @maynez2020].

Existing tools each address only part of this. Exploratory packages such as `openair` are excellent for analysis but generate no automated reports, no formal anomaly operators, and no verification of AI-generated text [@carslaw2012openair]. General hallucination-mitigation methods (retrieval-augmented generation, post-hoc fact verification, FactScore, FaithDial) target open-domain *semantic* faithfulness and require external retrieval or labelled corpora, not numerical, temporal, and causal verification against a closed, local data contract [@lewis2020rag; @min2023factscore; @dziri2022faithdial]. ATARS fills this gap for researchers, students, NGOs, and monitoring agencies who need an end-to-end, auditable, offline, and *trustworthy* air-quality reporting pipeline. Its novelty is the **combination**: a formally-specified statistical core and additive ML, wrapped in a professional 18-step data-quality engine and an atmospheric contextual layer, and made *checkable* by a nine-module verification suite that reports a reproducible `G_rate` and an interpretable `MNS`. Where prior work asks only "are the numbers grounded?", ATARS asks whether the numbers are grounded, the trends real, the causes warranted, the anomalies explained, the uncertainty preserved, the variables consistent, the language specific, and the benchmarks correct — in one auditable score. Because it is a single dependency-light Python module under a permissive licence, it is straightforward to install, audit, extend, and cite.

# Key features

- **Four-phase, single-command pipeline** from CSV/Excel ingestion through an 18-step data-quality engine, statistical analysis, contextual enrichment, and a multi-dimensional verification layer — all deterministic and offline-capable.
- **18-step automated data-quality engine** (reported as Section 00b of the Word output): duplicate/gap handling, physical-bounds clipping, 3×IQR winsorization, flatline/spike/negative nulling, four-strategy imputation, diurnal flagging, and null-streak analysis, scored on a seven-dimension quality composite with full before/after counts.
- **Sixteen formally numbered statistical operators** plus a **formal-inference layer** (Mann–Kendall non-parametric trend test and trend/seasonal/residual decomposition), all pure deterministic functions.
- **Additive machine learning** that never alters the formal operators and never enters `J`: Isolation-Forest multivariate anomaly detection and Holt–Winters triple exponential smoothing with prediction intervals.
- **Contextual enrichment**: free, key-less Open-Meteo weather retrieval, a Pasquill–Gifford atmospheric-stability proxy, a deterministic eight-type pollution-episode classifier, and a seven-day pollution-risk forecast.
- **Nine-module verification suite + Master Novelty Score** — the primary novel contribution: each module checks one axis of the LLM narrative against the typed contract `J` (numerical grounding/`G_rate`, temporal, causal, anomaly-explanation, report-drift, uncertainty, cross-variable consistency, narrative specificity, benchmark comparison) and writes machine-readable verification artifacts; the MNS aggregates them into one interpretable trustworthiness score.
- **Security and privacy by design:** the LLM receives only the aggregated JSON contract; the single permitted network call (a local Ollama server) is disabled by `--no-llm`; there is no telemetry.
- **Reproducibility:** fixed random seeds, deterministic LLM temperature, canonical-JSON SHA-256 hashing of the data contract and report, and a full configuration snapshot in the audit log.

# Example: Gurugram, India (2024)

On a publicly available CPCB dataset for Gurugram, Haryana (8,784 hourly records, 366 days, 10 variables) [@tripathi2024dataset], ATARS completed the full pipeline in about four minutes and produced a verified report. The 18-step engine scored overall data quality `Q(D) = 0.744` (MODERATE); the analysis surfaced a severe pollution burden (PM10 annual mean 185.1 µg/m³, ~4.1× the WHO guideline, exceeded on 99.5% of days). The Isolation Forest flagged 19 anomaly days, 16 of which were multivariate episodes that univariate z-score analysis missed entirely; Holt–Winters produced a 14-day PM10 forecast (in-sample RMSE 77.89 µg/m³); and the episode classifier labelled anomaly days by physical type with confidence scores. The verification suite checked the LLM narrative against the 143-entry data contract and reported `G_rate = 92.9%`, together with a Master Novelty Score summarising all nine verification axes. The run reproduces from the public dataset and the logged `SHA-256(J)`.

# Quality control

Every formal operator was validated against analytically-known synthetic data; the Isolation Forest and Holt–Winters modules were validated on synthetic series with injected anomalies and known seasonality; the 18-step data-quality engine was validated on datasets with injected duplicates, gaps, flatlines, spikes, and out-of-bounds values; and the verification modules were validated on a curated set of grounded, partially-grounded, ungrounded, temporal, causal, and non-numerical sentences. These validations are shipped as an automated test suite (`tests/test_atars.py`). Determinism is enforced by fixed seeds and `temperature = 0`, and verified by the invariance of the canonical-JSON SHA-256 hash across re-executions on the same architecture. The software runs fully offline with `--no-llm`.

# Acknowledgements

This software builds on the open-source scientific-Python ecosystem — NumPy [@harris2020numpy], pandas [@mckinney2010pandas], SciPy [@virtanen2020scipy], scikit-learn [@pedregosa2011scikit], python-docx, and python-pptx — and uses publicly available CPCB and Open-Meteo data [@tripathi2024dataset]. No external funding supported this work.

# References
