# ATARS v3.1.1
### Extended from the ATARS v2.0 Research Framework
### Automated Time-Series Analysis and Reporting System

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)](LICENSE)
[![Version](https://img.shields.io/badge/Version-v3.1-blueviolet?style=for-the-badge)](#)
[![Operators](https://img.shields.io/badge/Formal%20Operators-16-blue?style=for-the-badge)](#)
[![Verification](https://img.shields.io/badge/Verification-9%20Modules-success?style=for-the-badge)](#)
[![Cleaning](https://img.shields.io/badge/Data%20Cleaning-18%20Steps-orange?style=for-the-badge)](#)
[![Weather](https://img.shields.io/badge/Open--Meteo-18%20Variables-lightblue?style=for-the-badge)](#)
[![PPT](https://img.shields.io/badge/PowerPoint-19%20Slides-red?style=for-the-badge)](#)
[![MNS](https://img.shields.io/badge/MNS-AI%20Trust%20Metric-brightgreen?style=for-the-badge)](#)

> ATARS is a formal, reproducible, end-to-end environmental analytics framework that transforms raw sensor data into a complete analytical report — automatically.
>
> ATARS v3.1 extends the original v2.0 framework with multi-layer AI verification, statistical inference, weather-aware contextual analysis, advanced data cleaning, automated PowerPoint generation, and a unified trust metric for AI-generated reports.

---

## 🎯 Problem It Solves

Urban air-quality stations generate thousands of observations per year.

Manual analysis is:

- Repetitive
- Time-consuming
- Statistically inconsistent
- Difficult to reproduce
- Difficult to audit
- Risky when AI-generated text is involved

Traditional AI grounding systems verify only numerical values and cannot detect temporal errors, unsupported causation, missing uncertainty, chemical inconsistencies, benchmark omissions, or report-quality drift.

ATARS addresses these limitations through a reproducible statistical pipeline and a multi-module verification framework.

---

## 🚀 What ATARS Produces

**Input:** CSV or Excel environmental time-series dataset

**Output (fully automated):**

- 📄 Structured Word report
- 📊 Publication-grade analytical charts
- 📽 Executive PowerPoint presentation
- 📁 JSON statistical contract (J)
- 🔐 SHA-256 reproducibility audit
- 📈 G_rate grounding metrics
- 🎯 MNS trust score
- 🌦 Weather-enriched contextual analysis
- 📋 Data-cleaning audit logs

No manual formatting.
No manual chart creation.
No manual report writing.

---

## 🧠 Technical Core (Inherited from v2.0)

ATARS is built on a formally defined statistical foundation.

### 🔹 16 Formal Operators

- Data Quality Score Q(D)
- Aggregation Operator A(Dᵥ)
- Rolling Baseline B(D,W)
- Z-score Anomaly Detection ζd
- Percentage Deviation δd
- Confidence Intervals
- Autocorrelation (ACF)
- Partial Autocorrelation (PACF)
- Pearson Correlation
- OLS Regression

All operators are deterministic and reproducible.

### 🔹 Machine Learning Layer

- Isolation Forest (multivariate anomaly detection)
- Holt-Winters triple exponential smoothing
- Fixed random_state for reproducibility

Machine learning extends — never replaces — formal statistical operators.

### 🔹 Runtime Grounding Verifier (RGV)

ATARS v2.0 introduced Runtime Grounding Verification.

Process:

1. Statistical results are stored inside JSON Contract J
2. Optional LLM generates narrative text
3. Numerical claims are extracted
4. Values are verified against J
5. Grounding score is computed

```text
G_rate =
grounded_sentences /
total_numerical_sentences
```

This converts LLM validation into a measurable metric.

---

## 🆕 Major Additions in v3.1

ATARS v3.1 extends the original framework with:

### 1. 18-Step Data Cleaning Engine

Professional analyst-grade preprocessing before statistical computation.

Includes:

- Flatline detection
- Spike filtering
- 3×IQR winsorization
- 5-strategy imputation cascade
- Chemical constraint auditing
- Quality scoring system

All cleaning operations are logged automatically.

### 2. Statistical Inference Layer

Beyond descriptive statistics:

- Mann-Kendall Trend Test
- Shapiro-Wilk Normality Test
- D'Agostino Normality Test
- OLS Inference
- VIF
- Durbin-Watson
- Seasonal Decomposition
- Exceedance Frequency Analysis

### 3. Open-Meteo Weather Integration

- Free API
- No API key required
- ERA5 weather context
- 18 weather variables
- 18 derived atmospheric indicators

Including:

- Inversion Risk
- Fog Risk
- Dust Risk
- Stagnant Air Detection
- Pollution Risk Score

### 4. Automated Executive Presentation

ATARS automatically generates:

- Executive PowerPoint
- Live KPI dashboards
- Quality summaries
- Forecast summaries
- Verification summaries

---

## 🧠 Multi-Layer Verification Framework (v3.1)

ATARS v3.1 introduces 9 complementary verification modules.

| Module | Purpose |
|----------|----------|
| RGV | Numerical Grounding |
| TCV | Temporal Claim Verification |
| CCD | Causal Claim Detection |
| AEE | Anomaly Explanation Engine |
| RDD | Report Drift Detection |
| UPT | Uncertainty Propagation |
| CVCC | Cross-Variable Consistency |
| NSS | Narrative Specificity Scoring |
| BNC | Benchmark Comparison |

Each module catches a different failure mode that the others cannot detect.

---

## 🎯 Master Novelty Score (MNS)

ATARS aggregates verification outputs into a single trust metric.

```text
MNS =
G_v2 × 0.25 +
G_v3 × 0.20 +
TCV × 0.15 +
CCD × 0.15 +
CVCC × 0.10 +
Conf × 0.10 +
Sem × 0.05
```

Trust levels:

| Score | Verdict |
|---------|----------|
| ≥ 0.90 | CERTIFIED |
| ≥ 0.80 | TRUSTED |
| ≥ 0.70 | ACCEPTABLE |
| ≥ 0.55 | REVIEW |
| < 0.55 | UNRELIABLE |

---

## 🔐 Reproducibility & Security

- SHA-256 audit logging
- Deterministic execution
- Fixed random seeds
- Fully local execution supported
- JSON Contract J acts as formal boundary between computation and language
- Optional AI-free execution via `--no-llm`

---

## ⚡ Quick Start

```bash
python atars_v3.py --data "input.csv" --city "Delhi" --no-llm
```

Optional (local Ollama model):

```bash
python atars_v3.py --data "input.csv" --city "Delhi" --use-llm
```

Weather-aware execution:

```bash
python atars_v3.py --data "input.csv" --city "Delhi" --weather open_meteo
```

---

## 📊 Applications

- Urban Air Quality Monitoring
- Environmental Compliance Reporting
- Smart City Analytics
- Public Health Research
- Automated Scientific Reporting
- Environmental Risk Assessment
- Reproducible Statistical Workflows

---

## 🎓 Research Context

ATARS integrates:

- Formal Statistical Modeling
- Statistical Inference
- Machine Learning
- Environmental Context Modeling
- AI Narrative Verification
- Multi-Layer Trust Evaluation
- Deterministic Audit Trails

The novelty of ATARS v3.1 is not any individual verification module, but the integration of nine complementary verification modules into a single automated environmental reporting pipeline.

---

## 📄 License

MIT License — Copyright (c) 2026 Priyanshu

Free to use, modify, and distribute with attribution.

---

*ATARS v3.1 — Formal. Reproducible. Verified.*
