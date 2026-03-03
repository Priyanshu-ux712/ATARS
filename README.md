# ATARS v2.0
### Automated Time-Series Analysis and Reporting System

ATARS is a formal, reproducible, end-to-end time-series analysis framework that transforms raw environmental sensor data into a complete analytical report — automatically.

It integrates statistical operators, machine learning models, deterministic chart generation, and algorithmic LLM grounding verification into a single auditable pipeline.

---

## 🎯 Problem It Solves

Urban air quality stations generate thousands of hourly records per year.  
Manual analysis is:

- Repetitive  
- Time-consuming  
- Statistically inconsistent  
- Difficult to reproduce  
- Risky when AI-generated text is involved  

ATARS eliminates manual repetition and enforces statistical correctness with full reproducibility.

---

## 🚀 What ATARS Produces

Input:  
CSV or Excel time-series dataset (hourly pollutant data)

Output (fully automated):

- 📄 18-section structured Word report
- 📊 15 publication-grade analytical charts
- 📁 JSON statistical contract
- 🔐 SHA-256 reproducibility audit
- 📈 G_rate grounding score (LLM validation metric)

No manual formatting. No manual chart creation. No manual report writing.

---

## 🧠 Technical Core

ATARS is built on a formally defined statistical foundation.

### 🔹 16 Formal Operators
- Data Quality Score: Q(D)
- Aggregation operator A(D_v)
- Rolling baseline B(D, W=30)
- Z-score anomaly detection ζ_d
- Delta percentage deviation δ_d
- Confidence intervals
- Autocorrelation (ACF)
- Partial autocorrelation (PACF)
- Pearson correlation
- OLS regression

All operators are deterministic and reproducible.

---

### 🔹 Machine Learning Enhancements (v2.0)

- Isolation Forest (multivariate anomaly detection)
- Holt-Winters triple exponential smoothing (14-day PM10 forecast)
- Fixed random_state for reproducibility
- ML modules extend — never replace — formal operators

---

### 🔹 Runtime Grounding Verifier (RGV) — Novel Contribution

ATARS introduces an algorithmic safeguard against AI hallucination.

Process:
1. All computed statistics are stored in a structured JSON contract (J)
2. The LLM generates narrative text (optional, local model)
3. Every numerical claim is extracted
4. Each number is verified against J
5. Grounding score is computed:

G_rate = grounded_sentences / total_numerical_sentences

This converts LLM validation from a qualitative concept into a measurable metric.

---

## 🔐 Reproducibility & Security

- SHA-256 hash of JSON contract ensures auditability
- Deterministic execution (fixed seeds)
- Fully local execution supported
- LLM receives aggregated metrics only (never raw data)
- `--no-llm` mode disables AI completely

---

## ⚡ Quick Start

```bash
python atars_v2.py --input your_data.csv --city "Delhi" --no-llm
```

Optional (local LLM via Ollama):

```bash
python atars_v2.py --input your_data.csv --city "Delhi" --use-llm
```

---

## 📊 Applications

- Urban air quality monitoring
- Environmental compliance reporting
- Smart city analytics
- Public health research
- Automated research reporting
- Reproducible statistical workflows

---

## 🎓 Research Context

ATARS is designed as a reproducible analytical framework integrating:

- Formal statistical modeling
- Machine learning enhancement
- AI-grounded narrative verification
- Deterministic audit trails

The complete research paper is included in this repository.

---

## 📄 License

MIT License  
Copyright (c) 2026 Priyanshu  

Free to use, modify, and distribute with attribution.

---

### ATARS — Formal. Reproducible. Automated.
