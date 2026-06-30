# -*- coding: utf-8 -*-
"""
Automated test suite for ATARS v3.1 (atars_v3_final.py).

These tests validate the formal statistical operators, the data-quality engine,
and the machine-learning modules against analytically-known synthetic data, and
confirm the determinism that underpins the Reproducibility Theorem (Eq. 17).

Run with:   pytest -q
or simply:  python tests/test_atars.py
"""
import importlib.util
import math
import os
import sys

import numpy as np
import pandas as pd

# The pipeline prints progress with Unicode glyphs; force UTF-8 so the plain
# `python tests/test_atars.py` runner works on a Windows (cp1252) console too.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:  # noqa: BLE001
    pass

# ── Load the single-file pipeline as a module ────────────────────────────────
_HERE = os.path.dirname(os.path.abspath(__file__))
_SRC = os.path.join(os.path.dirname(_HERE), "atars_v3_final.py")
_spec = importlib.util.spec_from_file_location("atars_v3_final", _SRC)
atars = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(atars)


# ── 1. Formal statistical operators (Eqs. 3–10) ──────────────────────────────
def test_zscore_known_value():
    # ζ = (x - μ_W) / σ_W
    assert atars.z_score(20.0, 10.0, 5.0) == 2.0
    assert atars.z_score(10.0, 10.0, 5.0) == 0.0


def test_zscore_guards_zero_std():
    assert np.isnan(atars.z_score(20.0, 10.0, 0.0))


def test_delta_pct_known_value():
    assert atars.delta_pct(110.0, 100.0) == 10.0
    assert atars.delta_pct(90.0, 100.0) == -10.0


def test_aggregation_operator():
    agg = atars.aggregation_operator(pd.Series([1, 2, 3, 4, 5]))
    assert agg["mean"] == 3.0
    assert agg["N_v"] == 5
    assert agg["max"] == 5.0 and agg["min"] == 1.0
    # sample std (ddof=1) of 1..5 = sqrt(2.5); values are rounded to 4 dp
    assert abs(agg["std"] - math.sqrt(2.5)) < 1e-3


def test_aggregation_empty_series():
    agg = atars.aggregation_operator(pd.Series([], dtype=float))
    assert agg["N_v"] == 0
    assert np.isnan(agg["mean"])


def test_confidence_interval_classical():
    ci = atars.confidence_interval(100.0, 10.0, 100)
    # 100 ± 1.96 * 10/sqrt(100) = 100 ± 1.96
    assert abs(ci["lower"] - 98.04) < 0.02
    assert abs(ci["upper"] - 101.96) < 0.02


def test_confidence_interval_guards_small_n():
    ci = atars.confidence_interval(100.0, 10.0, 1)
    assert np.isnan(ci["lower"])


# ── 2. Quality scoring (Eqs. 1–2) ────────────────────────────────────────────
def test_quality_score_all_valid():
    df = pd.DataFrame({"PM10": [10, 20, 30], "q_PM10": [1, 1, 1]})
    q = atars.compute_quality_score(df, "PM10")
    assert q["Q"] == 1.0
    assert q["N_valid"] == 3 and q["N_total"] == 3


def test_quality_score_partial():
    df = pd.DataFrame({"PM10": [10, 20, 30, 40], "q_PM10": [1, 1, 3, 3]})
    q = atars.compute_quality_score(df, "PM10")
    assert q["Q"] == 0.5
    assert q["N_invalid"] == 2


# ── 3. Mann–Kendall trend test (statistical inference layer) ──────────────────
def test_mann_kendall_detects_increasing_trend():
    mk = atars.mann_kendall_test(np.arange(1, 31, dtype=float))
    assert mk["available"] is True
    assert mk["S"] > 0
    assert mk["p_value"] < 0.05  # strictly increasing series => significant


def test_mann_kendall_no_trend_flat():
    mk = atars.mann_kendall_test(np.array([5.0, 5.0, 5.0, 5.0, 5.0, 5.0]))
    # constant series has zero variance -> reported as unavailable, not a trend
    assert mk["available"] is False


# ── 4. Isolation Forest catches multivariate anomalies z-score misses ─────────
def _synthetic_long_daily(n=120, seed=42):
    """Build a LONG-format daily frame (date, variable, mean, is_anomaly, z_score)
    matching what build_daily_stats() produces, with 3 injected joint episodes."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2024-01-01", periods=n, freq="D")
    specs = {"PM10": (120, 15), "NO2": (40, 6), "SO2": (12, 3), "Ozone": (30, 5)}
    series = {v: rng.normal(mu, sd, n) for v, (mu, sd) in specs.items()}
    for d in (30, 60, 90):                 # joint multivariate episodes
        for v in series:
            series[v][d] *= 1.8
    rows = []
    for v, vals in series.items():
        for i, dt in enumerate(dates):
            rows.append({"date": dt, "variable": v, "mean": float(vals[i]),
                         "is_anomaly": False, "z_score": 0.0})
    return pd.DataFrame(rows)


def test_isolation_forest_runs_and_flags():
    res = atars.run_isolation_forest(_synthetic_long_daily(), atars.DEFAULT_CONFIG.copy())
    assert isinstance(res, dict)
    assert res.get("available", True) is not False
    assert res["n_anomalies"] >= 1            # must surface the injected episodes


# ── 5. Determinism — Reproducibility Theorem (Eq. 17) ─────────────────────────
def test_isolation_forest_is_deterministic():
    df = _synthetic_long_daily()
    r1 = atars.run_isolation_forest(df.copy(), atars.DEFAULT_CONFIG.copy())
    r2 = atars.run_isolation_forest(df.copy(), atars.DEFAULT_CONFIG.copy())
    # fixed random_state=42 => identical anomaly flags across runs
    assert list(r1["flagged_dates"]) == list(r2["flagged_dates"])


# ── 6. Holt–Winters forecast (ML-2) ───────────────────────────────────────────
def test_holt_winters_forecast_shape_and_determinism():
    # 16 weeks of weekly-seasonal daily data
    t = np.arange(112)
    series = 120 + 10 * np.sin(2 * np.pi * t / 7) + 0.1 * t
    f1 = atars.holt_winters_forecast(series, seasonal_periods=7, forecast_days=14)
    f2 = atars.holt_winters_forecast(series, seasonal_periods=7, forecast_days=14)
    assert f1.get("available", True) is not False
    assert len(f1["forecast"]) == 14
    assert np.allclose(f1["forecast"], f2["forecast"])   # deterministic


def test_holt_winters_guards_short_series():
    f = atars.holt_winters_forecast(np.arange(5, dtype=float), seasonal_periods=7)
    assert f["available"] is False


def test_column_normalization_synonyms():
    df = pd.DataFrame({"PM10 (ug/m3)": [1, 2], "Timestamp": ["2024-01-01", "2024-01-02"]})
    out = atars.normalize_columns(df)
    # normalisation must not silently drop columns
    assert out.shape[0] == 2
    assert out.shape[1] >= 2


if __name__ == "__main__":
    # Minimal runner so the suite works without pytest installed.
    fns = [v for k, v in sorted(globals().items())
           if k.startswith("test_") and callable(v)]
    passed = failed = 0
    for fn in fns:
        try:
            fn()
            print(f"PASS  {fn.__name__}")
            passed += 1
        except Exception as e:  # noqa: BLE001
            print(f"FAIL  {fn.__name__}: {e}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)
