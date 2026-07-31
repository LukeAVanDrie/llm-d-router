"""Statistics unit tests. Run: .venv/bin/python -m tests"""

from __future__ import annotations

import sys

import numpy as np

from estimators import base as estbase
from estimators.b3_km import B3DiscreteHazardKM
from estimators.baselines import B2cCensoredLognormal, B2Lognormal
from estimators.conformal import ConformalWrapper
from eval import metrics
from sim import workloads

FAILURES = []


def check(name, cond, detail=""):
    status = "ok" if cond else "FAIL"
    print(f"  [{status}] {name} {detail}")
    if not cond:
        FAILURES.append(name)


def test_oracle_vs_empirical():
    """Analytic survival must match the empirical survival of its own sampler.
    Evaluated at integer points, where ceil-sampling makes the continuous survival exact."""
    params = workloads.load_params()
    rng = np.random.default_rng(7)
    for regime in ("R1", "R2", "R3", "R4"):
        dist, _ = workloads.build_regime(regime, params)
        x = dist.sample(rng, 10_000_000)
        grid = np.unique(np.quantile(x, np.linspace(0.01, 0.995, 60)).astype(np.int64))
        emp = (x[None, :] > grid[:, None]).mean(axis=1)
        ana = dist.survival(grid.astype(np.float64))
        err = float(np.abs(emp - ana).max())
        check(f"oracle S {regime}", err < 0.005, f"max|dS|={err:.5f}")


def test_pinball():
    rng = np.random.default_rng(1)
    y = rng.normal(0, 1, 200_000)
    # The pinball-optimal constant predictor is the q-quantile; check it beats offsets.
    for q in (0.5, 0.95):
        opt = float(np.quantile(y, q))
        l_opt = metrics.pinball(y, np.full_like(y, opt), q)
        l_off = min(metrics.pinball(y, np.full_like(y, opt + 0.3), q),
                    metrics.pinball(y, np.full_like(y, opt - 0.3), q))
        check(f"pinball optimality q={q}", l_opt < l_off, f"{l_opt:.4f} < {l_off:.4f}")


def test_b3_matches_empirical():
    """With shrinkage off, the discrete-hazard fit must track empirical survival."""
    rng = np.random.default_rng(2)
    dist, _ = workloads.build_regime("R3", workloads.load_params())
    x = dist.sample(rng, 50_000)
    m = B3DiscreteHazardKM(prior_obs=0.0).fit(x)
    grid = np.quantile(x, np.linspace(0.05, 0.95, 30)).astype(np.float64)
    emp = (x[None, :] > grid[:, None]).mean(axis=1)
    fit = m.survival(grid)
    err = float(np.abs(emp - fit).max())
    check("B3 vs empirical survival", err < 0.03, f"max|dS|={err:.4f}")


def test_b2c_censored_recovery():
    """Censored MLE must recover lognormal params from cap-censored data far better than
    the naive fit that treats cap-hits as complete."""
    rng = np.random.default_rng(3)
    mu, sigma, cap = np.log(600.0), 1.2, 1024
    x = np.minimum(np.ceil(rng.lognormal(mu, sigma, 40_000)), cap).astype(np.int64)
    naive = B2Lognormal().fit(x)
    cens = B2cCensoredLognormal(cap).fit(x)
    check("B2c recovers mu", abs(cens.mu - mu) < 0.05,
          f"cens err {abs(cens.mu - mu):.3f} vs naive {abs(naive.mu - mu):.3f}")
    check("B2c recovers sigma", abs(cens.sigma - sigma) < 0.05,
          f"cens err {abs(cens.sigma - sigma):.3f} vs naive {abs(naive.sigma - sigma):.3f}")


def test_aggregation_vs_mc():
    """Normal-approx mean/sd must match Monte Carlo on a synthetic lease set."""
    rng = np.random.default_rng(4)
    n = 400
    p = rng.random(n)
    vals = rng.lognormal(6, 1, n)
    mean = (p * vals).sum()
    sd = float(np.sqrt((vals * vals * p * (1 - p)).sum()))
    draws = ((rng.random((20_000, n)) < p) @ vals)
    check("aggregate mean", abs(draws.mean() - mean) / mean < 0.01)
    check("aggregate sd", abs(draws.std() - sd) / sd < 0.05)
    q95_mc = np.quantile(draws, 0.95)
    from scipy import stats
    q95_na = mean + stats.norm.ppf(0.95) * sd
    check("aggregate q95 normal~MC", abs(q95_na - q95_mc) / sd < 0.15,
          f"na={q95_na:.0f} mc={q95_mc:.0f}")


def test_conformal_coverage():
    rng = np.random.default_rng(5)
    point_c, point_e = np.zeros(2000), np.zeros(5000)
    real_c = rng.normal(0.5, 2.0, 2000)   # biased, non-unit-scale residuals
    real_e = rng.normal(0.5, 2.0, 5000)
    cw = ConformalWrapper()
    cw.calibrate(("m", 1.0), point_c, real_c, (0.5, 0.95))
    upper = point_e + cw.residual_q[("m", 1.0)][0.95]
    cov = float((real_e <= upper).mean())
    check("conformal coverage", 0.94 <= cov <= 0.96, f"cov={cov:.3f}")


if __name__ == "__main__":
    for t in (test_pinball, test_aggregation_vs_mc, test_conformal_coverage,
              test_b2c_censored_recovery, test_b3_matches_empirical,
              test_oracle_vs_empirical):
        print(t.__name__)
        t()
    if FAILURES:
        print(f"\n{len(FAILURES)} FAILURES: {FAILURES}")
        sys.exit(1)
    print("\nall tests passed")
