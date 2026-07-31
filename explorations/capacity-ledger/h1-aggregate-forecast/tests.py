"""Statistics unit tests. Run: .venv/bin/python -m tests"""

from __future__ import annotations

import sys

import numpy as np

from estimators import base as estbase
from estimators.b3_km import B3DiscreteHazardKM
from estimators.baselines import B2cCensoredLognormal, B2Lognormal, OraclePerLease
from estimators.conformal import ConformalWrapper, rolling_quantile
from estimators.mixture_em import B4LognormalMixtureEM
from eval import metrics
from sim import simulator, workloads

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


def test_b4_em_recovery():
    """EM must recover the R3-class two-component mixture, uncensored and cap-censored."""
    rng = np.random.default_rng(6)
    true = workloads.LognormalMixture((0.6, 0.4), (80.0, 2000.0), (0.6, 0.6))
    x = true.sample(rng, 50_000)
    m = B4LognormalMixtureEM(None).fit(x)
    order = np.argsort(m.mu)
    w, mu, sig = m.w[order], m.mu[order], m.sigma[order]
    check("B4 recovers w", abs(w[0] - 0.6) < 0.02, f"w={w[0]:.3f}")
    check("B4 recovers mu", max(abs(mu[0] - np.log(80)), abs(mu[1] - np.log(2000))) < 0.05)
    check("B4 recovers sigma", max(abs(sig[0] - 0.6), abs(sig[1] - 0.6)) < 0.05)
    grid = np.quantile(x, np.linspace(0.05, 0.99, 40)).astype(np.float64)
    err = float(np.abs(m.survival(grid) - true.survival(grid)).max())
    check("B4 survival vs true", err < 0.01, f"max|dS|={err:.4f}")

    cap = 4096
    xc = np.minimum(true.sample(rng, 50_000), cap)
    mc = B4LognormalMixtureEM(cap).fit(xc)
    naive = B4LognormalMixtureEM(None).fit(xc)
    capped = workloads.Capped(true, cap)
    grid = np.quantile(xc[xc < cap], np.linspace(0.05, 0.99, 40)).astype(np.float64)
    err_c = float(np.abs(mc.survival(grid) - capped.survival(grid)).max())
    err_n = float(np.abs(naive.survival(grid) - capped.survival(grid)).max())
    check("B4 censored survival", err_c < 0.01, f"cens {err_c:.4f} vs naive {err_n:.4f}")


def test_rolling_conformal_honesty():
    """The rolling quantile at forecast time s must use only residuals realized by s:
    poisoning every residual that realizes after s must not change the answer."""
    times = np.arange(0.0, 200.0)
    horizon = 10.0
    rng = np.random.default_rng(8)
    resid = rng.normal(0, 1, len(times))
    eval_times = np.array([150.0])
    q_clean = rolling_quantile(times, horizon, resid, eval_times, 60.0, 0.95)
    poisoned = resid.copy()
    poisoned[times + horizon > eval_times[0]] = 1e9
    q_pois = rolling_quantile(times, horizon, poisoned, eval_times, 60.0, 0.95)
    check("rolling honesty", q_clean[0] == q_pois[0], f"{q_clean[0]:.3f} vs {q_pois[0]:.3f}")
    # Window content: realization times in (90, 150] -> snapshot times in (80, 140].
    expect = float(np.quantile(resid[(times > 80.0) & (times <= 140.0)], 0.95,
                               method="higher"))
    check("rolling window bounds", q_clean[0] == expect)


def test_drift_mix_and_per_lease_oracle():
    """TwoComponentMix survival matches its sampler; OraclePerLease equals per-component
    conditional survival routed by component id."""
    params = workloads.load_params()
    a, _ = workloads.build_regime("R1", params)
    b, _ = workloads.build_regime("R2", params)
    mix = workloads.TwoComponentMix(a, b, 0.25)
    rng = np.random.default_rng(9)
    x = mix.sample(rng, 2_000_000)
    grid = np.unique(np.quantile(x, np.linspace(0.02, 0.99, 40)).astype(np.int64))
    err = float(np.abs((x[None, :] > grid[:, None]).mean(axis=1)
                       - mix.survival(grid.astype(np.float64))).max())
    check("two-component mix survival", err < 0.005, f"max|dS|={err:.5f}")

    ages = np.array([50.0, 50.0, 800.0, 800.0])
    comp = np.array([0, 1, 0, 1], dtype=np.int8)
    snap = simulator.Snapshot(0.0, ages, ages, ages * 0, ages, 0.0, {}, comp=comp)
    p = OraclePerLease(a, b).p_alive_leases(snap, 200.0)
    for i, dist in enumerate([a, b, a, b]):
        want = (dist.survival(np.array([ages[i] + 200.0]))
                / dist.survival(np.array([ages[i]])))[0]
        check(f"per-lease oracle lease{i}", abs(p[i] - want) < 1e-12)


def test_steady_warmup_unbiased():
    """Steady-rule training completions must be length-unbiased where the legacy rule
    truncates: R2/N=1000 training max must exceed the true q95 (legacy max sits far
    below it, eval/diag_training_bias.py)."""
    params = workloads.load_params()
    dist, _ = workloads.build_regime("R2", params)
    ref = dist.sample(np.random.default_rng(10), 200_000)
    pilot = simulator.run_cell("R2", 1000, 999, params, capacity=None)
    res = simulator.run_cell("R2", 1000, 0, params, capacity=pilot.capacity,
                             warmup_rule="steady", calib_window_s=1.0, eval_window_s=1.0)
    tl = res.training_lengths
    true_mean, true_q95 = float(ref.mean()), float(np.quantile(ref, 0.95))
    check("steady warmup mean", abs(tl.mean() - true_mean) / true_mean < 0.25,
          f"train {tl.mean():.0f} vs true {true_mean:.0f}")
    check("steady warmup tail", float(tl.max()) > true_q95,
          f"max {tl.max():.0f} vs q95 {true_q95:.0f}")


if __name__ == "__main__":
    for t in (test_pinball, test_aggregation_vs_mc, test_conformal_coverage,
              test_b2c_censored_recovery, test_b3_matches_empirical,
              test_b4_em_recovery, test_rolling_conformal_honesty,
              test_drift_mix_and_per_lease_oracle, test_steady_warmup_unbiased,
              test_oracle_vs_empirical):
        print(t.__name__)
        t()
    if FAILURES:
        print(f"\n{len(FAILURES)} FAILURES: {FAILURES}")
        sys.exit(1)
    print("\nall tests passed")
