# Theory notes: the capacity ledger's mathematical spine

Closed-form results underwriting the design in [ledger-revision.md](ledger-revision.md).
Each result states its assumptions, the design element it supports, and where the
assumption is stressed — with the measured evidence that covers the gap when it breaks.
Notation is ASCII throughout. Standings per [STYLE.md](STYLE.md): every proof here is a
derivation shown in place; named constants from the literature are marked with their
standing where they appear.

## Setup

A pool holds N active leases at forecast time. Lease i has age n_i (tokens generated),
current footprint f_i (prompt plus generated tokens), and a fixed known decode rate r
(tokens/s; the phase-1 attribution choice — see "Where the assumptions break"). Its
latent total output length L_i is drawn from its stratum's distribution with survival
function S(n) = P(L > n); leases are independent. With a generation cap (max_tokens),
the effective length is min(L_i, cap) and S is zero at and beyond the cap.

The forecast target is occupancy at horizon t attributable to currently active leases:

    X(t) = sum_i  v_i * A_i,    v_i = f_i + r*t,    A_i = 1[L_i > n_i + r*t].

Admission consumers read a one-sided upper quantile of X(t).

## Results

**Lemma 1 (conditional survival).** Given that lease i is alive at age n_i, the
probability it remains alive at the horizon is

    p_i = P(L_i > n_i + r*t | L_i > n_i) = S(n_i + r*t) / S(n_i).

*Proof.* The event {L > n + r*t} is contained in {L > n}, so the conditional
probability is the ratio of the two survival values. QED.

Design element: the per-lease probability every forecast is built from
(`estimators/base.py` implements exactly this ratio).

**Proposition 2 (pool moments).** Under lease independence,

    E[X]   = sum_i p_i * v_i,
    Var[X] = sum_i v_i^2 * p_i * (1 - p_i).

*Proof.* Each term v_i * A_i is a scaled Bernoulli with mean p_i v_i and variance
v_i^2 p_i (1 - p_i); independence makes variances additive. QED.

Design element: the mean/variance aggregation behind native quantiles.

**Proposition 3 (why aggregate forecasting works where per-request prediction fails).**
Suppose footprints are bounded, v_i <= v_max, and the pool mean is proportional to its
size, E[X] >= c*N for some c > 0 (true when a constant fraction of leases survives with
bounded-below footprints). Then the coefficient of variation obeys

    sd(X) / E[X]  <=  (v_max / (2c)) * N^{-1/2},

so the relative width of any fixed-z quantile band shrinks as N^{-1/2}. A single
lease's contribution has sd/mean = sqrt((1-p)/p), a constant that no estimator can
reduce.

*Proof.* Var[X] <= sum v_i^2 / 4 <= N v_max^2 / 4 since p(1-p) <= 1/4; take square
roots and divide by E[X] >= cN. For one lease, the Bernoulli ratio is immediate. QED.

Design element: the per-request-probability vs point-prediction distinction — the
aggregate discount is consumed summed, where errors cancel at rate sqrt(N); a
per-lease probability is calibrated on average and wrong for any individual.

**Proposition 4 (normal-approximation error).** With Y_i = v_i * A_i independent and
sigma^2 = Var[X], the Berry-Esseen inequality for non-identical summands gives

    sup_x | P((X - E[X])/sigma <= x) - Phi(x) |  <=  C0 * v_max / sigma,

where Phi is the standard normal CDF and C0 is an absolute constant (C0 <= 0.56 per
the literature on the non-i.i.d. Berry-Esseen constant; the constant's exact value is
recalled, unverified — any C0 <= 1 preserves the conclusion below).

*Proof sketch.* The third absolute central moment of a scaled Bernoulli satisfies
E|Y_i - E Y_i|^3 = v_i^3 p_i (1-p_i) (p_i^2 + (1-p_i)^2) <= v_i^3 p_i (1-p_i). Then
sum E|Y_i - E Y_i|^3 / sigma^3 <= v_max * sum v_i^2 p_i (1-p_i) / sigma^3
= v_max / sigma, and Berry-Esseen applies. QED.

Consequence: the CDF error falls as 1/sigma, i.e. as N^{-1/2} at fixed composition.
Design element: the harness's normal-approximation convention at N >= 100, gated
empirically by the MC-vs-normal test and the oracle coverage band; this bound says why
the gate passes and when it would not (small pools, or one lease dominating v_max).

**Theorem 5 (split-conformal validity — the safety layer's guarantee).** Let a point
forecaster produce residuals R_1, ..., R_m on a calibration window and R_{m+1} on the
forecast being scored, and suppose the m+1 residuals are exchangeable. Let q_hat be
the k-th order statistic of R_1..R_m with k = ceil((m+1)(1-alpha)) (the "higher"
empirical quantile). Then

    P( R_{m+1} <= q_hat )  >=  1 - alpha,

with the matching upper bound 1 - alpha + 1/(m+1) when residuals are almost surely
distinct. No assumption is made about the survival model, the length distribution, or
cross-lease dependence.

*Proof.* By exchangeability, the rank of R_{m+1} among the m+1 residuals is uniform on
{1, ..., m+1}. The event R_{m+1} <= q_hat contains the event that its rank is at most
k, which has probability k/(m+1) >= 1 - alpha. The upper bound is (k)/(m+1) with k the
smallest integer >= (m+1)(1-alpha). QED.

Two consequences the design leans on:

- The survival model affects sharpness (bound width), never validity. A wrong model
  yields wide bounds that still cover; this is why the degraded mode (conformal on trivial
  growth) is a safe fallback and why estimator complexity was killable on skill alone.
- Cross-lease dependence does not break this guarantee: exchangeability is required of
  the residual sequence over time, not of leases within a pool. The safety layer
  therefore rests on strictly weaker assumptions than the moment calculations above —
  bursty, correlated pools stress Propositions 2-4 (variance underestimation, slower
  CLT) but not Theorem 5. What does break it is the next result.

**Proposition 6 (no calibration through an unobserved shift; the fallback is
mandatory).** Fix any rule that maps past observations to an upper bound q_hat with
q_hat < sum_i v_i (any bound tighter than the deterministic ceiling). There exists a
shift of the output-length distribution, occurring after the calibration data was
collected, under which the realized coverage at the shift is 0.

*Proof.* The bound q_hat is a function of past data only. Shift the output
distribution so that all mass lies beyond every lease's horizon threshold
n_i + r*t; then no lease completes by the horizon, X(t) = sum_i v_i with probability
1, and the bound is exceeded surely. QED.

The only rule immune to this construction is the one that always outputs the
deterministic ceiling — which is the fallback itself. Calibration buys sharpness on
the distributions it has seen; the ceiling is the unique shift-proof bound.

Design element: drift is a detected regime, not a forecastable one. This proposition
is the in-principle form; the measured form is KD (frozen fits lose coverage outright)
and KR (even continuous refit cannot re-validate until shifted data is observed). The
canary plus fallback is not a concession but the only coherent response.

**Proposition 7 (age-conditioning has value exactly when lengths are not memoryless).**
The mean residual life m(n) = E[L - n | L > n] is constant in n if and only if L is
geometric (discrete) or exponential (continuous), equivalently iff the hazard is
constant, equivalently iff S(n + k) = S(n) * S(k) for all n, k >= 0.

*Proof.* If S(n+k) = S(n)S(k), survival beyond age n has the same law as survival from
zero, so m is constant. Conversely, constant hazard h gives S(n) = (1-h)^n (or
e^{-hn}), the unique solutions of the multiplicative functional equation under
monotonicity. QED.

Design element: the justification for conditioning on decode age at all. LLM output
lengths are strongly non-memoryless (modes near typical response lengths, heavy right
tails), and the measured gap between the age-blind constant-hazard fit and the
age-conditioned lognormal (B1 vs B2 across the round-2 verdict grid) is this
proposition made quantitative.

**Proposition 8 (one-sided error of the deterministic ledger).** Suppose each lease's
booking is b_i = prompt_i - cached_i + cap_i * branching_i (output side booked at its
ceiling) and the engine enforces generation <= cap_i. Then at every instant the booked
total is an upper bound for the realized total of admitted work; consequently any
booking-estimation error causes under-admission, never over-admission. Separately,
revoking lease i frees exactly its current measured footprint f_i, so eviction sizing
incurs no estimation error.

*Proof.* Pointwise, a lease's realized occupancy never exceeds its prompt-side
measurement plus its enforced generation ceiling; sum over leases. The revocation
claim is definitional: the freed quantity is the current footprint, a measured value.
QED.

Design element: "layer 1 stands alone"; the utilization cost of loose ceilings is the
stochastic layer's yield, and safety does not depend on estimate quality.

**Proposition 9 (the prefill backlog gate).** If an endpoint prefills at sustained
rate mu_p uncached prompt tokens per second and serves its backlog work-conservingly,
then capping the admitted-but-not-yet-prefilled backlog at Q <= mu_p * W bounds every
admitted request's prefill queueing delay by W seconds.

*Proof.* A newly admitted request finds at most Q tokens ahead of it; a
work-conserving server clears them in at most Q / mu_p <= W seconds. QED.

Design element: the rate-gate formulation of the prefill axis; it converts the
existing token-mode constant into an operator quantity stated in seconds
(Q_p_max = mu_p * TTFT_budget).

**Theorem 10 (the confidence dial's setting rule is the newsvendor fractile).**
Reserve capacity q against an uncertain requirement Z with CDF F, at per-unit waste
cost c_w when q exceeds Z and per-unit revocation cost c_r when Z exceeds q. The
expected cost E[ c_w (q - Z)^+ + c_r (Z - q)^+ ] is minimized at

    q* = F^{-1}( rho / (rho + 1) ),    rho = c_r / c_w.

*Proof.* The objective is convex in q with subgradient c_w F(q) - c_r (1 - F(q));
setting it to zero gives F(q*) = c_r / (c_r + c_w) = rho / (rho + 1). QED.

Design element: the dial's operational meaning — the quantile is not a style choice
but the cost-ratio optimum; arXiv:2607.16892 (Proposition IV.2, read; sources.md
`dro2026`) is this classical result instantiated for admission-time KV reservation,
and its no-single-optimal-quantile finding is the corollary that rho varies by
deployment.

**Proposition 11 (a distribution-free conservative bound for the dial's guaranteed
end).** With independent contributions 0 <= Y_i <= v_max, sigma^2 = Var[X], and any
delta in (0,1), Bernstein's inequality gives

    P( X  >=  E[X] + s(delta) )  <=  delta,
    s(delta) = sqrt( 2 sigma^2 ln(1/delta) )  +  (2/3) v_max ln(1/delta),

(the stated s(delta) upper-inverts the standard Bernstein tail
exp( -s^2 / (2 sigma^2 + (2/3) v_max s) )).

*Proof.* Bernstein's inequality for bounded independent summands gives the tail above;
it suffices to show the stated s satisfies s^2 >= L * (2 sigma^2 + (2/3) v_max s) with
L = ln(1/delta). Write a = 2 sigma^2 L and b = (2/3) v_max L, so the requirement is
s^2 - b*s - a >= 0, and s = sqrt(a) + b. Then

    s^2 - b*s - a = (a + 2b*sqrt(a) + b^2) - (b*sqrt(a) + b^2) - a = b*sqrt(a) >= 0.

QED.

Design element: a quantile between the calibrated q95 and the absolute ceiling that
needs no calibration window at all — usable during the uncalibrated periods the
transient discipline and cold start create, wider than conformal but valid under
independence alone. This absorbs the field inventory's "Bernstein-bound note" item.

## Where the assumptions break, and what covers the gap

| Assumption | Used by | Breaks under | Covered by |
|---|---|---|---|
| Lease independence | Prop 2, 3, 4, 11 | burst-correlated lengths | Theorem 5 (conformal validity survives); L0 backlog sensitivity; BLIS re-score |
| Fixed known decode rate r | the target X(t) itself | load-coupled batching | BLIS re-score (L0.5); engine co-design telemetry |
| Residual exchangeability over time | Theorem 5 | any distribution shift (Prop 6) | canary + fallback (DD, KR); stratification for labeled shifts |
| Output side booked at an enforced ceiling | Prop 8 | engines not enforcing cap | engine-source verification (work table row 1) |
| Work-conserving prefill at measurable mu_p | Prop 9 | scheduler priorities preempting prefill | engine-source verification |
| Stationarity within stratum | everything statistical | within-stratum drift | KD/KR/DD verdicts; the operating contract |

The table is the theory-side twin of the assessment's standing table: the design's
safety claims route through Theorem 5, Proposition 6, and Proposition 8 — the three
results with the weakest assumptions — while every result with strong assumptions
(moments, CLT, Bernstein) prices only sharpness, never coverage.
