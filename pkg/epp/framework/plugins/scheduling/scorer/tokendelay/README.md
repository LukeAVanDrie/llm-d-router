# Token Delay Scorer Plugin

**Type:** `token-delay-scorer` (Beta)

Scores candidate endpoints by minimizing expected token delay, combining in-flight token backlog, per-endpoint uncached prompt tokens, and a KV-cache congestion multiplier without requiring hand-tuned scorer weights.

## Overview

Instead of normalizing and summing incompatible signals—such as prefix-cache hit rate, queue length, and KV memory utilization—with hand-tuned weights, `token-delay-scorer` prices every routing trade-off in a single physical currency: **equivalent uncached token-steps of GPU work**. On any candidate endpoint $e$, dispatching a request $r$ with $u_e(r)$ uncached tokens incurs two directly comparable token-step costs:

1. **Immediate queueing and batch-stall delay**, $Q_e(t) + \big(1 + n_e(t)\big)\,\text{stepTokens}_e(r)$, which counts the in-flight prefill tokens queued ahead of $r$ plus its own uncached prefill work amplified by the $n_e(t)$ co-resident decode streams that are stalled while those tokens compute.
2. **Downstream KV eviction toll**, $\frac{\pi(t)}{(1-\pi(t))^2}\big(u_e(r) - u_{\min}(r)\big)$, which prices the cascading future cache misses triggered across the pool (at KV occupancy $\pi(t)$) whenever the router bypasses the warmest candidate ($u_{\min}(r)$) and allocates $\Delta u_e(r) = u_e(r) - u_{\min}(r)$ duplicate KV blocks on a colder endpoint.

Because both terms share the same token-step units, the scorer adds them directly—spreading load across idle endpoints when KV memory is plentiful ($\pi(t) \to 0$, where the eviction toll vanishes) and stiffening into cache affinity as KV memory fills up ($\pi(t) \to 1$, where the eviction multiplier diverges), only spilling to a colder endpoint when the warm endpoint's queue backlog physically exceeds the downstream eviction cost.

## Mathematical Formulation & Optimality

Minimizing $L_e(t, r)$ is not a heuristic linear combination; it is the **myopically system-optimal (Wardrop System Optimum) one-step routing policy** that minimizes the total marginal delay $\Delta \mathcal{D}_{\text{sys}}(e, r) = L_e(t, r) / \mu_e^{\text{pf}}$ imposed on the arriving request and all co-resident and future requests:

1. **Chunked-Prefill Delay Conservation ($\big(1 + n_e(t)\big)\,u_e(r)$)**:
   Under an affine step-time law $\Delta t_{\text{pf}}(s) = u_{e,s}(r) / \mu_e^{\text{pf}}$ (where $\mu_e^{\text{pf}}$ is the prefill token throughput), slicing $u_e(r)$ uncached tokens across any number of chunked-prefill iterations $k_{\text{pf}} \ge 1$ ($\sum_{s=1}^{k_{\text{pf}}} u_{e,s}(r) = u_e(r)$) delays $r$'s own first token by $\big(Q_e(t) + u_e(r)\big)/\mu_e^{\text{pf}}$ (**self TTFT**) while simultaneously stretching each of those $k_{\text{pf}}$ iterations for all $n_e(t)$ co-resident decoding sequences on endpoint $e$ by $\sum_{s=1}^{k_{\text{pf}}} n_e(t)\,u_{e,s}(r)/\mu_e^{\text{pf}} = n_e(t)\,u_e(r)/\mu_e^{\text{pf}}$ (**cross-request decode batch stall externality**). Summing both terms gives the chunk-invariant compute load:
   $$
   L_e^{\text{comp}}(t, r) = Q_e(t) + \big(1 + n_e(t)\big)\,u_e(r).
   $$
2. **Wardrop / Pigouvian KV Eviction Externality ($\kappa(t) - 1 = \frac{\pi(t)}{(1-\pi(t))^2}$)**:
   Endowing pooled KV cache occupancy $\pi \in [0, 1)$ with the normalized M/M/1-type hyperbolic barrier potential $\Phi(\pi) = \frac{1}{1 - \pi}$, the marginal social externality (under Wardrop's System Optimum and Naor's toll) of allocating duplicate KV blocks $\Delta u_e(r) = u_e(r) - u_{\min}(r) > 0$ on a colder endpoint is $\pi(t)\,\Phi'\big(\pi(t)\big) = \frac{\pi(t)}{(1 - \pi(t))^2}$. Adding this shadow price $\kappa(t) - 1$ to the baseline block cost prices the expected cascading cache-miss recomputation work imposed on future arrivals.
3. **Unique Regime Crossover ($\pi^*$) and Boundary Limits**:
   Because $\kappa'(\pi) = \frac{1 + \pi}{(1 - \pi)^3} > 0$ is strictly increasing on $\pi \in [0, 1)$ with $\kappa(0) - 1 = 0$ and $\lim_{\pi \to 1^-} \big(\kappa(\pi) - 1\big) = +\infty$, for any cache-warm endpoint $e_{\text{warm}}$ and cache-cold endpoint $e_{\text{cold}}$ with $u_{\text{warm}} < u_{\text{cold}}$ and $L_{e_{\text{cold}}}^{\text{comp}} < L_{e_{\text{warm}}}^{\text{comp}}$, there exists a **unique** crossover occupancy $\pi^* \in (0, 1)$ solving
   $$
   \kappa(\pi^*) - 1 = \frac{L_{e_{\text{warm}}}^{\text{comp}}(t, r) - L_{e_{\text{cold}}}^{\text{comp}}(t, r)}{u_{\text{cold}}(r) - u_{\text{warm}}(r)},
   $$
   below which ($\pi(t) < \pi^*$) the scorer spreads load to $e_{\text{cold}}$ and above which ($\pi(t) > \pi^*$) it pins to $e_{\text{warm}}$.
4. **Foster–Lyapunov Queue Stability**:
   Because $n_e(t)$, $u_e(r)$, and $\kappa(t)$ (clamped at $\pi(t) \le 0.999$) are bounded independent of $Q_e(t)$, $L_e(t, r)$ is a bounded perturbation of Join-the-Shortest-Queue-Work ($\arg\min_e Q_e(t)/\mu_e^{\text{pf}}$), guaranteeing negative quadratic Foster–Lyapunov drift on $V(\mathbf{Q}) = \frac{1}{2}\sum_e \frac{1}{\mu_e^{\text{pf}}}Q_e(t)^2$ (throughput-optimality across the capacity region $\lambda\bar{u} < \sum_e \mu_e^{\text{pf}}$).

## What it does

For a request $r$ and candidate endpoints $\mathcal{E}$, the plugin reads:
- $Q_e(t)$ and $n_e(t)$ from `InFlightLoad` (`Tokens` and `Requests`).
- $u_e(r)$ from `UncachedRequestTokens` (`Tokens`).
- `KVCacheUsagePercent` (and `KvCacheMaxTokenCapacity` when positive) from each endpoint with updated metrics to compute per-endpoint effective KV occupancy $\pi_e(t) \in [0, 1]$ (including in-flight tokens $Q_e(t) / \text{KvCacheMaxTokenCapacity}_e$ when capacity is known), the pooled KV occupancy $\pi(t) \in [0, 0.999]$, and the congestion multiplier:

$$
\kappa(t) = 1.0 + \frac{\pi(t)}{\big(1.0 - \pi(t)\big)^2}
$$

With $u_{\min}(r) = \min_{f \in \mathcal{E}} u_f(r)$ and $\text{stepTokens}_e(r) = u_e(r)$ (or $1$ when $u_e(r) = 0$ and $n_e(t) > 0$), the effective token delay load $L_e(t, r)$ on endpoint $e$ is:

$$
L_e(t, r) = Q_e(t) + \big(1 + n_e(t)\big)\,\text{stepTokens}_e(r) + \big(\kappa(t) - 1\big)\big(u_e(r) - u_{\min}(r)\big)
$$

Scores are normalized to $[0, 1]$ by candidate maximum load $L_{\max}(t, r) = \max_{f \in \mathcal{E}} L_f(t, r)$:

$$
\text{score}(e) = \begin{cases}
1.0 & \text{if } L_{\max}(t, r) \le 0 \text{ or } L_e(t, r) \le 0, \\
1.0 - \dfrac{L_e(t, r)}{L_{\max}(t, r)} & \text{otherwise.}
\end{cases}
$$

When all candidates have identical uncached tokens ($u_e(r) = u_{\min}(r)$ for all $e \in \mathcal{E}$, such as cold requests) and equal token delay load $L_e(t, r)$, ties are broken by per-endpoint effective KV occupancy $\pi_e(t)$ so endpoints with more free KV headroom score higher.

## Inputs consumed

- Required:
  - `attrconcurrency.InFlightLoadDataKey` (`*attrconcurrency.InFlightLoad`)
  - `attrconcurrency.UncachedRequestTokensDataKey` (`*attrconcurrency.UncachedRequestTokens`)
- Optional:
  - `KVCacheUsagePercent` and `KvCacheMaxTokenCapacity` from `core-metrics-extractor`

## Configuration

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `inFlightLoadProducerName` | `string` | No | `""` | Producer instance name for `InFlightLoad` and `UncachedRequestTokens`. |

```yaml
plugins:
  - type: token-delay-scorer
    name: token-delay
schedulingProfiles:
  - name: default
    plugins:
      - pluginRef: token-delay
        weight: 1
```
