"""Plots from results/cells.csv: skill vs horizon per regime, coverage-width frontier.

Run after eval.run_grid: .venv/bin/python -m eval.plots
"""

from __future__ import annotations

import csv
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"

MODELS = ("B0", "B0g", "B1", "B2", "B2c", "B3", "oracle")
COLORS = {"B0": "#999999", "B0g": "#555555", "B1": "#e6a13c", "B2": "#4c9f70",
          "B2c": "#2e7d54", "B3": "#3b6fb6", "oracle": "#000000"}


def load_cells():
    rows = []
    with open(RESULTS_DIR / "cells.csv") as f:
        for row in csv.DictReader(f):
            rows.append(row)
    return rows


def best_mode(rows, regime, n, t, model):
    """Lowest pinball@q95 among this model's modes with coverage >= 0.90, averaged over
    seeds."""
    agg = defaultdict(list)
    for r in rows:
        if (r["regime"], int(r["N"]), float(r["t"]), r["model"]) == (regime, n, t, model):
            agg[r["mode"]].append((float(r["pinball0.95"]), float(r["cov95"]),
                                   float(r["width95"])))
    best = None
    for mode, vals in agg.items():
        loss = sum(v[0] for v in vals) / len(vals)
        cov = sum(v[1] for v in vals) / len(vals)
        width = sum(v[2] for v in vals) / len(vals)
        if cov < 0.90:
            continue
        if best is None or loss < best[0]:
            best = (loss, cov, width)
    return best


def main():
    rows = load_cells()
    regimes = sorted({r["regime"] for r in rows})
    ns = sorted({int(r["N"]) for r in rows})
    ts = sorted({float(r["t"]) for r in rows})
    n_plot = max(n for n in ns if n >= 100) if any(n >= 100 for n in ns) else max(ns)

    # Skill vs horizon per regime (vs best trivial at each point).
    fig, axes = plt.subplots(1, len(regimes), figsize=(4.2 * len(regimes), 3.6),
                             sharey=True)
    for ax, regime in zip(axes if len(regimes) > 1 else [axes], regimes):
        triv = {t: min(x[0] for x in filter(None, (
            best_mode(rows, regime, n_plot, t, m) for m in ("B0", "B0g")))) for t in ts}
        for model in ("B1", "B2", "B2c", "B3", "oracle"):
            xs, ys = [], []
            for t in ts:
                b = best_mode(rows, regime, n_plot, t, model)
                if b is None:
                    continue
                xs.append(t)
                ys.append(1.0 - b[0] / triv[t])
            ax.plot(xs, ys, marker="o", label=model, color=COLORS[model])
        ax.axhline(0, color="#cccccc", lw=1)
        ax.set_title(f"{regime} (N={n_plot})")
        ax.set_xlabel("horizon (s)")
    (axes[0] if len(regimes) > 1 else axes).set_ylabel("skill vs best trivial (q95 pinball)")
    (axes[-1] if len(regimes) > 1 else axes).legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(RESULTS_DIR / "skill_vs_horizon.png", dpi=140)

    # Coverage-width frontier at t=5s, verdict N.
    fig, axes = plt.subplots(1, len(regimes), figsize=(4.2 * len(regimes), 3.6))
    for ax, regime in zip(axes if len(regimes) > 1 else [axes], regimes):
        for model in MODELS:
            b = best_mode(rows, regime, n_plot, 5.0, model)
            if b is None:
                continue
            ax.scatter(b[1], b[2], color=COLORS[model], label=model)
            ax.annotate(model, (b[1], b[2]), fontsize=7,
                        xytext=(3, 3), textcoords="offset points")
        ax.axvline(0.95, color="#cccccc", lw=1)
        ax.set_title(f"{regime} t=5s (N={n_plot})")
        ax.set_xlabel("q95 coverage")
        ax.set_ylabel("q95 width (tokens)")
    fig.tight_layout()
    fig.savefig(RESULTS_DIR / "coverage_width.png", dpi=140)
    print(f"wrote {RESULTS_DIR}/skill_vs_horizon.png, coverage_width.png")


if __name__ == "__main__":
    main()
