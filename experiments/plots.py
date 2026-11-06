"""Turn results/*.json into report figures (PNG, 200 dpi).

    python -m experiments.plots
"""
import glob
import json
import re
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from experiments.common import RESULTS

MODE_COLORS = {"serializable": "#2a6fdb", "pessimistic": "#e07b39", "optimistic": "#3a9e6e"}
REGION_COLORS = {"us-west": "#2a6fdb", "us-east": "#e07b39", "eu-west": "#3a9e6e"}


def _style(ax):
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", color="#e6e6e6", lw=0.8)
    ax.set_axisbelow(True)


def fig_e1():
    p = RESULTS / "e1_contention.json"
    if not p.exists():
        return
    d = json.load(open(p))
    agg = defaultdict(lambda: defaultdict(list))
    for r in d["rows"]:
        k = (r["mode"], r["workers"])
        agg[k]["p50"].append(r["latency"]["p50_ms"])
        agg[k]["p99"].append(r["latency"]["p99_ms"])
        agg[k]["retries"].append(r["retries_per_buy"])
        agg[k]["thr"].append(r["throughput_buys_per_s"])
    modes = [m for m in MODE_COLORS if any(k[0] == m for k in agg)]
    workers = sorted({k[1] for k in agg})
    fig, ax = plt.subplots(1, 3, figsize=(13, 3.8))
    for m in modes:
        for i, (key, title, unit) in enumerate([("p99", "(a) p99 latency per purchase", "ms"),
                                                ("retries", "(b) Retries per successful purchase", "retries"),
                                                ("thr", "(c) Purchases committed per second", "buys/s")]):
            ys = [np.mean(agg[(m, w)][key]) for w in workers]
            lo = [np.mean(agg[(m, w)][key]) - min(agg[(m, w)][key]) for w in workers]
            hi = [max(agg[(m, w)][key]) - np.mean(agg[(m, w)][key]) for w in workers]
            ax[i].errorbar(workers, ys, yerr=[lo, hi], marker="o", capsize=3, label=m, color=MODE_COLORS[m])
            ax[i].set_title(title, fontsize=11)
            ax[i].set_ylabel(unit)
            ax[i].set_xlabel("concurrent buyers")
            ax[i].set_xscale("log", base=2)
            ax[i].set_xticks(workers, [str(w) for w in workers])
            _style(ax[i])
    ax[0].legend(frameon=False)
    fig.tight_layout()
    fig.savefig(RESULTS / "fig_e1_contention.png", dpi=200)
    plt.close(fig)


def fig_e3():
    p = RESULTS / "e3_failover.json"
    if not p.exists():
        return
    d = json.load(open(p))
    tl = d["timeline"][:-1]
    t = [r["t"] for r in tl]
    ok = [r["ok"] for r in tl]
    fail = [r["fail"] for r in tl]
    sr = [100 * r["success_rate"] if r["success_rate"] is not None else np.nan for r in tl]
    fig, ax = plt.subplots(2, 1, figsize=(10, 5.2), sharex=True, gridspec_kw={"height_ratios": [2, 1.2]})
    ax[0].bar(t, ok, width=1.0, color="#2a6fdb", label="succeeded")
    ax[0].bar(t, fail, bottom=ok, width=1.0, color="#d64545", label="failed")
    ax[0].set_ylabel("requests per second")
    ax[0].legend(frameon=False, loc="lower left")
    ax[1].plot(t, sr, color="#222")
    ax[1].set_ylabel("success rate (%)")
    ax[1].set_xlabel("seconds since start")
    ax[1].set_ylim(min(90, np.nanmin(sr) - 2) if np.isfinite(np.nanmin(sr)) else 0, 101)
    for e, ts in d["events"]:
        for a in ax:
            a.axvline(ts, color="#888", ls="--", lw=1)
        ax[0].text(ts + 0.5, ax[0].get_ylim()[1] * 0.92, f"{d['node_killed']} {'killed' if e == 'kill' else 'restarted'}",
                   fontsize=9, color="#444")
    for a in ax:
        _style(a)
    fig.tight_layout()
    fig.savefig(RESULTS / "fig_e3_failover.png", dpi=200)
    plt.close(fig)


def fig_e4():
    files = sorted(glob.glob(str(RESULTS / "e4_follower_*.json")))
    if not files:
        return
    rows = []
    for f in files:
        d = json.load(open(f))
        m = re.search(r"rtt(\d+)", d["label"])
        rows.append((int(m.group(1)) if m else 0, d))
    rows.sort(key=lambda x: x[0])
    rtt = [r[0] for r in rows]
    fig, ax = plt.subplots(1, 2, figsize=(11, 3.8))
    for kind, color in (("strong", "#d64545"), ("follower", "#2a6fdb")):
        ax[0].plot(rtt, [r[1]["heartbeat_latency"][kind]["p50_ms"] for r in rows], "o-", color=color,
                   label=f"{kind} read p50")
        ax[0].plot(rtt, [r[1]["heartbeat_latency"][kind]["p99_ms"] for r in rows], "o--", color=color, alpha=0.6,
                   label=f"{kind} read p99")
        ax[1].plot(rtt, [r[1]["staleness_s"][kind]["mean"] for r in rows], "o-", color=color, label=f"{kind} read")
    ax[0].set_title("(a) Read latency from an eu-west gateway", fontsize=11)
    ax[0].set_ylabel("ms")
    ax[1].set_title("(b) Staleness of the value read", fontsize=11)
    ax[1].set_ylabel("seconds behind the latest write")
    for a in ax:
        a.set_xlabel("injected inter-region round-trip time (ms)")
        a.set_xticks(rtt)
        a.legend(frameon=False)
        _style(a)
    fig.tight_layout()
    fig.savefig(RESULTS / "fig_e4_follower.png", dpi=200)
    plt.close(fig)


def fig_ranges():
    p = RESULTS / "cluster_ranges.json"
    if not p.exists():
        return
    d = json.load(open(p))
    nodes = d["nodes"]
    reg = {}
    for n in nodes:
        m = re.search(r"region=([\w-]+)", n["locality"])
        reg[n["node_id"]] = m.group(1) if m else "?"
    tables = ["players", "wallets", "inventory", "farm_plots", "items", "shared_fields", "harvest_log",
              "item_catalog", "leaderboard"]
    tables = [t for t in tables if any(r["table"] == t for r in d["ranges"])]
    ids = [n["node_id"] for n in nodes]
    lease = np.zeros((len(tables), len(ids)))
    voters = np.zeros((len(tables), len(ids)))
    for r in d["ranges"]:
        if r["table"] not in tables:
            continue
        i = tables.index(r["table"])
        if r["lease_holder"] in ids:
            lease[i, ids.index(r["lease_holder"])] += 1
        for v in (r["voting"] or r["replicas"]):
            if v in ids:
                voters[i, ids.index(v)] += 1
    fig, ax = plt.subplots(1, 2, figsize=(13, 4.2))
    for a, mat, title in ((ax[0], voters, "(a) Voting replicas per node"),
                          (ax[1], lease, "(b) Leaseholders per node")):
        im = a.imshow(mat, cmap="Blues", aspect="auto")
        a.set_yticks(range(len(tables)), tables)
        a.set_xticks(range(len(ids)), [f"n{i}\n{reg[i]}" for i in ids], fontsize=8)
        a.set_title(title, fontsize=11)
        for (i, j), v in np.ndenumerate(mat):
            if v:
                a.text(j, i, int(v), ha="center", va="center", fontsize=7,
                       color="white" if v > mat.max() * 0.6 else "#222")
        fig.colorbar(im, ax=a, shrink=0.8, label="ranges")
    fig.tight_layout()
    fig.savefig(RESULTS / "fig_shard_placement.png", dpi=200)
    plt.close(fig)


if __name__ == "__main__":
    fig_e1()
    fig_e3()
    fig_e4()
    fig_ranges()
    print("figures written to", RESULTS)
