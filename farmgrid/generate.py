"""Synthetic data generation pipeline.

    python -m farmgrid.generate [--config config/default.yaml] [--out data] [--scale 1.0]

Stages: (1) read config + seed, (2) generate tables in dependency order,
(3) validate counts, keys and distributions, (4) write gzip CSVs plus a
generation report (JSON) and a data-profile figure.
Same config + same seed => same rows (timestamps are anchored to --anchor).
"""
import argparse
import csv
import gzip
import json
import math
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
from faker import Faker
from scipy import stats

from farmgrid.config import REGIONS, ROOT, load_config

GOLDEN_SEED = 0


def uuid_from(rng: np.random.Generator) -> str:
    return str(uuid.UUID(bytes=rng.bytes(16), version=4))


def zipf_probs(n: int, s: float) -> np.ndarray:
    ranks = np.arange(1, n + 1, dtype=float)
    w = ranks ** -s
    return w / w.sum()


def daily_cycle_weights() -> np.ndarray:
    """Relative activity by local hour: low overnight, lunch bump, evening peak."""
    h = np.arange(24)
    w = 0.15 + 0.45 * np.exp(-((h - 12.5) ** 2) / 6) + 1.0 * np.exp(-((h - 20.5) ** 2) / 5)
    return w / w.sum()


def generate(cfg: dict, scale: float, anchor: datetime) -> dict:
    rng = np.random.default_rng(cfg["seed"])
    fake = Faker()
    Faker.seed(cfg["seed"])
    sc = {k: (max(1, int(v * scale)) if isinstance(v, int) else v) for k, v in cfg["scale"].items()}
    region_names = list(cfg["regions"].keys())
    region_p = np.array(list(cfg["regions"].values()), dtype=float)
    region_p /= region_p.sum()
    t = {}

    # --- item_catalog --------------------------------------------------------
    cat = cfg["catalog"]
    rarities = list(cat["rarity_shares"].keys())
    rar_p = np.array(list(cat["rarity_shares"].values()))
    catalog = [(GOLDEN_SEED, "Golden Seed", "seed", "legendary", cfg["fields"]["golden_seed_price"], None)]
    for cid, (name, growth, _y) in cfg["crops"].items():
        catalog.append((int(cid), name, "crop", "common", int(5 + 3 * int(cid)), int(growth)))
    n_crops = len(cfg["crops"])
    for i in range(n_crops + 1, sc["catalog_items"] + 1):
        r = rarities[rng.choice(len(rarities), p=rar_p)]
        median = cat["rarity_value_median"][r]
        value = max(1, int(rng.lognormal(math.log(median), cat["value_sigma"])))
        kind = ["seed", "tool", "decor"][rng.integers(3)]
        catalog.append((i, f"{fake.word().title()} {kind.title()} {i}", kind, r, value, None))
    t["item_catalog"] = (["item_type_id", "name", "kind", "rarity", "base_value", "growth_seconds"], catalog)
    catalog_ids = np.array([c[0] for c in catalog if c[0] != GOLDEN_SEED])

    # --- players ---------------------------------------------------------------
    pc = cfg["players"]
    n = sc["players"]
    regions = rng.choice(len(region_names), size=n, p=region_p)
    levels = np.minimum(rng.geometric(pc["level_geometric_p"], size=n), pc["max_level"])
    pids = [uuid_from(rng) for _ in range(n)]
    created = [anchor - timedelta(days=float(d)) for d in rng.uniform(0, 365, size=n)]
    players = [(pids[i], f"{fake.user_name()}_{i}", region_names[regions[i]], int(levels[i]),
                created[i].isoformat(), created[i].isoformat()) for i in range(n)]
    t["players"] = (["player_id", "username", "home_region", "level", "created_at", "updated_at"], players)
    p_region = {pids[i]: region_names[regions[i]] for i in range(n)}
    level_w = levels / levels.sum()

    # --- wallets ---------------------------------------------------------------
    wc = cfg["wallets"]
    mu = np.log(wc["coins_log_median"]) + wc["coins_level_slope"] * levels
    coins = rng.lognormal(mu, wc["coins_log_sigma"]).astype(np.int64)
    t["wallets"] = (["player_id", "region", "coins"],
                    [(pids[i], p_region[pids[i]], int(coins[i])) for i in range(n)])

    # --- inventory (item types per player drawn by Zipf popularity) --------------
    zc = cfg["items"]["zipf_exponent"]
    perm = rng.permutation(catalog_ids)          # popularity rank -> item type id
    z_p = zipf_probs(len(perm), zc)
    ic = cfg["inventory"]
    inv = []
    for i in range(n):
        k = int(min(len(perm), max(1, rng.poisson(ic["types_per_player_mean"]))))
        types = rng.choice(perm, size=k, replace=False, p=z_p)
        qty = np.ceil(rng.lognormal(ic["quantity_log_mean"], ic["quantity_log_sigma"], size=k)).astype(int)
        for ty, q in zip(types, qty):
            inv.append((pids[i], int(ty), p_region[pids[i]], int(q)))
    t["inventory"] = (["player_id", "item_type_id", "region", "quantity"], inv)

    # --- items (individually owned; owner weighted by level) ----------------------
    owners = rng.choice(n, size=sc["items"], p=level_w)
    item_types = rng.choice(perm, size=sc["items"], p=z_p)
    acq = rng.uniform(0, 180, size=sc["items"])
    items = [(uuid_from(rng), int(item_types[j]), pids[owners[j]], p_region[pids[owners[j]]],
              (anchor - timedelta(days=float(acq[j]))).isoformat()) for j in range(sc["items"])]
    t["items"] = (["item_id", "item_type_id", "owner_id", "region", "acquired_at"], items)

    # --- farm_plots --------------------------------------------------------------
    plc = cfg["plots"]
    crops = {int(k): v for k, v in cfg["crops"].items()}
    crop_ids = np.array(sorted(crops))
    plots = []
    for i in range(n):
        for _ in range(plc["base"] + int(levels[i]) // plc["per_levels"]):
            if rng.random() < plc["planted_share"]:
                c = int(rng.choice(crop_ids))
                planted = anchor - timedelta(seconds=float(rng.uniform(0, 2 * crops[c][1])))
                ready = planted + timedelta(seconds=crops[c][1])
                plots.append((uuid_from(rng), pids[i], p_region[pids[i]], c,
                              planted.isoformat(), ready.isoformat(), 0))
            else:
                plots.append((uuid_from(rng), pids[i], p_region[pids[i]], None, None, None, 0))
    t["farm_plots"] = (["plot_id", "owner_id", "region", "crop_type_id", "planted_at", "ready_at", "version"], plots)

    # --- shared_fields (Golden Seed Drop) ----------------------------------------
    fc = cfg["fields"]
    nf = sc["shared_fields"]
    f_regions = rng.choice(len(region_names), size=nf, p=region_p)
    f_pop = zipf_probs(nf, fc["popularity_zipf_exponent"])
    stock = rng.integers(fc["stock_min"], fc["stock_max"] + 1, size=nf)
    fields = [(f + 1, region_names[f_regions[f]], GOLDEN_SEED, int(stock[f]), int(stock[f]),
               float(f_pop[f]), 0, 0) for f in range(nf)]
    t["shared_fields"] = (["field_id", "region", "item_type_id", "quantity_remaining", "initial_quantity",
                           "popularity", "epoch", "version"], fields)

    # --- harvest_log history (daily activity cycle in each region's local time) ---
    cyc = daily_cycle_weights()
    offs = cfg["utc_offset_hours"]
    nh = sc["harvest_history"]
    h_players = rng.choice(n, size=nh, p=level_w)
    h_hours = rng.choice(24, size=nh, p=cyc)
    h_days = rng.integers(1, 31, size=nh)
    h_mins = rng.uniform(0, 60, size=nh)
    h_crop = rng.choice(crop_ids, size=nh)
    day0 = anchor.replace(hour=0, minute=0, second=0, microsecond=0)
    harvests = []
    for j in range(nh):
        pid = pids[h_players[j]]
        reg = p_region[pid]
        local = day0 - timedelta(days=int(h_days[j])) + timedelta(hours=int(h_hours[j]), minutes=float(h_mins[j]))
        utc = local - timedelta(hours=offs[reg])
        q = max(1, int(rng.poisson(crops[int(h_crop[j])][2])))
        harvests.append((uuid_from(rng), pid, reg, "plot", "history", int(h_crop[j]), q, "history", utc.isoformat()))
    t["harvest_log"] = (["harvest_id", "player_id", "region", "source", "source_id", "item_type_id",
                         "quantity", "kind", "harvested_at"], harvests)

    # --- presence ------------------------------------------------------------------
    k = int(n * sc["presence_share"])
    online = rng.choice(n, size=k, replace=False)
    ttl = cfg["presence"]["ttl_minutes"]
    pres = []
    for i in online:
        seen = anchor - timedelta(seconds=float(rng.uniform(0, 60 * (ttl - 1))))
        pres.append((pids[i], p_region[pids[i]], "online", seen.isoformat(), (seen + timedelta(minutes=ttl)).isoformat()))
    t["presence"] = (["player_id", "region", "status", "last_seen", "expires_at"], pres)

    meta = {"zipf_rank_to_type": [int(x) for x in perm], "levels": levels, "coins": coins,
            "harvest_local_hours": h_hours, "harvest_regions": [p_region[pids[i]] for i in h_players]}
    return {"tables": t, "meta": meta, "scale": sc}


def validate(gen: dict, cfg: dict) -> dict:
    """Checks that become the 'validation' section of the generation report."""
    t, meta = gen["tables"], gen["meta"]
    res = {"counts": {name: len(rows) for name, (_, rows) in t.items()}, "checks": {}}
    pids = {r[0] for r in t["players"][1]}
    cat_ids = {r[0] for r in t["item_catalog"][1]}
    chk = res["checks"]

    # Referential integrity and constraints
    chk["inventory_fk_players"] = all(r[0] in pids for r in t["inventory"][1])
    chk["inventory_fk_catalog"] = all(r[1] in cat_ids for r in t["inventory"][1])
    chk["items_fk_players"] = all(r[2] in pids for r in t["items"][1])
    chk["plots_fk_players"] = all(r[1] in pids for r in t["farm_plots"][1])
    chk["no_negative_quantities"] = all(r[3] >= 0 for r in t["inventory"][1])
    chk["no_negative_coins"] = all(r[2] >= 0 for r in t["wallets"][1])
    chk["unique_player_ids"] = len(pids) == len(t["players"][1])
    chk["unique_inventory_keys"] = len({(r[0], r[1]) for r in t["inventory"][1]}) == len(t["inventory"][1])

    # Region split vs target (chi-square goodness of fit)
    obs = Counter(r[2] for r in t["players"][1])
    names = list(cfg["regions"].keys())
    o = np.array([obs[r] for r in names])
    e = np.array(list(cfg["regions"].values())) * o.sum()
    chi = stats.chisquare(o, e)
    res["region_split"] = {r: round(obs[r] / o.sum(), 4) for r in names}
    chk["region_split_matches_target"] = bool(chi.pvalue > 0.01)
    res["region_split_chi2_p"] = float(chi.pvalue)

    # Item popularity: fitted Zipf exponent on the rank-frequency curve
    freq = Counter(r[1] for r in t["items"][1])
    f = np.array(sorted(freq.values(), reverse=True), dtype=float)
    top = min(len(f), 100)
    slope, _ = np.polyfit(np.log(np.arange(1, top + 1)), np.log(f[:top]), 1)
    res["item_zipf_exponent_fitted"] = round(-slope, 3)
    res["item_zipf_exponent_target"] = cfg["items"]["zipf_exponent"]
    res["top10_item_types_share_of_items"] = round(f[:10].sum() / f.sum(), 4)
    chk["zipf_exponent_within_0.15"] = bool(abs(-slope - cfg["items"]["zipf_exponent"]) < 0.15)

    # Levels: geometric; KS distance to the target distribution
    lv = meta["levels"]
    p = cfg["players"]["level_geometric_p"]
    res["level_mean"] = round(float(lv.mean()), 3)
    res["level_mean_target"] = round(1 / p, 3)
    # KS distance for a discrete variable: max gap between CDFs at each level
    k = np.arange(1, cfg["players"]["max_level"] + 1)
    emp = np.searchsorted(np.sort(lv), k, side="right") / len(lv)
    tgt = stats.geom.cdf(k, p)
    tgt[-1] = 1.0                      # levels above max are clipped to max
    res["level_ks_distance"] = round(float(np.abs(emp - tgt).max()), 4)
    chk["level_ks_below_0.02"] = res["level_ks_distance"] < 0.02

    # Coins: lognormal by construction; report fitted log-sigma
    lc = np.log(np.maximum(meta["coins"], 1))
    res["coins_median"] = int(np.median(meta["coins"]))
    res["coins_p99"] = int(np.percentile(meta["coins"], 99))
    res["coins_top1pct_share"] = round(float(np.sort(meta["coins"])[-len(lc) // 100:].sum() / meta["coins"].sum()), 4)

    # Daily activity: peak local hour per region
    hours = np.array(meta["harvest_local_hours"])
    regs = np.array(meta["harvest_regions"])
    res["harvest_peak_local_hour"] = {r: int(np.bincount(hours[regs == r], minlength=24).argmax()) for r in names}

    # Golden Seed Drop fields
    fields = t["shared_fields"][1]
    res["golden_seeds_total"] = int(sum(r[3] for r in fields))
    pop = np.array([r[5] for r in fields])
    res["top10_fields_share_of_traffic"] = round(float(np.sort(pop)[-10:].sum()), 4)
    res["all_checks_passed"] = all(chk.values())
    return res


def write_csvs(gen: dict, out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    for name, (cols, rows) in gen["tables"].items():
        with gzip.open(out / f"{name}.csv.gz", "wt", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(cols)
            for r in rows:
                w.writerow(["" if v is None else v for v in r])


def profile_figure(gen: dict, cfg: dict, path: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    t, meta = gen["tables"], gen["meta"]
    names = list(cfg["regions"].keys())
    colors = {"us-west": "#2a6fdb", "us-east": "#e07b39", "eu-west": "#3a9e6e"}
    fig, ax = plt.subplots(2, 3, figsize=(13, 7.2))

    obs = Counter(r[2] for r in t["players"][1])
    tot = sum(obs.values())
    x = np.arange(len(names))
    ax[0, 0].bar(x - 0.18, [obs[r] / tot for r in names], 0.36, label="generated", color="#2a6fdb")
    ax[0, 0].bar(x + 0.18, [cfg["regions"][r] for r in names], 0.36, label="target", color="#b8c4d6")
    ax[0, 0].set_xticks(x, names)
    ax[0, 0].set_title("(a) Players by home region")
    ax[0, 0].set_ylabel("share of players")
    ax[0, 0].legend(frameon=False)

    lv = meta["levels"]
    ax[0, 1].hist(lv, bins=np.arange(1, 52) - 0.5, color="#2a6fdb")
    ax[0, 1].set_title("(b) Player level (geometric)")
    ax[0, 1].set_xlabel("level")
    ax[0, 1].set_ylabel("players")

    freq = Counter(r[1] for r in t["items"][1])
    f = np.array(sorted(freq.values(), reverse=True), dtype=float)
    ranks = np.arange(1, len(f) + 1)
    ax[0, 2].loglog(ranks, f, ".", ms=3, color="#2a6fdb", label="generated")
    s = cfg["items"]["zipf_exponent"]
    ax[0, 2].loglog(ranks, f[0] * ranks ** -s, "-", color="#999", label=f"Zipf s={s}")
    ax[0, 2].set_title("(c) Item-type popularity (rank vs count)")
    ax[0, 2].set_xlabel("popularity rank")
    ax[0, 2].set_ylabel("items owned")
    ax[0, 2].legend(frameon=False)

    ax[1, 0].hist(np.log10(np.maximum(meta["coins"], 1)), bins=50, color="#2a6fdb")
    ax[1, 0].set_title("(d) Wallet coins (lognormal)")
    ax[1, 0].set_xlabel("log10(coins)")
    ax[1, 0].set_ylabel("players")

    hours = np.array(meta["harvest_local_hours"])
    regs = np.array(meta["harvest_regions"])
    for r in names:
        c = np.bincount(hours[regs == r], minlength=24)
        ax[1, 1].plot(range(24), c / c.sum(), label=r, color=colors.get(r))
    ax[1, 1].set_title("(e) Harvest activity by local hour")
    ax[1, 1].set_xlabel("local hour of day")
    ax[1, 1].set_ylabel("share of harvests")
    ax[1, 1].legend(frameon=False)

    pop = np.array([r[5] for r in t["shared_fields"][1]])
    ax[1, 2].plot(np.arange(1, len(pop) + 1), np.cumsum(np.sort(pop)[::-1]), color="#2a6fdb")
    ax[1, 2].set_xscale("log")
    ax[1, 2].set_title("(f) Golden Seed field traffic (cumulative)")
    ax[1, 2].set_xlabel("fields, most popular first")
    ax[1, 2].set_ylabel("share of field requests")

    for a in ax.flat:
        a.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(path, dpi=200)
    plt.close(fig)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config")
    ap.add_argument("--out", default=str(ROOT / "data"))
    ap.add_argument("--scale", type=float, default=1.0)
    ap.add_argument("--anchor", help="ISO timestamp all generated times are relative to (default: now)")
    a = ap.parse_args()
    cfg = load_config(a.config)
    anchor = datetime.fromisoformat(a.anchor) if a.anchor else datetime.now(timezone.utc).replace(microsecond=0)
    gen = generate(cfg, a.scale, anchor)
    report = validate(gen, cfg)
    report.update({"seed": cfg["seed"], "scale_factor": a.scale, "anchor": anchor.isoformat()})
    out = Path(a.out)
    write_csvs(gen, out)
    (ROOT / "results").mkdir(exist_ok=True)
    with open(ROOT / "results" / "generation_report.json", "w") as fh:
        json.dump(report, fh, indent=2, default=str)
    profile_figure(gen, cfg, ROOT / "results" / "fig_data_profile.png")
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
