"""Dump the cluster's topology, partitioning and replica placement as evidence.

    python -m experiments.inspect_cluster

Writes results/cluster_inspect.txt (human readable) and
results/cluster_ranges.json (for the shard-placement figure).
"""
import json

from experiments.common import RESULTS, farm_admin

QUERIES = [
    ("Nodes and their localities",
     "SELECT node_id, address, locality, is_live FROM crdb_internal.gossip_nodes ORDER BY node_id"),
    ("Database regions", "SHOW REGIONS FROM DATABASE farm"),
    ("Survival goal", "SHOW SURVIVAL GOAL FROM DATABASE farm"),
    ("Table localities",
     "SELECT name AS table_name, locality FROM crdb_internal.tables WHERE database_name = 'farm' AND drop_time IS NULL ORDER BY 1"),
    ("Partitions of players (one per region)", "SHOW PARTITIONS FROM TABLE players"),
    ("Zone configuration of players", "SHOW ZONE CONFIGURATION FROM TABLE players"),
    ("Ranges of players with leaseholder and replicas",
     "SELECT range_id, start_key, end_key, lease_holder, lease_holder_locality, replicas, voting_replicas, "
     "non_voting_replicas FROM [SHOW RANGES FROM TABLE players WITH DETAILS] ORDER BY start_key"),
    ("Leaseholders per node, whole farm database",
     "SELECT lease_holder, count(*) AS ranges FROM [SHOW RANGES FROM DATABASE farm WITH DETAILS] GROUP BY 1 ORDER BY 1"),
    ("Rows per region (players)",
     "SELECT home_region, count(*) FROM players GROUP BY 1 ORDER BY 1"),
    ("Local query: one player's farm (EXPLAIN ANALYZE)",
     "EXPLAIN ANALYZE SELECT * FROM farm_plots WHERE owner_id = (SELECT player_id FROM players WHERE home_region = 'us-west' LIMIT 1)"),
    ("Cross-region query: richest players in every region (EXPLAIN ANALYZE)",
     "EXPLAIN ANALYZE SELECT p.home_region, max(w.coins) FROM players p JOIN wallets w ON w.player_id = p.player_id GROUP BY 1"),
    ("Follower read of the leaderboard (EXPLAIN ANALYZE)",
     "EXPLAIN ANALYZE SELECT player_id, score FROM leaderboard AS OF SYSTEM TIME follower_read_timestamp() ORDER BY score DESC LIMIT 10"),
]


def fmt(rows):
    if not rows:
        return "(no rows)"
    cols = list(rows[0].keys())
    if len(cols) == 1:
        return "\n".join(str(r[cols[0]]) for r in rows)
    w = {c: min(60, max(len(c), *(len(str(r[c])) for r in rows))) for c in cols}
    head = "  ".join(c.ljust(w[c]) for c in cols)
    lines = [head, "-" * len(head)]
    for r in rows:
        lines.append("  ".join(str(r[c])[:60].ljust(w[c]) for c in cols))
    return "\n".join(lines)


def main():
    conn = farm_admin()
    out = []
    for title, q in QUERIES:
        out.append(f"\n=== {title} ===\n> {q}\n")
        try:
            out.append(fmt(conn.execute(q).fetchall()))
        except Exception as e:
            out.append(f"ERROR: {str(e).splitlines()[0]}")
    text = "\n".join(out)
    (RESULTS / "cluster_inspect.txt").write_text(text)
    print(text)

    # Replica and leaseholder placement per node, per table (for the figure).
    place = []
    tables = [r["table_name"] for r in conn.execute(
        "SELECT name AS table_name FROM crdb_internal.tables WHERE database_name = 'farm' AND drop_time IS NULL").fetchall()]
    for t in tables:
        try:
            for r in conn.execute(f"SELECT range_id, lease_holder, replicas, voting_replicas FROM "
                                  f"[SHOW RANGES FROM TABLE {t} WITH DETAILS]").fetchall():
                place.append({"table": t, "range_id": r["range_id"], "lease_holder": r["lease_holder"],
                              "replicas": list(r["replicas"]), "voting": list(r["voting_replicas"] or [])})
        except Exception as e:
            print(f"ranges for {t} failed: {e}")
    nodes = [{"node_id": r["node_id"], "locality": r["locality"]} for r in conn.execute(
        "SELECT node_id, locality FROM crdb_internal.gossip_nodes ORDER BY node_id").fetchall()]
    with open(RESULTS / "cluster_ranges.json", "w") as fh:
        json.dump({"nodes": nodes, "ranges": place}, fh, indent=1)


if __name__ == "__main__":
    main()
