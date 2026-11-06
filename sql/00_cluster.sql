-- Cluster-wide settings (run once, as root).
-- READ COMMITTED is needed for the optimistic-concurrency variant of E1.
SET CLUSTER SETTING sql.txn.read_committed_isolation.enabled = true;
