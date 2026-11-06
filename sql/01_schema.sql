-- Sunfield farm-game schema for CockroachDB (multi-region).
-- Regions come from each node's --locality flag; the database is homed in
-- us-west and also placed in us-east and eu-west.
--
-- Table localities (fragmentation + replication policy):
--   REGIONAL BY ROW  : each row lives in the region named by its region column
--                      (primary horizontal fragmentation on region; child tables
--                      follow their owner's region = derived fragmentation)
--   GLOBAL           : small, read-mostly reference data replicated everywhere
--   REGIONAL BY TABLE: whole table homed in the primary region

CREATE DATABASE IF NOT EXISTS farm PRIMARY REGION "us-west" REGIONS "us-east", "eu-west" SURVIVE ZONE FAILURE;
USE farm;

-- Reference data -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS item_catalog (
    item_type_id   INT PRIMARY KEY,
    name           STRING NOT NULL,
    kind           STRING NOT NULL CHECK (kind IN ('crop', 'seed', 'tool', 'decor')),
    rarity         STRING NOT NULL CHECK (rarity IN ('common', 'uncommon', 'rare', 'legendary')),
    base_value     INT NOT NULL CHECK (base_value > 0),
    growth_seconds INT                      -- crops only
) LOCALITY GLOBAL;

-- General tier: player-owned state, strong reads ------------------------------
CREATE TABLE IF NOT EXISTS players (
    player_id   UUID PRIMARY KEY,
    username    STRING NOT NULL,
    home_region crdb_internal_region NOT NULL,
    level       INT NOT NULL CHECK (level BETWEEN 1 AND 50),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
) LOCALITY REGIONAL BY ROW AS home_region;

CREATE TABLE IF NOT EXISTS farm_plots (
    plot_id      UUID PRIMARY KEY,
    owner_id     UUID NOT NULL REFERENCES players (player_id),
    region       crdb_internal_region NOT NULL,
    crop_type_id INT REFERENCES item_catalog (item_type_id),
    planted_at   TIMESTAMPTZ,
    ready_at     TIMESTAMPTZ,
    version      INT NOT NULL DEFAULT 0,
    INDEX plots_by_owner (owner_id)
) LOCALITY REGIONAL BY ROW AS region;

-- Critical tier: money and ownership, serializable transactions ----------------
CREATE TABLE IF NOT EXISTS wallets (
    player_id  UUID PRIMARY KEY REFERENCES players (player_id),
    region     crdb_internal_region NOT NULL,
    coins      INT8 NOT NULL CHECK (coins >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
) LOCALITY REGIONAL BY ROW AS region;

CREATE TABLE IF NOT EXISTS inventory (
    player_id    UUID NOT NULL REFERENCES players (player_id),
    item_type_id INT NOT NULL REFERENCES item_catalog (item_type_id),
    region       crdb_internal_region NOT NULL,
    quantity     INT8 NOT NULL CHECK (quantity >= 0),
    PRIMARY KEY (player_id, item_type_id)
) LOCALITY REGIONAL BY ROW AS region;

CREATE TABLE IF NOT EXISTS items (            -- individually owned rare items
    item_id      UUID PRIMARY KEY,
    item_type_id INT NOT NULL REFERENCES item_catalog (item_type_id),
    owner_id     UUID NOT NULL REFERENCES players (player_id),
    region       crdb_internal_region NOT NULL,
    acquired_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    INDEX items_by_owner (owner_id)
) LOCALITY REGIONAL BY ROW AS region;

-- Contended shared resources: the Golden Seed Drop fields.
CREATE TABLE IF NOT EXISTS shared_fields (
    field_id           INT PRIMARY KEY,
    region             crdb_internal_region NOT NULL,
    item_type_id       INT NOT NULL REFERENCES item_catalog (item_type_id),
    quantity_remaining INT NOT NULL CHECK (quantity_remaining >= 0),
    initial_quantity   INT NOT NULL,
    popularity         FLOAT8 NOT NULL,     -- Zipf weight used by the workload
    epoch              INT NOT NULL DEFAULT 0,   -- bumped when an experiment restocks
    version            INT NOT NULL DEFAULT 0    -- used by optimistic concurrency control
) LOCALITY REGIONAL BY ROW AS region;

CREATE TABLE IF NOT EXISTS field_purchases (
    purchase_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    field_id    INT NOT NULL,
    epoch       INT NOT NULL,
    player_id   UUID NOT NULL,
    region      crdb_internal_region NOT NULL,   -- buyer's region
    price       INT8 NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    INDEX purchases_by_field (field_id, epoch)
) LOCALITY REGIONAL BY ROW AS region;

CREATE TABLE IF NOT EXISTS trades (
    trade_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    request_id   UUID NOT NULL,
    seller_id    UUID NOT NULL,
    buyer_id     UUID NOT NULL,
    item_type_id INT NOT NULL,
    quantity     INT NOT NULL CHECK (quantity > 0),
    price        INT8 NOT NULL CHECK (price >= 0),
    region       crdb_internal_region NOT NULL,  -- seller's region
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    INDEX trades_by_request (request_id)
) LOCALITY REGIONAL BY ROW AS region;

-- Idempotency keys: a retried request returns the stored result instead of
-- running twice. Written in the same transaction as the action itself.
CREATE TABLE IF NOT EXISTS requests (
    request_id UUID PRIMARY KEY,
    region     crdb_internal_region NOT NULL,
    endpoint   STRING NOT NULL,
    response   JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
) LOCALITY REGIONAL BY ROW AS region;

-- Durable record of confirmed harvests (history + live).
CREATE TABLE IF NOT EXISTS harvest_log (
    harvest_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    player_id    UUID NOT NULL,
    region       crdb_internal_region NOT NULL,
    source       STRING NOT NULL CHECK (source IN ('plot', 'field')),
    source_id    STRING NOT NULL,
    item_type_id INT NOT NULL,
    quantity     INT NOT NULL CHECK (quantity > 0),
    kind         STRING NOT NULL DEFAULT 'live' CHECK (kind IN ('history', 'live')),
    harvested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    INDEX harvest_by_player (player_id)
) LOCALITY REGIONAL BY ROW AS region;

-- Delay-tolerant tier ---------------------------------------------------------
-- Leaderboard: derived from harvest_log by a periodic refresh, read with
-- follower reads (AS OF SYSTEM TIME follower_read_timestamp()).
CREATE TABLE IF NOT EXISTS leaderboard (
    player_id  UUID PRIMARY KEY,
    region     STRING NOT NULL,
    score      INT8 NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    INDEX leaderboard_by_score (score DESC)
) LOCALITY REGIONAL BY TABLE IN PRIMARY REGION;

-- Presence: who is online. Rows expire automatically via row-level TTL.
CREATE TABLE IF NOT EXISTS presence (
    player_id  UUID PRIMARY KEY,
    region     crdb_internal_region NOT NULL,
    status     STRING NOT NULL DEFAULT 'online',
    last_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL
        DEFAULT now() + INTERVAL '5 minutes'
) LOCALITY REGIONAL BY ROW AS region;

ALTER TABLE presence SET (
    ttl_expiration_expression = 'expires_at',
    ttl_job_cron = '*/5 * * * *'
);

-- Experiment support ------------------------------------------------------------
-- Heartbeat row written every ~50 ms; follower reads of it measure staleness.
CREATE TABLE IF NOT EXISTS heartbeat (
    id        INT PRIMARY KEY,
    client_ts FLOAT8 NOT NULL
) LOCALITY REGIONAL BY TABLE IN PRIMARY REGION;

-- Every write the client saw acknowledged during the failover test.
CREATE TABLE IF NOT EXISTS durability_probe (
    probe_id  UUID PRIMARY KEY,
    region    crdb_internal_region NOT NULL,
    worker    INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
) LOCALITY REGIONAL BY ROW AS region;

-- Invariant-audit baseline (totals that must be conserved).
CREATE TABLE IF NOT EXISTS audit_baseline (
    k STRING PRIMARY KEY,
    v INT8 NOT NULL
) LOCALITY REGIONAL BY TABLE IN PRIMARY REGION;
