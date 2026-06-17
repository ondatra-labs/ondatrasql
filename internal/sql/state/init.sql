-- OndatraSQL operational state schema.
-- Runs against the catalog ATTACHed as `state` by config/state.sql.
-- USE state has already been executed by state.Open, so unqualified
-- table names resolve into the state catalog's default schema.

CREATE TABLE IF NOT EXISTS sync_evt (
    target VARCHAR NOT NULL,
    seq BIGINT NOT NULL,
    payload BLOB NOT NULL,
    created_at TIMESTAMP DEFAULT now(),
    PRIMARY KEY (target, seq)
);

CREATE TABLE IF NOT EXISTS sync_inflight (
    claim_id VARCHAR NOT NULL,
    target VARCHAR NOT NULL,
    seq BIGINT NOT NULL,
    payload BLOB NOT NULL,
    PRIMARY KEY (claim_id, target, seq)
);

CREATE TABLE IF NOT EXISTS sync_claim (
    claim_id VARCHAR PRIMARY KEY,
    target VARCHAR NOT NULL,
    claimed_at TIMESTAMP DEFAULT now(),
    heartbeat TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sync_jobref (
    target VARCHAR PRIMARY KEY,
    job_ref BLOB,
    row_hash VARCHAR,
    updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sync_apply_log (
    claim_id VARCHAR NOT NULL,
    target VARCHAR NOT NULL,
    ord BIGINT NOT NULL,
    payload BLOB NOT NULL,
    status VARCHAR NOT NULL,
    delete_jobref BOOLEAN DEFAULT false,
    recorded_at TIMESTAMP DEFAULT now(),
    PRIMARY KEY (claim_id, ord)
);

CREATE TABLE IF NOT EXISTS tokens (
    provider VARCHAR PRIMARY KEY,
    refresh_token VARCHAR NOT NULL,
    local BOOLEAN DEFAULT false,
    token_url VARCHAR,
    updated_at TIMESTAMP DEFAULT now()
);
