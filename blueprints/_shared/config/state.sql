-- state.sql - OndatraSQL operational state backend.
--
-- Holds the push queue, fetch staging buffers, and OAuth refresh
-- tokens. The file is encrypted at rest using DuckDB's native
-- AES-GCM-256 file encryption — the key lives in .env as
-- ONDATRA_STATE_KEY.
--
-- Single-process by default. To share state across multiple
-- ondatrasql processes (e.g. ephemeral pods), point this at a Quack
-- server instead — same SQL, the Go code is backend-agnostic.

ATTACH 'state.duckdb' AS state (ENCRYPTION_KEY '${ONDATRA_STATE_KEY}');

-- Multi-pod via Quack (one server per project):
-- LOAD quack;
-- ATTACH 'quack:state.example.com:9494' AS state (TYPE quack, TOKEN '${ONDATRA_QUACK_TOKEN}');
