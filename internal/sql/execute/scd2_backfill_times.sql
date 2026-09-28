-- scd2_backfill_times.sql - Date the versions of an SCD2 target that was built
-- before valid_from_at / valid_to_at existed, right after they are added.
-- Args: %[1]s = target table, %[2]s = target name, escaped for a string literal
--
-- valid_from_snapshot is curr_snapshot of the writing run: a snapshot no older
-- than the start of that ondatrasql process, and older than the write. A
-- process commits each model at most once, so the version was written by the
-- first commit of this model after it. valid_to_snapshot is one less than
-- curr_snapshot of the closing run.
--
-- A version is dated only when every snapshot from its starting one up to that
-- commit still exists. Snapshot ids are consecutive, so a gap means
-- ducklake_expire_snapshots removed one, possibly the writing commit, and the
-- first remaining commit of the model could be a later one. Such versions stay
-- NULL. Only NULL values are filled.

CREATE OR REPLACE TEMP TABLE scd2_version_times AS
WITH commits AS (
    SELECT snapshot_id, snapshot_time FROM snapshots()
    WHERE lower(commit_extra_info->>'model') = lower('%[2]s')
),
starts AS (
    SELECT valid_from_snapshot AS c FROM %[1]s WHERE valid_from_snapshot IS NOT NULL
    UNION
    SELECT valid_to_snapshot + 1 FROM %[1]s WHERE valid_to_snapshot IS NOT NULL
),
writes AS (
    SELECT s.c, arg_min(m.snapshot_id, m.snapshot_id) AS w, arg_min(m.snapshot_time, m.snapshot_id) AS t
    FROM starts s JOIN commits m ON m.snapshot_id > s.c
    GROUP BY s.c
)
SELECT w.c, w.t
FROM writes w
WHERE (SELECT count(*) FROM snapshots() WHERE snapshot_id BETWEEN w.c AND w.w) = w.w - w.c + 1;

UPDATE %[1]s AS tgt SET
    valid_from_at = coalesce(tgt.valid_from_at, (SELECT t FROM scd2_version_times WHERE c = tgt.valid_from_snapshot)),
    valid_to_at = coalesce(tgt.valid_to_at, (SELECT t FROM scd2_version_times WHERE c = tgt.valid_to_snapshot + 1));

DROP TABLE scd2_version_times
