-- @kind: table
-- @constraint: not_null(date)
-- One row per banking day, one column per currency (SEK per unit).
SELECT
    date,
    MAX(CASE WHEN series = 'SEKEURPMI' THEN value END)::DECIMAL(10, 4) AS eur,
    MAX(CASE WHEN series = 'SEKUSDPMI' THEN value END)::DECIMAL(10, 4) AS usd
FROM raw.riksbank_observations
GROUP BY date
