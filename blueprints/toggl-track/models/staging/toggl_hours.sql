-- @kind: table
-- @constraint: not_null(date)
-- Hours per day, client and project. Entries without a project or client
-- are grouped under NULL.
SELECT
    date,
    client_name,
    project_name,
    ROUND(SUM(seconds) / 3600.0, 2)::DECIMAL(10, 2)                                   AS hours,
    ROUND(SUM(CASE WHEN billable THEN seconds ELSE 0 END) / 3600.0, 2)::DECIMAL(10, 2) AS billable_hours,
    COUNT(*)                                                                           AS entries
FROM raw.toggl_time_entries
GROUP BY date, client_name, project_name
