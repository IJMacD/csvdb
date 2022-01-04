SELECT
    rowid,
    ROW_NUMBER(),
    *
FROM test
WHERE score = 5
ORDER BY name DESC
