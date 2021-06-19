SELECT 
    score,
    birth_date,
    rowid,
    ROW_NUMBER(),
    * 
FROM test
WHERE score IS NOT NULL 
ORDER BY name DESC
