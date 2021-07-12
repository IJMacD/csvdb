SELECT name, score FROM test3 FETCH FIRST ROW ONLY
SELECT name, score FROM test3 FETCH FIRST 5 ROWS ONLY
SELECT name, score FROM test3 OFFSET 2 FETCH FIRST 5 ROWS ONLY
SELECT name FROM test3 ORDER BY name FETCH FIRST 5 ROWS ONLY
SELECT name FROM test3 ORDER BY name DESC FETCH FIRST 5 ROWS ONLY
SELECT name, score FROM test3 WHERE score = 42 FETCH FIRST 5 ROWS ONLY
SELECT name, birth_date FROM test3 WHERE name = 'Walter KELLY'
SELECT name, birth_date FROM test3 WHERE name > 'Walter KELLY' FETCH FIRST 5 ROWS ONLY
SELECT name, birth_date FROM test3 WHERE name LIKE 'Walter K%'
SELECT name, birth_date FROM test3 WHERE birth_date = '2021-01-18'
SELECT name, birth_date FROM test3 WHERE birth_date > '2021-01-18' FETCH FIRST 5 ROWS ONLY
SELECT name, birth_date FROM test3 WHERE birth_date > '2021-01-18' ORDER BY name FETCH FIRST 5 ROWS ONLY
SELECT name, birth_date FROM test3 WHERE PK(id) = 769
SELECT name, birth_date FROM test3 WHERE UNIQUE(bd) = '2021-01-18'
SELECT name, birth_date FROM test3 WHERE INDEX(bbb) = 42 FETCH FIRST 5 ROWS ONLY
SELECT COUNT(*) FROM test3
SELECT COUNT(*) FROM test3 WHERE name = 'Walter KELLY'
SELECT COUNT(*) FROM test3 WHERE birth_date = '2021-01-18'
SELECT COUNT(*) FROM test3 WHERE score = 42