SELECT name, score FROM test FETCH FIRST ROW ONLY
SELECT name, score FROM test FETCH FIRST 5 ROWS ONLY
SELECT name, score FROM test OFFSET 2 FETCH FIRST 5 ROWS ONLY
CREATE INDEX ON test (name)
CREATE INDEX ON test (birth_date)
EXPLAIN SELECT name FROM test ORDER BY name FETCH FIRST 5 ROWS ONLY
SELECT name FROM test ORDER BY name FETCH FIRST 5 ROWS ONLY
SELECT name FROM test ORDER BY name DESC FETCH FIRST 5 ROWS ONLY
SELECT name, score FROM test WHERE score = 42 FETCH FIRST 5 ROWS ONLY
EXPLAIN SELECT name, birth_date FROM test WHERE name = 'Walter KELLY'
SELECT name, birth_date FROM test WHERE name = 'Walter KELLY'
SELECT name, birth_date FROM test WHERE name > 'Walter KELLY' FETCH FIRST 5 ROWS ONLY
SELECT name, birth_date FROM test WHERE name LIKE 'Walter M%' FETCH FIRST 5 ROWS ONLY
EXPLAIN SELECT name, birth_date FROM test WHERE birth_date = '2021-01-18'
SELECT name, birth_date FROM test WHERE birth_date = '2021-01-18'
EXPLAIN SELECT name, birth_date FROM test WHERE birth_date > '2021-01-18' FETCH FIRST 5 ROWS ONLY
SELECT name, birth_date FROM test WHERE birth_date > '2021-01-18' FETCH FIRST 5 ROWS ONLY
EXPLAIN SELECT name, birth_date FROM test WHERE birth_date > '2021-01-18' ORDER BY name FETCH FIRST 5 ROWS ONLY
SELECT name, birth_date FROM test WHERE birth_date > '2021-01-18' ORDER BY name FETCH FIRST 5 ROWS ONLY
SELECT name, birth_date FROM test WHERE PK(id) = 769
SELECT EXTRACT(YEAR FROM birth_date), EXTRACT(MONTH FROM birth_date), EXTRACT(DAY FROM birth_date), EXTRACT(YEARDAY FROM birth_date) FROM test FETCH FIRST ROW ONLY
SELECT COUNT(*) FROM test
SELECT COUNT(*) FROM test WHERE name = 'Walter KELLY'
SELECT COUNT(*) FROM test WHERE birth_date = '2021-01-18'
SELECT COUNT(*) FROM test WHERE score = 42
FROM test WHERE name < 'Bob' AND score > 50 FETCH FIRST 5 ROWS ONLY
EXPLAIN FROM test, CALENDAR ON date = birth_date WHERE name LIKE 'Walter M%' SELECT name, yearday
FROM test, CALENDAR ON date = birth_date WHERE name LIKE 'Walter M%' SELECT name, yearday
FROM suits AS s1, suits AS s2 ON s1.name < s2.name ORDER BY name