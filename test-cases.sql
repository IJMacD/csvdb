-- Basic tests
SELECT name, score FROM test FETCH FIRST ROW ONLY
SELECT name, score FROM test FETCH FIRST 5 ROWS ONLY
SELECT name, score FROM test OFFSET 2 ROWS FETCH NEXT 5 ROWS ONLY
-- Create indexes
CREATE INDEX ON test (name)
CREATE UNIQUE INDEX ON test (birth_date)
-- Test indexes
EXPLAIN SELECT name FROM test ORDER BY name FETCH FIRST 5 ROWS ONLY
SELECT name FROM test ORDER BY name FETCH FIRST 5 ROWS ONLY
SELECT name FROM test ORDER BY name DESC FETCH FIRST 5 ROWS ONLY
SELECT name, score FROM test WHERE score = 42 FETCH FIRST 5 ROWS ONLY
EXPLAIN SELECT name, birth_date FROM test WHERE name = 'Walter KELLY'
SELECT name, birth_date FROM test WHERE name = 'Walter KELLY'
SELECT name, birth_date FROM test WHERE name > 'Walter KELLY' FETCH FIRST 5 ROWS ONLY
EXPLAIN SELECT name, birth_date FROM test WHERE name LIKE 'Walter M%' FETCH FIRST 5 ROWS ONLY
SELECT name, birth_date FROM test WHERE name LIKE 'Walter M%' FETCH FIRST 5 ROWS ONLY
EXPLAIN SELECT name, SUM(score) FROM test WHERE name = 'Walter KELLY'
SELECT name, SUM(score) FROM test WHERE name = 'Walter KELLY'
EXPLAIN SELECT name, birth_date FROM test WHERE birth_date = '2050-01-01'
SELECT name, birth_date FROM test WHERE birth_date = '2050-01-01'
SELECT name, birth_date FROM test WHERE birth_date = '2050-01-02'
EXPLAIN SELECT name, birth_date FROM test WHERE birth_date > '2050-01-01' FETCH FIRST 5 ROWS ONLY
SELECT name, birth_date FROM test WHERE birth_date > '2050-01-01' FETCH FIRST 5 ROWS ONLY
EXPLAIN SELECT name, birth_date FROM test WHERE '2050-01-01' < birth_date  FETCH FIRST 5 ROWS ONLY
SELECT name, birth_date FROM test WHERE '2050-01-01' < birth_date  FETCH FIRST 5 ROWS ONLY
EXPLAIN SELECT name, birth_date FROM test WHERE birth_date > '2050-01-01' AND score > 95 ORDER BY name FETCH FIRST 5 ROWS ONLY
SELECT name, birth_date, score FROM test WHERE birth_date > '2050-01-01' AND score > 95 ORDER BY name FETCH FIRST 5 ROWS ONLY
EXPLAIN SELECT name, birth_date FROM test WHERE PK(id) = 769
SELECT name, birth_date FROM test WHERE PK(id) = 769
EXPLAIN SELECT id, name, birth_date FROM test WHERE PK(id) < 51
SELECT id, name, birth_date FROM test WHERE PK(id) < 51
SELECT id, name, birth_date FROM test WHERE 51 >= PK(id)
-- Test EXTRACT
SELECT birth_date, EXTRACT(YEAR FROM birth_date), EXTRACT(MONTH FROM birth_date), EXTRACT(DAY FROM birth_date), EXTRACT(YEARDAY FROM birth_date) FROM test FETCH FIRST ROW ONLY
-- Test COUNT(*)
SELECT COUNT(*) FROM test
SELECT COUNT(*) FROM test WHERE name = 'Walter KELLY'
SELECT COUNT(*) FROM test WHERE birth_date = '2050-01-01'
SELECT COUNT(*) FROM test WHERE score = 42
FROM test WHERE name < 'Bob' AND score > 50 FETCH FIRST 5 ROWS ONLY
-- Test Join to CALENDAR
EXPLAIN FROM test, CALENDAR ON date = birth_date WHERE name LIKE 'Walter M%' SELECT name, yearday, birth_date, date FETCH FIRST 5 ROWS ONLY
FROM test, CALENDAR ON date = birth_date WHERE name LIKE 'Walter M%' SELECT name, birth_date, yearday FETCH FIRST 5 ROWS ONLY
FROM test, CALENDAR ON birth_date = date WHERE name < 'Aaron Z' SELECT name, date, yearday ORDER BY yearday FETCH FIRST 5 ROWS ONLY
EXPLAIN FROM test WHERE EXTRACT(WEEKDAY FROM birth_date) = 5 FETCH FIRST 5 ROWS ONLY
FROM test WHERE EXTRACT(WEEKDAY FROM birth_date) = 5 FETCH FIRST 5 ROWS ONLY
EXPLAIN FROM test WHERE EXTRACT(WEEKDAY FROM birth_date) = 5 ORDER BY birth_date FETCH FIRST 5 ROWS ONLY
FROM test WHERE EXTRACT(WEEKDAY FROM birth_date) = 5 ORDER BY birth_date FETCH FIRST 5 ROWS ONLY
EXPLAIN FROM test WHERE birth_date > '1901-01-01' AND EXTRACT(WEEKDAY FROM birth_date) = 5 FETCH FIRST 5 ROWS ONLY
FROM test WHERE birth_date > '1901-01-01' AND EXTRACT(WEEKDAY FROM birth_date) = 5 FETCH FIRST 5 ROWS ONLY
-- Test View
FROM view FETCH FIRST 5 ROWS ONLY
-- Test Join on inequality
FROM suits AS s1, suits AS s2 ON s1.name < s2.name ORDER BY name
-- Test concat operator
FROM suits, ranks WHERE value > 10 ORDER BY name SELECT ranks.name || ' of ' || suits.name AS cards
-- Test query with no Table
EXPLAIN SELECT CURRENT_DATE, EXTRACT(YEARDAY FROM CURRENT_DATE), EXTRACT(JULIAN FROM '1995-10-10')
SELECT CURRENT_DATE, EXTRACT(YEARDAY FROM CURRENT_DATE), EXTRACT(JULIAN FROM '1995-10-10')
FROM CALENDAR WHERE date = CURRENT_DATE SELECT julian, date, yeardayString, weekdayString
-- Test TABLE clause
TABLE suits
-- Test LISTAGG
FROM suits SELECT LISTAGG(name)
-- Test different joins
FROM suits INNER JOIN ranks ON LENGTH(ranks.name) = LENGTH(suits.name)
FROM suits LEFT JOIN ranks ON LENGTH(ranks.name) = LENGTH(suits.name)
FROM suits JOIN ranks USING LENGTH(name)
-- Test FROM column aliasing
FROM suits AS s (n, s)
-- Test multi-column ordering
FROM suits, ranks ORDER BY ranks.name ASC, suits.name ASC FETCH FIRST 2 ROWS ONLY
FROM suits, ranks ORDER BY ranks.name ASC, suits.name DESC FETCH FIRST 2 ROWS ONLY
FROM suits, ranks ORDER BY ranks.name DESC, suits.name ASC FETCH FIRST 2 ROWS ONLY
FROM suits, ranks ORDER BY ranks.name DESC, suits.name DESC FETCH FIRST 2 ROWS ONLY
-- Test Subqueries
FROM (FROM SEQUENCE(10) OFFSET 5 ROWS) AS a, (FROM SEQUENCE(2)) AS b SELECT b.value, a.value
-- Test VALUES clause
VALUES ('a',1),('b',2),('c',3)
FROM (VALUES ('a',1),('b',2),('c',3)) AS a WHERE a.col2 < 3 SELECT a.col2, a.col1
FROM (VALUES ('a',1),('b',2),('c',3)) AS a (first, second)
-- Test CTEs
WITH r1 AS (FROM ranks WHERE value < 8 SELECT name, symbol) FROM r1 ORDER BY name
WITH r1 AS (FROM ranks WHERE value < 8 SELECT name, symbol), s1 AS (FROM suits SELECT name), v1 AS (VALUES ('b')) FROM r1,r1 AS r2,v1 ORDER BY r2.name, r1.name DESC FETCH FIRST 5 ROWS ONLY
-- Test Date functions
SELECT DATE_DIFF('2000-01-01', '1990-01-01')
SELECT DATE_DIFF(CURRENT_DATE, '2000-01-01')
FROM test SELECT birth_date, score, DATE_ADD(birth_date, score) FETCH FIRST 5 ROWS ONLY
-- Test arithmetic functions
SELECT ADD(5, 9)
FROM ranks SELECT SUB(value, 3) FETCH FIRST 5 ROWS ONLY
FROM ranks SELECT MUL(value, 4) FETCH FIRST 5 ROWS ONLY
FROM ranks SELECT DIV(36, value) FETCH FIRST 5 ROWS ONLY
FROM ranks SELECT MOD(19, value) FETCH FIRST 5 ROWS ONLY
FROM ranks SELECT POW(value, 2) FETCH FIRST 5 ROWS ONLY
FROM test WHERE name LIKE 'Adam WE%' GROUP BY name SELECT name, COUNT(*)