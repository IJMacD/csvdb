SELECT name, score FROM test FETCH FIRST ROW ONLY
SELECT name, score FROM test FETCH FIRST 5 ROWS ONLY
SELECT name, score FROM test OFFSET 2 FETCH NEXT 5 ROWS ONLY
CREATE INDEX ON test (name)
CREATE UNIQUE INDEX ON test (birth_date)
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
SELECT EXTRACT(YEAR FROM birth_date), EXTRACT(MONTH FROM birth_date), EXTRACT(DAY FROM birth_date), EXTRACT(YEARDAY FROM birth_date) FROM test FETCH FIRST ROW ONLY
SELECT COUNT(*) FROM test
SELECT COUNT(*) FROM test WHERE name = 'Walter KELLY'
SELECT COUNT(*) FROM test WHERE birth_date = '2050-01-01'
SELECT COUNT(*) FROM test WHERE score = 42
FROM test WHERE name < 'Bob' AND score > 50 FETCH FIRST 5 ROWS ONLY
EXPLAIN FROM test, CALENDAR ON date = birth_date WHERE name LIKE 'Walter M%' SELECT name, yearday FETCH FIRST 5 ROWS ONLY
FROM test, CALENDAR ON date = birth_date WHERE name LIKE 'Walter M%' SELECT name, birth_date, yearday FETCH FIRST 5 ROWS ONLY
FROM test, CALENDAR ON birth_date = date WHERE name < 'Aaron Z' SELECT name, date, yearday ORDER BY yearday FETCH FIRST 5 ROWS ONLY
EXPLAIN FROM test WHERE EXTRACT(WEEKDAY FROM birth_date) = 5 FETCH FIRST 5 ROWS ONLY
FROM test WHERE EXTRACT(WEEKDAY FROM birth_date) = 5 FETCH FIRST 5 ROWS ONLY
EXPLAIN FROM test WHERE EXTRACT(WEEKDAY FROM birth_date) = 5 ORDER BY birth_date FETCH FIRST 5 ROWS ONLY
FROM test WHERE EXTRACT(WEEKDAY FROM birth_date) = 5 ORDER BY birth_date FETCH FIRST 5 ROWS ONLY
EXPLAIN FROM test WHERE birth_date > '1901-01-01' AND EXTRACT(WEEKDAY FROM birth_date) = 5 FETCH FIRST 5 ROWS ONLY
FROM test WHERE birth_date > '1901-01-01' AND EXTRACT(WEEKDAY FROM birth_date) = 5 FETCH FIRST 5 ROWS ONLY
FROM view FETCH FIRST 5 ROWS ONLY
FROM suits AS s1, suits AS s2 ON s1.name < s2.name ORDER BY name
FROM suits, ranks WHERE value > 10 ORDER BY name SELECT ranks.name || ' of ' || suits.name AS cards
EXPLAIN SELECT TODAY(), EXTRACT(YEARDAY FROM TODAY()), EXTRACT(JULIAN FROM '1995-10-10')
SELECT TODAY(), EXTRACT(YEARDAY FROM TODAY()), EXTRACT(JULIAN FROM '1995-10-10')
FROM CALENDAR WHERE date = CURRENT_DATE SELECT julian, date, yeardayString, weekdayString
FROM suits SELECT LISTAGG(name)
FROM suits INNER JOIN ranks ON LENGTH(ranks.name) = LENGTH(suits.name)
FROM suits LEFT JOIN ranks ON LENGTH(ranks.name) = LENGTH(suits.name)
FROM suits JOIN ranks USING LENGTH(name)
FROM suits AS s (n, s)
TABLE suits
FROM (FROM SEQUENCE(10) OFFSET 5) AS a, (FROM SEQUENCE(2)) AS b SELECT b.value, a.value
VALUES ('a',1),('b',2),('c',3)
FROM (VALUES ('a',1),('b',2),('c',3)) AS a WHERE a.col2 < 3 SELECT a.col2, a.col1
FROM (VALUES ('a',1),('b',2),('c',3)) AS a (first, second)