-- Basic tests
SELECT name, score FROM test FETCH FIRST ROW ONLY;
SELECT name, score FROM test FETCH FIRST 5 ROWS ONLY;
SELECT name, score FROM test OFFSET 2 ROWS FETCH NEXT 5 ROWS ONLY;
-- Create indexes
CREATE INDEX ON test (name, birth_date);
CREATE UNIQUE INDEX ON test (birth_date);
-- Test indexes
SELECT name FROM test ORDER BY name FETCH FIRST 5 ROWS ONLY;
SELECT name FROM test ORDER BY name DESC FETCH FIRST 5 ROWS ONLY;
SELECT name, score FROM test WHERE score = 42 FETCH FIRST 5 ROWS ONLY;
SELECT name, birth_date FROM test WHERE name = 'Walter KELLY';
SELECT name, birth_date FROM test WHERE name > 'Walter KELLY' FETCH FIRST 5 ROWS ONLY;
SELECT name, birth_date FROM test WHERE name LIKE 'Walter M%' FETCH FIRST 5 ROWS ONLY;
SELECT name, SUM(score) FROM test WHERE name = 'Walter KELLY';
SELECT name, birth_date FROM test WHERE birth_date = '2050-01-01';
SELECT name, birth_date FROM test WHERE birth_date = '2050-01-02';
SELECT name, birth_date FROM test WHERE birth_date > '2050-01-01' FETCH FIRST 5 ROWS ONLY;
SELECT name, birth_date FROM test WHERE '2050-01-01' < birth_date  FETCH FIRST 5 ROWS ONLY;
SELECT name, birth_date, score FROM test WHERE birth_date > '2050-01-01' AND score > 95 ORDER BY name FETCH FIRST 5 ROWS ONLY;
-- Test explicit indexes
FROM test WHERE INDEX('test__name.index') = 'Claude MILLS';
FROM test WHERE UNIQUE('test__birth_date.unique') >= '0990-03-26' LIMIT 2;
SELECT name, birth_date FROM test WHERE PK(id) = 1000;
SELECT id, name, birth_date FROM test WHERE PK(id) < 51;
SELECT id, name, birth_date FROM test WHERE 51 >= PK(id);
-- Test EXTRACT
SELECT birth_date, EXTRACT(YEAR FROM birth_date), EXTRACT(MONTH FROM birth_date), EXTRACT(DAY FROM birth_date), EXTRACT(YEARDAY FROM birth_date) FROM test FETCH FIRST ROW ONLY;
-- Test Functions
SELECT RANDOM() AS "Your lucky number";
SELECT LEFT('Hello there', 4), RIGHT('shampoo', 3), TO_HEX(66), CHR(128169);
-- Test COUNT(*)
SELECT COUNT(*) FROM test;
SELECT COUNT(*) FROM test WHERE name = 'Walter KELLY';
SELECT COUNT(*) FROM test WHERE birth_date = '2050-01-01';
SELECT COUNT(*) FROM test WHERE score = 42;
FROM test WHERE name < 'Bob' AND score > 50 FETCH FIRST 5 ROWS ONLY;
-- Test * in function params
FROM SEQUENCE SELECT DATE_ADD('2023-10-31', *) LIMIT 5;
-- Test Join to CALENDAR
FROM test, CALENDAR ON date = birth_date WHERE name LIKE 'Walter M%' SELECT name, birth_date, yearday FETCH FIRST 5 ROWS ONLY;
FROM test, CALENDAR ON birth_date = date WHERE name < 'Aaron Z' SELECT name, date, yearday ORDER BY yearday FETCH FIRST 5 ROWS ONLY;
FROM test WHERE EXTRACT(WEEKDAY FROM birth_date) = 5 FETCH FIRST 5 ROWS ONLY;
FROM test WHERE EXTRACT(WEEKDAY FROM birth_date) = 5 ORDER BY birth_date FETCH FIRST 5 ROWS ONLY;
FROM test WHERE birth_date > '1901-01-01' AND EXTRACT(WEEKDAY FROM birth_date) = 5 FETCH FIRST 5 ROWS ONLY;
-- Test View
FROM view FETCH FIRST 5 ROWS ONLY;
-- Test Join on inequality
FROM suits AS s1, suits AS s2 ON s1.name < s2.name ORDER BY name;
-- Test concat operator
FROM suits, ranks WHERE value > 10 ORDER BY name SELECT ranks.name || ' of ' || suits.name AS cards;
-- Test query with no Table
SELECT CURRENT_DATE, EXTRACT(YEARDAY FROM CURRENT_DATE), EXTRACT(JULIAN FROM '1995-10-10');
FROM CALENDAR WHERE date = CURRENT_DATE SELECT julian, date, ordinalDate, weekDate;
-- Test TABLE clause
TABLE suits;
-- Test LISTAGG
FROM suits SELECT LISTAGG(name);
-- Test different joins
FROM suits INNER JOIN ranks ON LENGTH(ranks.name) = LENGTH(suits.name);
FROM suits LEFT JOIN ranks ON LENGTH(ranks.name) = LENGTH(suits.name);
FROM suits JOIN ranks USING LENGTH(name);
-- Test join predicates
FROM SEQUENCE AS s1 LEFT JOIN SEQUENCE AS s2 ON s1.value - 2 = s2.value AND s2.value < 2 LIMIT 5;
-- Test FROM column aliasing;
FROM suits AS s (n, s);
-- Test multi-column ordering
FROM suits, ranks ORDER BY ranks.name ASC, suits.name ASC FETCH FIRST 2 ROWS ONLY;
FROM suits, ranks ORDER BY ranks.name ASC, suits.name DESC FETCH FIRST 2 ROWS ONLY;
FROM suits, ranks ORDER BY ranks.name DESC, suits.name ASC FETCH FIRST 2 ROWS ONLY;
FROM suits, ranks ORDER BY ranks.name DESC, suits.name DESC FETCH FIRST 2 ROWS ONLY;
-- Test Subqueries
FROM (FROM SEQUENCE OFFSET 5 ROWS LIMIT 10) AS a, (FROM SEQUENCE LIMIT 2) AS b SELECT b.value, a.value;
-- Test VALUES clause
VALUES ('a',1),('b',2),('c',3);
FROM (VALUES ('a',1),('b',2),('c',3)) AS a WHERE a.col2 < 3 SELECT a.col2, a.col1;
FROM (VALUES ('a',1),('b',2),('c',3)) AS a (first, second);
-- Test CTEs
WITH r1 AS (FROM ranks WHERE value < 8 SELECT name, symbol) FROM r1 ORDER BY name;
WITH r1 AS (FROM ranks WHERE value < 8 SELECT name, symbol), s1 AS (FROM suits SELECT name), v1 AS (VALUES ('b')) FROM r1,r1 AS r2,v1 ORDER BY r2.name, r1.name DESC FETCH FIRST 5 ROWS ONLY;
-- Test Date functions
SELECT DATE_DIFF('2000-01-01', '1986-04-24');
SELECT DATE_DIFF(CURRENT_DATE, '2000-01-01');
FROM test SELECT birth_date, score, DATE_ADD(birth_date, score) FETCH FIRST 5 ROWS ONLY;
-- Test arithmetic functions
SELECT ADD(5, 9);
FROM ranks SELECT SUB(value, 3) FETCH FIRST 5 ROWS ONLY;
FROM ranks SELECT MUL(value, 4) FETCH FIRST 5 ROWS ONLY;
FROM ranks SELECT DIV(36, value) FETCH FIRST 5 ROWS ONLY;
FROM ranks SELECT MOD(19, value) FETCH FIRST 5 ROWS ONLY;
FROM ranks SELECT POW(value, 2) FETCH FIRST 5 ROWS ONLY;
-- Test Grouping
FROM test WHERE name LIKE 'Adam WE%' GROUP BY name SELECT name, COUNT(*);
FROM test WHERE name LIKE 'Adam K%' GROUP BY name ORDER BY score DESC SELECT name, COUNT(*), SUM(score) AS score;
FROM test GROUP BY day SELECT EXTRACT(WEEKDAY FROM birth_date) AS day, COUNT(*);
-- Functions on ORDER BY columns
SELECT name FROM test WHERE name < 'Adam' ORDER BY LENGTH(name) FETCH FIRST 2 ROWS ONLY;
SELECT name, birth_date, score FROM test WHERE birth_date > '2050-01-01' AND score > 95 ORDER BY LENGTH(name), birth_date FETCH FIRST 5 ROWS ONLY;
SELECT name, birth_date FROM test WHERE birth_date > '2050-01-01' AND score > 95 AND LENGTH(name) < 10 ORDER BY LENGTH(name) DESC, birth_date FETCH FIRST 5 ROWS ONLY;
SELECT name, birth_date FROM test WHERE birth_date > '2050-01-01' AND score > 95 AND LENGTH(name) = 10 ORDER BY LENGTH(name) DESC, birth_date FETCH FIRST 5 ROWS ONLY;
-- Simple arithmetic operators
SELECT 9 * 5;
FROM test SELECT 21 - score FETCH FIRST 5 ROWS ONLY;
FROM ranks SELECT value*4 FETCH FIRST 5 ROWS ONLY;
FROM ranks SELECT 1000 / value ORDER BY rowid DESC FETCH FIRST 5 ROWS ONLY;
FROM test SELECT score % 4 AS mod, COUNT(*) WHERE rowid < 100000 GROUP BY mod FETCH FIRST 5 ROWS ONLY;
FROM SEQUENCE SELECT 5 + 3 * 3, 5 * 3 + 3, LENGTH('cat') * 2, 2 + LENGTH('CAT' || 'DOG') + value = 9 LIMIT 2;
SELECT (5 + 3) * 3, 5 * (3 + 3), (5 + 3 * 3), (5 * 3 + 3), ((5 + 3) * 3), (5 + (3 * 3));
-- Operator Expressions
FROM CALENDAR WHERE date BETWEEN CURRENT_DATE AND CURRENT_DATE + 3 SELECT ordinalDate;
FROM CALENDAR WHERE date >= CURRENT_DATE - 2 SELECT julian, date, date = CURRENT_DATE LIMIT 5;
-- Dates
SELECT TODAY(), NOW(), DATE(NOW()), TIME(NOW()), CLOCK();
-- Date arithmetic
SELECT '2023-07-25' + 5, '2023-07-25' - 5, '2023-08-25' - '2023-07-25';
FROM test SELECT birth_date, birth_date + 5, birth_date - 5, TODAY() - birth_date FETCH FIRST 5 ROWS ONLY;
SELECT TODAY() + 10, DATE_DIFF(TODAY() + 10, '2000-01-01') AS a;
-- FILTER clause
FROM test SELECT COUNT(*) AS all, COUNT(*) FILTER (WHERE id % 2 = 0) AS even, COUNT(*) FILTER(WHERE id % 2 = 1) AS odd;
-- Temp Tables
CREATE TEMP TABLE ttt AS FROM SEQUENCE LIMIT 10; FROM ttt ORDER BY value DESC FETCH FIRST 5 ROWS ONLY;
CREATE TEMP TABLE tt2 AS FROM SEQUENCE LIMIT 2; INSERT INTO tt2 VALUES (14),(15),(16); TABLE tt2;
CREATE TEMP TABLE tt3 AS FROM (FROM test FETCH FIRST 40 ROWS ONLY) ORDER BY score, name; FROM tt3 GROUP BY score SELECT score, COUNT(*) FETCH FIRST 5 ROWS ONLY;
-- IN Operator
FROM suits WHERE name IN ('spades');
FROM suits WHERE name IN ('spades','hearts');
FROM suits WHERE name IN ('spades','hearts','diamonds');
-- Avoid stack smashing
FROM suits WHERE name IN ('spades','spades','spades','spades','spades','spades','spades','spades','spades','spades','spades','spades');
-- CAST
SELECT CAST('99' AS INT);
SELECT CAST('00:15:00' AS INT);
SELECT CAST('00:15:00' AS INTERVAL);
SELECT CAST(900 AS DURATION);
SELECT INTERVAL('00:15:00');
-- DateTime Arithmetic
SELECT '2024-01-18T10:00:00' + 9000;
SELECT 900000 + '2024-01-18T10:00:00';
SELECT '2024-01-18T10:00:00' - 900
SELECT '2024-01-18T10:00:00' - '2024-01-18T00:00:00';
SELECT '2024-01-18T10:00:00' < '2024-01-01T00:00:00';
SELECT '2024-01-18T10:00:00' > '2024-01-01T00:00:00';
-- Function on *
FROM test LIMIT 5 SELECT LEFT(*, 2) AS left_;
FROM(FROM test LIMIT 5) SELECT LISTAGG(*) AS agg_;
-- Subqueries in SELECT
SELECT (SELECT LISTAGG(name) FROM suits) AS n FROM ranks LIMIT 2;
SELECT (SELECT SUM(value) FROM ranks) + 10 FROM suits;
-- Efficient Index sorting
FROM test ORDER BY name ASC, birth_date ASC LIMIT 5;
FROM test ORDER BY name DESC, birth_date DESC LIMIT 5;
FROM test ORDER BY name ASC, birth_date DESC LIMIT 5;
FROM test ORDER BY birth_date ASC LIMIT 5;
FROM test ORDER BY birth_date DESC LIMIT 5;
-- Constants
SELECT 'CAT', 10, 'CURRENT_DATE', CURRENT_DATE = TODAY(), NULL;
-- Universal column aliasing
FROM SEQUENCE AS t(i), SEQUENCE AS s(j)  WHERE t.i < 5 AND s.j < 2;
-- VERSION() does not shadow a column named VERSION
SELECT VERSION FROM (VALUES ('column')) AS t (VERSION);
SELECT VERSION() = VERSION FROM (VALUES ('column')) AS t (VERSION);
SELECT CSVDB_VERSION = VERSION();
SELECT LENGTH(VERSION()) > 0;
-- Planner bugs: LIMIT must apply to groups, not rows fed into GROUP BY on an index
FROM test GROUP BY name SELECT name, COUNT(*) LIMIT 5;
-- Planner bugs: INDEX()/UNIQUE() source must still honour ORDER BY
FROM test WHERE INDEX('test__name.index') = 'Claude MILLS' ORDER BY score SELECT name, score LIMIT 3;
-- Planner bugs: OR (via IN) multi-source plan must fully sort before SELECT
FROM suits WHERE name IN ('spades','diamonds','hearts') ORDER BY name SELECT name;
-- Planner bugs: NEVER predicate/join must still emit aggregate row (COUNT=0)
FROM suits JOIN ranks ON 1 = 0 SELECT COUNT(*);
-- Medium-impact planner bugs
-- optimiseNodes treats cross-table WHERE as first-table-only (crash / wrong plan)
FROM lefty AS l, righty AS r WHERE l.id = r.id SELECT l.name, r.name ORDER BY l.id;
-- GROUP BY + OFFSET offsets rows inside one group list instead of skipping groups
FROM SEQUENCE WHERE value < 8 GROUP BY value SELECT value, COUNT(*) OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY;
-- Indexes for UNIQUE_RANGE + INDEX_JOIN secondary-sort bug
CREATE UNIQUE INDEX ON ranks (value);
CREATE INDEX ON dupes (key);
-- haveNonUniqueJoins ignores INDEX_JOIN so secondary ORDER BY is dropped
FROM ranks JOIN dupes ON ranks.value = dupes.key ORDER BY ranks.value ASC, dupes.label DESC SELECT ranks.name, dupes.label LIMIT 4;
-- Empty LIKE must not under-read the pattern
FROM test WHERE name LIKE '' SELECT name LIMIT 1;
-- Covering index: SELECT only indexed columns → COVERING INDEX SEEK on CSV
FROM test WHERE name = 'Walter KELLY' SELECT name;
-- Covering index opt-out: CALENDAR has no real index file (VFS flag off)
FROM CALENDAR WHERE date = '2024-01-01' SELECT date;
-- GROUP BY + ORDER BY: LIMIT/OFFSET must apply after sort, not during materialise
FROM ranks GROUP BY name SELECT name, COUNT(*) ORDER BY name OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY;
-- Qualified WHERE must still use index without mutating the live predicate
FROM test AS t WHERE t.name = 'Walter KELLY' SELECT t.name LIMIT 1;
-- UNIQUE index ORDER BY + non-unique join must keep secondary sort (no UNIQUE_RANGE)
FROM ranks, suits ORDER BY ranks.value ASC, suits.name DESC SELECT ranks.name, suits.name LIMIT 6;
-- Plan nodes are deep-copied: covering index remaps must not break SELECT
FROM test WHERE name = 'Walter KELLY' SELECT name ORDER BY name LIMIT 1;
-- Index-aware predicate reorder: unindexed range before indexed EQ still seeks
FROM test WHERE score > 90 AND name = 'Walter KELLY' SELECT name, score LIMIT 3;
-- Unique OR: duplicate IN values collapse (two sources, not three)
FROM suits WHERE name IN ('spades','spades','hearts') SELECT name;
-- Unique OR: all-duplicate IN promotes to a single equality predicate
FROM suits WHERE name IN ('spades','spades') SELECT name;
-- Drop tautology predicates: AND 1=1 must not leave ACCESS BY ROWID filters
FROM test WHERE name = 'Walter KELLY' AND 1 = 1 SELECT name LIMIT 1;
-- Indexed EQ preferred over unindexed EQ when both are first-table
FROM ranks WHERE name = 'Ace' AND value = 1 SELECT name, value;
-- BETWEEN dual-bound index seek (no residual value filter)
FROM ranks WHERE value BETWEEN 2 AND 4 SELECT name, value;
-- Redundant lower bounds fold to the tighter one
FROM ranks WHERE value >= 2 AND value >= 4 SELECT value ORDER BY value;
-- Rowid division algebra: value/10 = 5 → [50,60)
FROM SEQUENCE WHERE value < 100 AND value / 10 = 5 SELECT value;
-- Rowid multiplication algebra: value*10 = 50 → value = 5
FROM SEQUENCE WHERE value < 20 AND value * 10 = 50 SELECT value;
-- Same-column IN without LIMIT → per-arm index seeks
FROM test WHERE name IN ('Walter KELLY','Claude MILLS') SELECT name ORDER BY name, score LIMIT 5;
-- Transitive join predicate: l.id = r.id AND l.id = 2 → filter/join on 2
FROM lefty AS l JOIN righty AS r ON l.id = r.id WHERE l.id = 2 SELECT l.name, r.name;
-- ORDER BY + EQ with tiny LIMIT may use index-ordered scan
FROM test WHERE score = 42 ORDER BY name LIMIT 3;
