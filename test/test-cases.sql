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
-- Rowid division algebra: value/10 = 5 → value = 50 (float division)
-- Also exercises EXPLAIN with multiple predicates on one plan step
FROM SEQUENCE WHERE value < 100 AND value / 10 = 5 SELECT value;
-- Rowid multiplication algebra: value*10 = 50 → value = 5
FROM SEQUENCE WHERE value < 20 AND value * 10 = 50 SELECT value;
-- Same-column IN without LIMIT → per-arm index seeks
FROM test WHERE name IN ('Walter KELLY','Claude MILLS') SELECT name ORDER BY name, score LIMIT 5;
-- Transitive join predicate: l.id = r.id AND l.id = 2 → filter/join on 2
FROM lefty AS l JOIN righty AS r ON l.id = r.id WHERE l.id = 2 SELECT l.name, r.name;
-- ORDER BY + EQ with tiny LIMIT may use index-ordered scan
FROM test WHERE score = 42 ORDER BY name LIMIT 3;
-- Float support
SELECT 1.5 * 2, 5 / 2, 1.5 + 2.25, DIV(36, 5), MOD(5.5, 2), POW(2, 0.5);
SELECT 10.5 > 10, 1.5 = 1.50, 3.2 < 3.7, 2.5 >= 2.5;
FROM ranks SELECT AVG(value);
FROM (VALUES (10.5), (2), (10), (2.25)) AS t(v) ORDER BY v;
-- Math functions
SELECT ABS(-3.5), SQRT(9), SQRT(2), FLOOR(3.7), FLOOR(-3.7), CEIL(3.2), CEILING(-3.2);
SELECT ROUND(3.5), ROUND(3.14159, 2), ROUND(1234.56, -2);
SELECT EXP(0), EXP(1), LN(2.718281828459), LOG10(1000);
-- Complex numbers
SELECT COMPLEX(3, 4);
SELECT 3 + 4 * I;
SELECT SQRT(-1);
SELECT ABS('3+4i');
SELECT RE('3+4i'), IM('3+4i');
SELECT CONJ(3 + 4 * I);
SELECT COMPLEX(1, 2) + COMPLEX(3, 4);
SELECT COMPLEX(3, 4) * COMPLEX(0, 1);
SELECT '3+4i' = COMPLEX(3, 4), '3+4i' != COMPLEX(3, 5), '3+4i' < COMPLEX(3, 5);
FROM (VALUES ('1+2i'), ('3+4i'), ('5')) AS t(z) SELECT SUM(z), AVG(z);
FROM (VALUES ('3+4i'), ('1+2i'), ('2')) AS t(z) ORDER BY z SELECT z;
FROM (VALUES ('3+4i'), (10), (2)) AS t(v) SELECT MIN(v), MAX(v);
-- HAVING
FROM test WHERE name LIKE 'Adam WE%' GROUP BY name HAVING COUNT(*) > 1 ORDER BY name SELECT name, COUNT(*);
FROM test WHERE name LIKE 'Adam K%' GROUP BY name HAVING SUM(score) > 500 ORDER BY score DESC SELECT name, COUNT(*), SUM(score) AS score;
FROM SEQUENCE WHERE value < 8 GROUP BY value HAVING COUNT(*) = 1 ORDER BY value SELECT value, COUNT(*);
FROM ranks GROUP BY name HAVING COUNT(*) = 1 ORDER BY name SELECT name, COUNT(*) OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY;
FROM dupes GROUP BY key HAVING COUNT(*) > 1 ORDER BY key SELECT key, COUNT(*), LISTAGG(label);
SELECT COUNT(*) FROM suits HAVING COUNT(*) = 4;
-- DISTINCT
FROM suits SELECT DISTINCT name ORDER BY name;
FROM dupes SELECT DISTINCT key ORDER BY key;
FROM suits SELECT DISTINCT name ORDER BY name DESC;
-- DISTINCT ON
FROM dupes SELECT DISTINCT ON (key) key, label ORDER BY key ASC, label ASC;
FROM dupes SELECT DISTINCT ON (key) key, label ORDER BY key ASC, label DESC;
-- WHERE OR
FROM suits WHERE name = 'spades' OR name = 'hearts' ORDER BY name SELECT name;
FROM ranks WHERE value = 1 OR name = 'King' ORDER BY value SELECT value, name;
FROM ranks WHERE value = 1 OR value = 13 AND name = 'Nope' SELECT value, name;
FROM ranks WHERE value = 1 AND name = 'Ace' OR value = 13 ORDER BY value SELECT value, name;
FROM ranks WHERE value = 1 OR name = 'Ace' SELECT value, name;
FROM ranks SELECT COUNT(*) FILTER (WHERE value = 1 OR value = 13);
FROM ranks GROUP BY name HAVING name = 'Ace' OR name = 'King' ORDER BY name SELECT name;
-- JOIN ON OR (same parseNodeList plumbing)
FROM lefty AS l JOIN righty AS r ON l.id = r.id OR l.name = r.name SELECT l.id, l.name, r.id, r.name ORDER BY l.id;
FROM lefty AS l JOIN righty AS r ON l.id = r.id OR l.id = 99 SELECT l.id, r.id ORDER BY l.id, r.id;
FROM lefty AS l JOIN righty AS r ON l.id = 1 OR l.id = r.id SELECT l.id, r.id ORDER BY l.id, r.id;
FROM lefty AS l JOIN righty AS r ON l.id = r.id AND l.name != 'x' SELECT l.id ORDER BY l.id;
-- Boolean parentheses
FROM ranks WHERE (value = 1 OR value = 13) AND name != 'Nope' ORDER BY value SELECT value, name;
FROM ranks WHERE value = 1 OR (value = 13 AND name = 'Nope') SELECT value, name;
FROM ranks WHERE (value = 1 OR value = 13) AND name = 'Nope' SELECT value, name;
FROM ranks WHERE ((value = 1 OR value = 2) AND value < 10) OR value = 13 ORDER BY value SELECT value, name;
FROM ranks WHERE (value = 1) SELECT value, name;
FROM lefty AS l JOIN righty AS r ON (l.id = r.id OR l.name = r.name) SELECT l.id ORDER BY l.id;
FROM ranks SELECT COUNT(*) FILTER (WHERE (value = 1 OR value = 13) AND name != 'x');
-- Index rowid is the last column by position (header need not be named "rowid")
FROM suits WHERE UNIQUE('suits_by_name.unique') = 'hearts' SELECT name, symbol;
FROM suits WHERE UNIQUE('suits_by_name.unique') >= 'diamonds' ORDER BY name SELECT name;
-- Composite index: EQ on leading key + ORDER BY trailing key → no SORT
FROM test WHERE name = 'Walter KELLY' ORDER BY birth_date SELECT name, birth_date LIMIT 3;
-- Composite index: residual on trailing key filtered during index walk (covering)
FROM test WHERE name = 'Walter KELLY' AND birth_date > '0500-01-01' SELECT name, birth_date LIMIT 5;
-- Composite index: residual on non-index column still uses ACCESS BY ROWID
FROM test WHERE name = 'Walter KELLY' AND score > 50 SELECT name, score LIMIT 5;
-- DATE_TRUNC
SELECT DATE_TRUNC(YEAR, '2023-10-31'), DATE_TRUNC(QUARTER, '2023-10-31'), DATE_TRUNC(MONTH, '2023-10-31');
SELECT DATE_TRUNC(WEEK, '2023-10-31'), DATE_TRUNC(HOUR, '2023-10-31T15:45:22'), DATE_TRUNC(MINUTE, '2023-10-31T15:45:22');
SELECT DATE_TRUNC(DECADE, '2023-10-31'), DATE_TRUNC(CENTURY, '2023-10-31'), DATE_TRUNC(MILLENNIUM, '2023-10-31');
SELECT DATE_TRUNC(DECADE, '1995-06-15'), DATE_TRUNC(CENTURY, '1995-06-15'), DATE_TRUNC(MILLENNIUM, '1995-06-15');
-- Unit-aware DATE_ADD / DATE_SUB (path A and path B)
SELECT DATE_ADD('2023-01-31', 1, MONTH), DATE_ADD('2023-01-31', INTERVAL 1 MONTH);
SELECT DATE_ADD('2023-01-31', 1, YEAR), DATE_SUB('2024-03-31', INTERVAL 1 MONTH);
SELECT DATE_ADD('2023-10-30', 1, WEEK), DATE_ADD('2023-10-31T12:00:00', 2, HOUR);
SELECT DATE_SUB('2023-10-31T12:00:00', 90, MINUTE), DATE_ADD('2023-01-31', 1);
-- INTERVAL constants
SELECT INTERVAL 1 WEEK, INTERVAL 1 DAY, INTERVAL 1 HOUR, INTERVAL 15 MINUTE, INTERVAL 30 SECOND;
SELECT '2023-07-25' + INTERVAL 1 WEEK, '2024-01-18T10:00:00' + INTERVAL 1 HOUR;
-- Timezone parse / AT_TIME_ZONE / UTC Z
SELECT '2023-10-31T03:00:00Z';
SELECT AT_TIME_ZONE('2023-10-31T03:00:00Z', '+09:00');
SELECT AT_TIME_ZONE('2023-10-31T12:00:00+09:00', 'UTC');
SELECT '2023-10-31T08:00:00Z' + 3600;
SELECT EXTRACT(HOUR FROM '2023-10-31T15:45:22'), EXTRACT(MINUTE FROM '2023-10-31T15:45:22'), EXTRACT(SECOND FROM '2023-10-31T15:45:22');
-- Bare timezone keywords (LOCAL/UTC/Z) for AT_TIME_ZONE and NOW
SELECT AT_TIME_ZONE('2023-10-31T03:00:00Z', UTC);
SELECT AT_TIME_ZONE('2023-10-31T03:00:00Z', Z);
SELECT AT_TIME_ZONE('2023-10-31T03:00:00Z', '+00:00');
SELECT AT_TIME_ZONE('2023-10-31T03:00:00Z', LOCAL);
SELECT NOW(UTC);
SELECT NOW(LOCAL);
SELECT NOW('LOCAL');
-- SEQUENCE % optimisation: value % k = n → arithmetic progression
FROM SEQUENCE WHERE value < 20 AND value % 3 = 2 SELECT value;
-- Constant % column must not be dropped by SEQUENCE (evaluate per row)
FROM SEQUENCE WHERE value BETWEEN 1 AND 20 AND 15 % value = 0 SELECT value;
-- Prime: no divisors in range → empty (must not return every row)
FROM SEQUENCE WHERE value BETWEEN 2 AND 30 AND 97 % value = 0 SELECT value;
-- Multi-MOD combined via LCM/CRT (multiples of 6)
FROM SEQUENCE WHERE value < 30 AND value % 2 = 0 AND value % 3 = 0 SELECT value;
-- Incongruent MOD pair → empty
FROM SEQUENCE WHERE value < 30 AND value % 2 = 0 AND value % 2 = 1 SELECT value;
-- SEQUENCE value is rowid: algebra rewrite value+1=10 → value=9
FROM SEQUENCE WHERE value + 1 = 10 SELECT value, rowid;
-- CALENDAR finer bounds inside year
FROM CALENDAR WHERE year = 2024 AND month = 6 SELECT COUNT(*);
FROM CALENDAR WHERE year = 2024 AND month = 6 ORDER BY date SELECT date LIMIT 1;
FROM CALENDAR WHERE year = 2024 AND month = 6 ORDER BY date DESC SELECT date LIMIT 1;
FROM CALENDAR WHERE year = 2020 AND quarter = 2 SELECT COUNT(*);
FROM CALENDAR WHERE year = 2024 AND yearday = 60 SELECT date;
FROM CALENDAR WHERE year = 2024 AND month = 2 AND day = 29 SELECT date;
-- CALENDAR weekday stepping within a bounded range
FROM CALENDAR WHERE date BETWEEN '2024-01-01' AND '2024-01-31' AND weekday = 3 SELECT date;
-- PI()
SELECT PI();
SELECT EXP(I * PI());
SELECT ROUND(PI(), 5);
-- EPHEMERIS: new/full moon in Jan 2024
FROM EPHEMERIS WHERE date BETWEEN '2024-01-01' AND '2024-01-31' AND isNewMoon = 1 SELECT date;
FROM EPHEMERIS WHERE date BETWEEN '2024-01-01' AND '2024-01-31' AND isFullMoon = 1 SELECT date;
FROM EPHEMERIS WHERE date = '2024-06-21' SELECT ROUND(solarDeclination, 1);
FROM EPHEMERIS WHERE date = '2024-01-11' SELECT isNewMoon, isFullMoon, isFirstQuarter, isLastQuarter;
FROM EPHEMERIS WHERE date BETWEEN '2024-01-01' AND '2024-01-07' SELECT date, ROUND(moonPhase, 2);
-- EPHEMERIS(lat,lon): rise/set and eclipse flags
FROM EPHEMERIS(51.477, 0.0) WHERE date = '2024-06-21' SELECT LEFT(sunRise, 13), LEFT(sunSet, 13), sunRise < sunSet;
FROM EPHEMERIS WHERE date = '2024-06-21' SELECT LENGTH(sunRise), LENGTH(sunSet);
FROM EPHEMERIS WHERE date BETWEEN '2024-04-01' AND '2024-04-15' AND isSolarEclipse = 1 AND isNewMoon = 1 SELECT date;
FROM EPHEMERIS WHERE date BETWEEN '2024-03-20' AND '2024-03-30' AND isLunarEclipse = 1 SELECT date, isFullMoon;
-- PERMUTATIONS / COMBINATIONS
FROM PERMUTATIONS(3) SELECT rank, p0, p1, p2;
FROM COMBINATIONS(5, 2) SELECT rank, c0, c1;
FROM PERMUTATIONS(4) WHERE rank < 3 SELECT p0, p1, p2, p3;
FROM COMBINATIONS(7, 3) WHERE c0 = 0 SELECT COUNT(*);
-- CRON
FROM CRON('0 9 * * 1-5') WHERE date BETWEEN '2024-01-01' AND '2024-01-07' SELECT datetime, weekday;
FROM CRON('0/15 9-10 * * *') WHERE date = '2024-01-01' SELECT datetime, hour, minute;
FROM CRON('0 9 * * 1-5') WHERE date BETWEEN '2024-01-01' AND '2024-01-07' SELECT COUNT(*);
-- table.* expansion (qualified star)
FROM suits AS s SELECT s.*;
FROM suits AS s, ranks AS r WHERE s.name = 'hearts' AND r.value = 1 SELECT r.*;
FROM CALENDAR AS c JOIN EPHEMERIS AS e ON e.julian = c.julian WHERE c.date = '2024-01-11' SELECT e.date, e.isNewMoon;
-- ISO week parse: Jan 1 Thursday (2026) must not shift week 1 by +7
FROM CALENDAR WHERE date = '2026-W01-1' SELECT date, weekString, weekday;
FROM CALENDAR WHERE date = '2026-W48-1' SELECT date, weekString, weekday;
FROM CALENDAR WHERE weekString = '2026-W48' SELECT date, weekString;
-- ISO week parse: Jan 1 Friday still shifts correctly
FROM CALENDAR WHERE date = '2021-W01-1' SELECT date, weekString, weekday;
FROM CALENDAR WHERE date = '2023-W08-6' SELECT date, weekString, weekday;
-- LIMIT pushdown onto CALENDAR for all-LEFT joins with first-table WHERE
FROM CALENDAR AS c LEFT JOIN EPHEMERIS AS e ON e.date = c.date WHERE c.weekString >= '2026-W48' SELECT e.date, e.moonPhase LIMIT 5;
FROM CALENDAR AS c LEFT JOIN EPHEMERIS AS e ON e.julian = c.julian WHERE c.date >= '2026-11-24' SELECT e.date LIMIT 3;
-- Scalar subquery with WHERE (must not be mistaken for a boolean group)
SELECT (SELECT COUNT(*) FROM suits WHERE name = 'spades') AS n;
SELECT (SELECT COUNT(*) FROM suits WHERE name = 'spades' AND symbol = '♠') AS n;
SELECT (SELECT name FROM suits WHERE name = 'hearts' OR name = 'spades' ORDER BY name LIMIT 1) AS n;
-- Multiple scalar subqueries in one SELECT (join.function must be ALWAYS)
SELECT (SELECT COUNT(*) FROM suits) AS a, (SELECT COUNT(*) FROM ranks) AS b;
SELECT (SELECT COUNT(*) FROM suits) AS a, (SELECT COUNT(*) FROM ranks) AS b FROM ranks LIMIT 1;
FROM ranks SELECT (SELECT COUNT(*) FROM suits) AS a, (SELECT SUM(value) FROM ranks) AS b LIMIT 1;
-- Correlated scalar subqueries (FUNC_SUBQUERY + parse-once clone + AST bind).
-- Uncorrelated scalar subqueries are folded to constants during resolve.
FROM ranks AS r SELECT name, value, (SELECT COUNT(*) FROM ranks AS r2 WHERE r2.value < r.value) AS less_count ORDER BY value LIMIT 5;
FROM suits AS s SELECT name, (SELECT COUNT(*) FROM ranks WHERE value = LENGTH(s.name)) AS matches;
FROM ranks AS r WHERE (SELECT COUNT(*) FROM ranks AS r2 WHERE r2.value > r.value) = 0 SELECT name, value;
FROM SEQUENCE WHERE value < 4 SELECT value, (SELECT value * 2) AS doubled;
-- Multi-arg CHAR/CHR builds a string from code points (NULLs skipped)
SELECT CHAR(72, 101, 108, 108, 111);
SELECT CHR(65, 66, 67);
SELECT CHAR(72, NULL, 105);
-- Statistical aggregates (sample vs population)
-- Classic set 2,4,4,4,5,5,7,9 → μ=5, VAR_POP=4, VAR_SAMP=32/7, STDDEV_POP=2
FROM (VALUES (2), (4), (4), (4), (5), (5), (7), (9)) AS t(v) SELECT AVG(v), VAR_POP(v), VARIANCE(v), STDDEV_POP(v), STDDEV(v);
FROM (VALUES (2), (4), (4), (4), (5), (5), (7), (9)) AS t(v) SELECT VAR_SAMP(v), STDDEV_SAMP(v);
-- Fewer than 2 non-nulls → sample stats NULL; population still defined for n=1
FROM (VALUES (10)) AS t(v) SELECT VAR_POP(v), VAR_SAMP(v), STDDEV_POP(v), STDDEV_SAMP(v);
-- FILTER must pack included rows densely (regression for skipped indices)
FROM (VALUES (1), (3), (5)) AS t(v) SELECT AVG(v) FILTER (WHERE v > 1), STDDEV(v) FILTER (WHERE v > 1), COUNT(v) FILTER (WHERE v > 1);
-- Discrete MEDIAN: odd → middle; even → lower middle (always from the set)
FROM (VALUES (1), (3), (2), (9), (5)) AS t(v) SELECT MEDIAN(v);
FROM (VALUES (1), (3), (2), (9)) AS t(v) SELECT MEDIAN(v);
-- MODE: strings + numeric ties (lexicographically smallest wins)
FROM (VALUES ('apple'), ('apple'), ('banana'), ('banana'), ('banana'), ('cherry')) AS t(v) SELECT MODE(v);
FROM (VALUES ('apple'), ('apple'), ('banana'), ('banana'), ('cherry')) AS t(v) SELECT MODE(v);
FROM (VALUES (1), (2), (2), (3), (3), (3)) AS t(v) SELECT MODE(v);
-- Constant-only aggregates must not be folded via evaluateFunction
SELECT SUM(1), MEDIAN(1), MODE('x');
-- PERCENTILE aliases DISC; CONT interpolates; both match MEDIAN at 0.5 for DISC
FROM (VALUES (1), (2), (3), (4)) AS t(v) SELECT PERCENTILE(v, 0.5), PERCENTILE_DISC(v, 0.5), PERCENTILE_CONT(v, 0.5), MEDIAN(v);
FROM (VALUES (1), (2), (3), (4)) AS t(v) SELECT PERCENTILE_DISC(v, 0), PERCENTILE_DISC(v, 1), PERCENTILE(v, 0), PERCENTILE(v, 1);
FROM (VALUES (10), (20), (30), (40), (50)) AS t(v) SELECT PERCENTILE(v, 0.25), PERCENTILE_DISC(v, 0.25), PERCENTILE_CONT(v, 0.25);
-- LISTAGG optional separator (default comma)
FROM (VALUES ('a'), ('b'), ('c')) AS t(v) SELECT LISTAGG(v), LISTAGG(v, '|'), LISTAGG(v, '');
-- RIGHT JOIN rewritten as LEFT JOIN by swapping tables
FROM lefty AS l RIGHT JOIN righty AS r ON l.id = r.id SELECT l.id, l.name, r.id, r.name ORDER BY r.id;
FROM lefty AS l RIGHT OUTER JOIN righty AS r ON l.id = r.id SELECT l.id, l.name, r.id, r.name ORDER BY r.id;
FROM lefty AS l LEFT OUTER JOIN righty AS r ON l.id = r.id SELECT l.id, l.name, r.id, r.name ORDER BY l.id;
FROM suits RIGHT JOIN ranks ON LENGTH(ranks.name) = LENGTH(suits.name) SELECT suits.name, ranks.name ORDER BY ranks.name, suits.name;
-- CROSS JOIN keyword (same as comma / JOIN without ON)
FROM lefty AS l CROSS JOIN righty AS r WHERE r.id = 4 SELECT l.id, r.name ORDER BY l.id;
-- IN (subquery) / = ANY / > ALL (uncorrelated + correlated)
FROM suits WHERE name IN (SELECT name FROM suits WHERE name = 'hearts') SELECT name;
FROM suits WHERE name = ANY (SELECT name FROM suits WHERE name IN ('spades','diamonds')) SELECT name ORDER BY name;
FROM suits WHERE name = SOME (SELECT name FROM suits WHERE name = 'clubs') SELECT name;
FROM ranks WHERE value > ALL (SELECT value FROM ranks WHERE value < 5) SELECT name, value ORDER BY value;
FROM ranks WHERE value > ANY (SELECT value FROM ranks WHERE value = 3 OR value = 5) SELECT name, value ORDER BY value;
FROM ranks WHERE value = ALL (SELECT value FROM ranks WHERE value = 1) SELECT name, value;
-- Empty subquery: ANY false, ALL true
FROM suits WHERE name = ANY (SELECT name FROM suits WHERE name = 'nope') SELECT name;
FROM suits WHERE name = ALL (SELECT name FROM suits WHERE name = 'nope') SELECT name ORDER BY name LIMIT 1;
-- Correlated IN / ANY
FROM suits AS s WHERE name IN (SELECT name FROM suits WHERE LENGTH(name) = LENGTH(s.name) AND name = 'hearts') SELECT name;
FROM ranks AS r WHERE value = ANY (SELECT value FROM ranks WHERE value = r.value AND name = 'Ace') SELECT name, value;
-- Whitespace between '(' and SELECT must still parse as a subquery
FROM suits AS s WHERE name = ANY ( SELECT name FROM suits WHERE LENGTH(name) = LENGTH(s.name)) SELECT name ORDER BY name;
-- Subquery may start with any query-start clause (not only SELECT/FROM)
FROM suits WHERE name IN (WHERE name = 'hearts' FROM suits SELECT name) SELECT name;
FROM suits WHERE name = ANY (WHERE name IN ('spades','diamonds') FROM suits SELECT name) SELECT name ORDER BY name;
SELECT (LIMIT 1 FROM suits SELECT name ORDER BY name);
SELECT (TABLE suits);
FROM suits WHERE name IN (TABLE suits) SELECT name ORDER BY name;
SELECT (WITH x AS (FROM suits SELECT name LIMIT 1) FROM x SELECT name);
-- Calendar INTERVAL under +/- rewritten to DATE_ADD / DATE_SUB
SELECT '2023-01-31' + INTERVAL 1 MONTH, INTERVAL 1 MONTH + '2023-01-31';
SELECT '2024-03-31' - INTERVAL 1 MONTH, '2023-01-31' + INTERVAL 1 YEAR;
SELECT '2023-01-31' + INTERVAL 10 MILLENNIA, '2023-01-31' + INTERVAL 1 DECADE;
-- ISO-8601 period strings with +/- and DATE_ADD / DATE_SUB (quoted like dates)
SELECT '2023-01-31' + 'P1M', 'P1M' + '2023-01-31', '2024-03-31' - 'P1M';
SELECT '2023-01-31' + 'P1Y2M', DATE_ADD('2023-01-31', 'P10Y'), '2023-01-31' + 'P2W';
SELECT '2023-10-31T12:00:00' + 'PT1H30M', DATE_SUB('2023-10-31T12:00:00', 'PT90M');
SELECT '2023-01-31' + '-P1Y', '2023-01-31' - '-P1M';
-- ISO-8601 period * integer (and date + period * n)
SELECT 'P1Y6M' * 2, 2 * 'P1Y6M', 'PT15M' * 4;
SELECT '2023-01-31' + 'P1Y6M' * 2, '2023-01-31' + 2 * 'P6M';
SELECT '2023-01-31' + 'P1Y' * -1, 'P1M' * 0;
-- Unary minus after operators (1 + -2, 1 * -2 + 3)
SELECT 1 + -2, 1 * -2 + 3, 10 / -2;
-- PK as identity in SELECT; range seek below min key
SELECT PK(id) FROM test WHERE PK(id) = 1000;
SELECT PK(id) > 1000 FROM test WHERE PK(id) = 10;
FROM test WHERE PK(id) > 1 LIMIT 1;
FROM test WHERE PK(id) > 9 LIMIT 1;
FROM test WHERE PK(id) >= 1 LIMIT 1;
-- IN (literal/subquery) + ORDER BY must seek then SORT, not scan order index
FROM CALENDAR WHERE date IN ('2026-01-03','2026-03-03','2026-02-02') ORDER BY julian SELECT date, julian;
FROM CALENDAR WHERE date IN (SELECT date FROM EPHEMERIS WHERE date BETWEEN '2026-01-01' AND '2026-04-01' AND isFullMoon = 1) SELECT date, julian ORDER BY julian;
-- FROM / CTE subquery bodies longer than 255 bytes (heap Query*, not table.name)
FROM (FROM suits WHERE name = 'hearts' OR name = 'hearts' OR name = 'hearts' OR name = 'hearts' OR name = 'hearts' OR name = 'hearts' OR name = 'hearts' OR name = 'hearts' OR name = 'hearts' OR name = 'hearts' OR name = 'hearts' OR name = 'hearts' OR name = 'hearts' OR name = 'hearts' OR name = 'hearts' OR name = 'hearts' OR name = 'hearts' OR name = 'hearts' OR name = 'hearts' OR name = 'hearts') AS t SELECT name, symbol;
WITH long_cte AS (FROM suits WHERE name = 'spades' OR name = 'spades' OR name = 'spades' OR name = 'spades' OR name = 'spades' OR name = 'spades' OR name = 'spades' OR name = 'spades' OR name = 'spades' OR name = 'spades' OR name = 'spades' OR name = 'spades' OR name = 'spades' OR name = 'spades' OR name = 'spades' OR name = 'spades' OR name = 'spades' OR name = 'spades' OR name = 'spades' OR name = 'spades') FROM long_cte SELECT name;
-- UNION ALL (bag concat; outer ORDER/LIMIT; left-assoc chain)
VALUES (1),(2) UNION ALL VALUES (3),(4);
FROM suits WHERE name = 'hearts' SELECT name UNION ALL FROM suits WHERE name = 'spades' SELECT name;
VALUES (1) UNION ALL VALUES (2) UNION ALL VALUES (3);
VALUES (3),(1) UNION ALL VALUES (2) ORDER BY col1;
VALUES (1),(2),(3) UNION ALL VALUES (4),(5) LIMIT 3;
-- UNION / INTERSECT / EXCEPT (set semantics)
VALUES (1),(2),(2) UNION VALUES (2),(3);
VALUES (1),(2),(2) UNION DISTINCT VALUES (2),(3);
VALUES (1),(2) INTERSECT VALUES (2),(3),(2);
VALUES (1),(2),(2),(3) EXCEPT VALUES (2),(4);
VALUES (3),(1),(2) UNION VALUES (2) ORDER BY col1;
VALUES (1),(2) INTERSECT VALUES (2),(1) LIMIT 1;
