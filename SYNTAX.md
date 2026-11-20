# csvdb Syntax Guide

csvdb uses a custom SQL dialect for querying CSV (and related) files. Clauses are declarative and may appear in any order. `SELECT` defaults to `*`, and `FROM` defaults to `stdin`.

Multiple statements may be separated by `;`.

---

## Statement overview

| Kind | Form |
|------|------|
| Query | `[EXPLAIN] [WITH …] SELECT … FROM … WHERE … GROUP BY … ORDER BY … OFFSET … FETCH/LIMIT …` |
| Values | `VALUES (…), (…), …` |
| Table sugar | `TABLE <file>` |
| Create table | `CREATE TABLE <file> [(cols)] AS <query>` |
| Temp table | `CREATE TEMP TABLE <name> AS <query>` |
| Create view | `CREATE VIEW <file> AS <query>` |
| Create index | `CREATE [UNIQUE] INDEX [<index_file>] ON <file> (<field>, …)` |
| Insert | `INSERT INTO <file> <query>` |

---

## Query syntax

```text
[EXPLAIN]
[WITH name AS (<query>) [, name AS (<query>) …]]
SELECT <expr> [FILTER (WHERE <predicate>)] [AS alias] [, …]
FROM <source> [[AS] alias [(col_alias, …)]]
    [, <source> …]
    | [INNER|LEFT] JOIN <source> [ON <predicate> | USING <expr>]
WHERE <predicate> [AND <predicate> …]
GROUP BY <expr> [, …]
ORDER BY <expr> [ASC|DESC] [, …]
OFFSET <n> ROW[S]
FETCH {FIRST|NEXT} [<m>] ROW[S] ONLY
LIMIT <m>
```

Notes:

- Any clause may be omitted; omitted `SELECT` means `SELECT *`, omitted `FROM` means `FROM stdin`.
- Clause order is not fixed (e.g. `FROM t WHERE … SELECT … ORDER BY …` is valid).
- `EXPLAIN` (or CLI `-E` / `--explain`) prints the query plan instead of results.
- Up to 10 CTEs are supported via `WITH`.

### `SELECT`

```sql
SELECT name, score AS pts FROM test;
SELECT LEFT(*, 2) AS left_ FROM test LIMIT 5;   -- apply function to every column
SELECT COUNT(*) FILTER (WHERE id % 2 = 0) AS even FROM test;
```

- `*` expands to all columns of the involved tables.
- `table.*` expands one table’s columns.
- Column aliases: `AS alias` or (for a single double-quoted identifier) the quoted name.
- Aggregate expressions may take `FILTER (WHERE <predicate>)`.

### `FROM` and joins

```sql
FROM suits;
FROM suits AS s (n, s);                          -- table + column aliases
FROM suits, ranks WHERE value > 10;
FROM suits INNER JOIN ranks ON LENGTH(ranks.name) = LENGTH(suits.name);
FROM suits LEFT JOIN ranks ON …;
FROM suits JOIN ranks USING LENGTH(name);        -- equality on same expression
FROM (FROM SEQUENCE LIMIT 10) AS a;
FROM (VALUES ('a',1),('b',2)) AS t (first, second);
```

- Comma joins are cross joins (predicates go in `WHERE` / `ON`).
- Supported join keywords: `JOIN`, `INNER JOIN`, `LEFT JOIN`.
- Join conditions: `ON <predicate>` or `USING <expr>` (equality of that expression across the joined tables).

### `WHERE`

```sql
WHERE score = 42
WHERE name LIKE 'Walter M%'
WHERE birth_date BETWEEN CURRENT_DATE AND CURRENT_DATE + 3
WHERE name IN ('spades','hearts')
WHERE name IS NULL
WHERE name IS NOT NULL
WHERE a = 1 AND b > 2
```

Chained predicates use `AND`. `IN (…)` is expanded internally to equality/`OR`. There is no top-level `OR` / `HAVING` / `DISTINCT` / `UNION` keyword.

### `GROUP BY` / `ORDER BY` / limits

```sql
FROM test GROUP BY name SELECT name, COUNT(*);
FROM test ORDER BY score DESC, name ASC;
FROM test OFFSET 2 ROWS FETCH NEXT 5 ROWS ONLY;
FROM test LIMIT 5;
FROM test FETCH FIRST ROW ONLY;                  -- same as LIMIT 1
```

- `LIMIT <n>` is accepted as a shorter form of `FETCH`.
- `ORDER BY PK(…)` sorts by primary-key / row order (parentheses contents are ignored).

### `VALUES` and `TABLE`

```sql
VALUES ('a',1),('b',2),('c',3);
TABLE suits;                                     -- sugar for SELECT * FROM suits
```

`VALUES` columns are named `col1`, `col2`, … unless aliased: `AS t (first, second)`.

### CTEs

```sql
WITH r1 AS (FROM ranks WHERE value < 8 SELECT name, symbol)
FROM r1 ORDER BY name;
```

---

## DDL / DML

```sql
CREATE TABLE out.csv AS SELECT name, score FROM test;
CREATE TABLE out.csv (name, score) AS SELECT …;
CREATE TEMP TABLE ttt AS FROM SEQUENCE LIMIT 10;
CREATE VIEW myview AS FROM test WHERE score > 90;
CREATE INDEX ON test (name, birth_date);
CREATE UNIQUE INDEX ON test (birth_date);
CREATE UNIQUE INDEX my_idx ON test (birth_date);
INSERT INTO out.csv SELECT name FROM test;
INSERT INTO tt2 VALUES (14),(15),(16);
```

- Views are stored as `.sql` files and behave as tables when referenced.
- Temp tables live for the process lifetime.
- Indexes write `.index` / `.unique` files used by the planner.

---

## Sources and file types

| Source | Meaning |
|--------|---------|
| `file.csv` / `file` | CSV table (`.csv` tried if exact name missing) |
| `file.tsv` | Tab-separated |
| `file.col` | Fixed-width (width from first row) |
| `file.wsv` | Whitespace-separated |
| `file.sql` | View (SQL text) |
| `stdin` / `stdin.csv` | Read CSV from stdin |
| `stdin.tsv` / `stdin.col` / `stdin.wsv` | Stdin with that format |
| `CALENDAR` | Builtin calendar virtual table |
| `SEQUENCE` | Builtin integer sequence (`value` = 0 … 9999999) |

Quoted identifiers are allowed: `FROM "my file.csv"`.

---

## Expressions

### Literals and named constants

| Token | Result |
|-------|--------|
| `'string'` | String (`''` escapes a quote) |
| `123`, `1.5`, `0xFF` | Numbers (hex → decimal integer) |
| `NULL` | Empty / null value |
| `CURRENT_DATE` | Today’s date (`YYYY-MM-DD`) |
| `CURRENT_TIME` | Current time (`HH:MM:SS`) |
| `CSVDB_VERSION` | Version string (also `VERSION()`) |

### Special fields

| Name | Meaning |
|------|---------|
| `*` / `table.*` | All columns |
| `rowid` / `table.rowid` | Zero-based row index in the source |
| `ROW_NUMBER()` | One-based output row number |
| `PK(col)` | Hint: treat `col` as primary key for seeks / sorts |
| `INDEX('file.index')` | Force use of a named index |
| `UNIQUE('file.unique')` | Force use of a named unique index |

### Operators

**Arithmetic / string**

| Op | Meaning |
|----|---------|
| `+` `-` `*` `/` `%` | Arithmetic (also date/time arithmetic; see below) |
| `\|\|` | String concatenation |

**Comparison** (in `WHERE` / `ON` / `FILTER`)

| Op | Meaning |
|----|---------|
| `=` `IS` | Equal |
| `!=` `IS NOT` | Not equal |
| `<` `<=` `>` `>=` | Ordering |
| `LIKE` | Prefix match; trailing `%` is a wildcard (`name LIKE 'Wa%'`). Exact match if no `%`. |
| `BETWEEN a AND b` | `>= a AND <= b` |
| `IN (…)` | Membership (expanded to `OR` of equalities) |
| `AND` | Conjunction |

Parentheses group expressions: `(5 + 3) * 3`.

### Date / time arithmetic with `+` / `-`

| Expression | Result |
|------------|--------|
| `date + n` / `n + date` | Date plus *n* days |
| `date - n` | Date minus *n* days |
| `date - date` | Day difference |
| `datetime + n` / `n + datetime` | DateTime plus *n* seconds |
| `datetime - n` | DateTime minus *n* seconds |
| `datetime - datetime` | Second difference |
| `time + n` / `time - n` | Time ± seconds |
| `time + time` / `time - time` | Time sum / difference |

Date format: `YYYY-MM-DD`. DateTime: `YYYY-MM-DDTHH:MM:SS`. Time / duration: `HH:MM:SS`.

### Subqueries in expressions

```sql
SELECT (SELECT LISTAGG(name) FROM suits) AS n FROM ranks LIMIT 2;
SELECT (SELECT SUM(value) FROM ranks) + 10 FROM suits;
```

Scalar subqueries must start with `SELECT` or `FROM` inside `(…)`.

---

## Function reference

Unless noted, a `NULL` (empty) first argument makes the whole call `NULL`.

### Scalar — no arguments

| Function | Returns |
|----------|---------|
| `RANDOM()` | Random integer in `0 … RAND_MAX` |
| `VERSION()` | csvdb version string |
| `TODAY()` | Current date (`YYYY-MM-DD`) |
| `NOW()` | Current datetime (`YYYY-MM-DDTHH:MM:SS`) |
| `CLOCK()` | Current time (`HH:MM:SS`) |

### Scalar — string / encoding

| Function | Signature | Description |
|----------|-----------|-------------|
| `LENGTH` | `(s)` | Byte length of string |
| `LEFT` | `(s, n)` | Leftmost *n* characters |
| `RIGHT` | `(s, n)` | Rightmost *n* characters |
| `CHR` / `CHAR` | `(codepoint)` | UTF-8 character from Unicode code point |
| `CODEPOINT` | `(s)` | Code point of first UTF-8 character |
| `HEX` | `(s)` | Hex digits of each byte (MySQL-style) |
| `TO_HEX` | `(n)` | Integer as `0x…` hex |
| `W1252` | `(s)` | Repair Windows-1252 mis-decoded as UTF-8 |

Concatenation is via `||` (there is no `CONCAT()` call form in the parser).

### Scalar — maths

| Function | Signature | Description |
|----------|-----------|-------------|
| `ADD` | `(a, b, …)` | Sum (also dates/times; same as `+`) |
| `SUB` | `(a, b, …)` | Difference (same as `-`) |
| `MUL` | `(a, b, …)` | Product |
| `DIV` | `(a, b, …)` | Division (`NULL` on divide-by-zero) |
| `MOD` | `(a, b)` | Remainder (`fmod`) |
| `POW` | `(a, b)` | Power |
| `ABS` | `(x)` | Absolute value |
| `SQRT` | `(x)` | Square root (`NULL` if *x* &lt; 0) |
| `FLOOR` | `(x)` | Floor |
| `CEIL` / `CEILING` | `(x)` | Ceiling |
| `ROUND` | `(x)` or `(x, digits)` | Round; *digits* may be negative |
| `EXP` | `(x)` | *e*<sup>*x*</sup> |
| `LN` | `(x)` | Natural log (`NULL` if *x* ≤ 0) |
| `LOG10` | `(x)` | Base-10 log (`NULL` if *x* ≤ 0) |

### Scalar — date / time

| Function | Signature | Description |
|----------|-----------|-------------|
| `DATE` | `(datetime)` | Date part of a datetime |
| `TIME` | `(datetime)` | Time part of a datetime |
| `DATE_ADD` | `(date, days)` | Add days to a date |
| `DATE_SUB` | `(date, days)` | Subtract days from a date |
| `DATE_DIFF` | `(date1, date2)` | `date1 − date2` in days |
| `MAKE_DATE` | `(y [, m [, d]])` | Build date (defaults: month/day = 1) |
| `MAKE_TIME` | `(h [, m [, s]])` | Build time (defaults: 0) |
| `MAKE_DATETIME` | `(y, m, d [, h [, mi [, s]]])` | Build datetime |
| `INTERVAL` | `(x)` | Synonym for `CAST(x AS INT)` (durations → seconds) |

### `EXTRACT`

```sql
EXTRACT(<part> FROM <date-or-datetime>)
```

| Part | Result |
|------|--------|
| `YEAR` | Year |
| `MONTH` | Month 1–12 |
| `DAY` | Day of month |
| `WEEK` | ISO week number |
| `WEEKYEAR` | ISO week-year |
| `WEEKDAY` | Weekday (Mon=1 … Sun=7 style used by engine) |
| `YEARDAY` | Day of year |
| `HEYEAR` | Year + 10000 (Holocene) |
| `MILLENNIUM` | `year / 1000` |
| `CENTURY` | `year / 100` |
| `DECADE` | `year / 10` |
| `QUARTER` | Quarter 1–4 |
| `JULIAN` | Julian day number |
| `DATE` | Date string |
| `DATETIME` | Datetime string |
| `MONTH_STRING` | `YYYY-MM` |
| `WEEK_STRING` | `YYYY-Www` |
| `YEARDAY_STRING` | `YYYY-DDD` (ordinal date) |

### `CAST`

```sql
CAST(expr AS INT)         -- also AS INTERVAL; durations → seconds
CAST(expr AS DURATION)    -- seconds → HH:MM:SS
INTERVAL(expr)            -- same as CAST(expr AS INT)
```

### Aggregates

| Function | Signature | Description |
|----------|-----------|-------------|
| `COUNT` | `(*)` or `(expr)` | Row count / non-null count |
| `MIN` | `(expr)` | Minimum (numeric) |
| `MAX` | `(expr)` | Maximum (numeric) |
| `SUM` | `(expr)` | Sum |
| `AVG` | `(expr)` | Average |
| `LISTAGG` | `(expr)` or `(*)` | Concatenate non-null values |

All aggregates accept `FILTER (WHERE …)`. Aggregates imply grouping when used without `GROUP BY` (single group over the result).

---

## Builtin tables

### `SEQUENCE`

Virtual table of integers.

| Column | Description |
|--------|-------------|
| `value` | Integer from 0 up to 10 000 000 − 1 |

```sql
FROM SEQUENCE WHERE value < 10;
FROM SEQUENCE OFFSET 5 ROWS LIMIT 10;
```

### `CALENDAR`

Virtual date table (one row per Julian day). Prefer predicates on `date` / `julian` so the planner can bound the scan.

| Column | Description |
|--------|-------------|
| `julian` / `rowid` | Julian day number |
| `date` | `YYYY-MM-DD` |
| `year`, `month`, `day` | Calendar parts |
| `weekyear`, `week`, `weekday`, `yearday` | ISO / ordinal parts |
| `millenium`, `century`, `decade`, `quarter` | Coarser parts |
| `firstOfYear`, `lastOfYear` | Year bounds |
| `firstOfQuarter`, `lastOfQuarter` | Quarter bounds |
| `firstOfMonth`, `lastOfMonth` | Month bounds |
| `firstOfWeek`, `lastOfWeek` | Week bounds |
| `isLeapYear`, `isWeekend` | Flags |
| `weekdayInMonth` | Occurrence of weekday in month |
| `monthString` | `YYYY-MM` |
| `ordinalDate` | `YYYY-DDD` |
| `weekString` | `YYYY-Www` |
| `weekDate` | ISO week date string |

```sql
FROM CALENDAR WHERE date = CURRENT_DATE
SELECT julian, date, ordinalDate, weekDate;
```

---

## Esoteric / dialect notes

1. **Declarative clause order** — write clauses in whatever order is readable.
2. **Optional `SELECT` / `FROM`** — `FROM test LIMIT 5` and `SELECT 1 + 1` are both fine.
3. **Star in functions** — `LEFT(*, 2)` or `LISTAGG(*)` expands across columns / values.
4. **Star with sequence** — `DATE_ADD('2023-10-31', *)` from `SEQUENCE` applies the function to each `value`.
5. **Index hints** — `WHERE INDEX('test__name.index') = '…'` / `UNIQUE('…')` / `PK(id)`.
6. **Comments** — MySQL-style `/* … */` block comments are skipped by the parser. Line `--` comments are used in scripts but are not a first-class SQL comment token in all contexts; prefer `/* */` inside queries.
7. **No** `DISTINCT`, `UNION`, `HAVING`, `RIGHT JOIN`, `FULL JOIN`, or free-standing `OR` in `WHERE` (use `IN` instead).

---

## Quick examples

```sql
-- Projection + limit
SELECT name, score FROM test FETCH FIRST 5 ROWS ONLY;

-- Arbitrary clause order + calendar join
FROM test, CALENDAR ON date = birth_date
WHERE name LIKE 'Walter M%'
SELECT name, birth_date, yearday
FETCH FIRST 5 ROWS ONLY;

-- Concat and arithmetic
FROM suits, ranks
WHERE value > 10
ORDER BY name
SELECT ranks.name || ' of ' || suits.name AS cards;

-- Grouping + filter
FROM test
SELECT
  COUNT(*) AS all,
  COUNT(*) FILTER (WHERE id % 2 = 0) AS even,
  COUNT(*) FILTER (WHERE id % 2 = 1) AS odd;

-- CTE + VALUES
WITH v AS (VALUES (1),(2),(3))
FROM v AS t(n) WHERE n > 1;
```

See also `README.md` for CLI flags and output formats, and `test/test-cases.sql` for a large set of dialect examples.
