# csvdb Syntax Guide

csvdb uses a custom SQL dialect for querying CSV (and related) files. Clauses are declarative and may appear in any order. `SELECT` defaults to `*`, and `FROM` defaults to `stdin`.

Multiple statements may be separated by `;`.

---

## Comments

```sql
-- Line comment to end of line

/* Block comment (MySQL dump style) */
```

`--` comments are skipped wherever whitespace is allowed. `/* … */` comments are recognised between clauses.

---

## Statement overview

| Kind | Form |
|------|------|
| Query | `[EXPLAIN] [WITH …] SELECT [DISTINCT …] … FROM … WHERE … GROUP BY … HAVING … ORDER BY … OFFSET … FETCH/LIMIT …` |
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
SELECT [DISTINCT [ON (<expr> [, …])]]
    <expr> [FILTER (WHERE <predicate>)] [AS alias] [, …]
FROM <source> [[AS] alias [(col_alias, …)]]
    [, <source> …]
    | [INNER|LEFT] JOIN <source> [ON <predicate> | USING <expr>]
WHERE <predicate> [{AND|OR} <predicate> …]
GROUP BY <expr> [, …]
HAVING <predicate> [{AND|OR} <predicate> …]
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
SELECT LEFT(*, 2) AS left_ FROM test LIMIT 5;   -- one result column per table column
SELECT COUNT(*) FILTER (WHERE id % 2 = 0) AS even FROM test;
SELECT DISTINCT name FROM suits ORDER BY name;
SELECT DISTINCT ON (key) key, label FROM dupes ORDER BY key ASC, label ASC;
```

- `*` expands to all columns of the involved tables.
- `table.*` expands one table’s columns.
- Column aliases: `AS alias` or (for a single double-quoted identifier) the quoted name.
- Aggregate expressions may take `FILTER (WHERE <predicate>)`.
- `SELECT DISTINCT` removes duplicate result rows (not combined with `GROUP BY` / aggregates).
- `SELECT DISTINCT ON (exprs)` keeps the first row of each peer group after sorting; pair with `ORDER BY` that starts with the same expressions.

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
- Join conditions: `ON <predicate>` (`AND`/`OR`, same precedence as `WHERE`) or `USING <expr>` (equality on that expression across the joined tables).

### `WHERE`

```sql
WHERE score = 42
WHERE name LIKE 'Walter M%'
WHERE birth_date BETWEEN CURRENT_DATE AND CURRENT_DATE + 3
WHERE birth_date BETWEEN CURRENT_DATE ANDX CURRENT_DATE + 3
WHERE name IN ('spades','hearts')
WHERE name IS NULL
WHERE name IS NOT NULL
WHERE a = 1 AND b > 2
WHERE name = 'spades' OR name = 'hearts'
WHERE value = 1 OR value = 13 AND name = 'Nope'   -- AND before OR → value = 1
WHERE value = 1 AND name = 'Ace' OR value = 13    -- (1 AND Ace) OR 13
WHERE (value = 1 OR value = 13) AND name != 'Nope'  -- parens override precedence
```

Predicates may be joined with `AND` and `OR`. **`AND` binds tighter than `OR`** (standard SQL). Parentheses may regroup boolean expressions, including nested groups, in `WHERE`, `JOIN … ON`, `HAVING`, and `FILTER (WHERE …)`.

### `GROUP BY` / `HAVING`

```sql
FROM test WHERE name LIKE 'Adam WE%'
GROUP BY name
HAVING COUNT(*) > 1
SELECT name, COUNT(*);

SELECT COUNT(*) FROM suits HAVING COUNT(*) = 4;   -- HAVING alone implies one group
FROM ranks GROUP BY name HAVING name = 'Ace' OR name = 'King' SELECT name;
```

`HAVING` filters groups after aggregation. Predicates use the same shapes and `AND`/`OR` precedence as `WHERE`.

### `ORDER BY` / limits

```sql
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
- Indexes write sidecar files used by the planner (see [Indexes](#indexes)).

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

## Indexes

Indexes speed up equality, range, and prefix-`LIKE` filters, as well as ordered scans and some joins. The planner looks for them automatically; you can also force a named index.

### Creating indexes

```sql
CREATE INDEX ON test (name, birth_date);     -- writes test__name.index.csv
CREATE UNIQUE INDEX ON test (birth_date);  -- writes test__birth_date.unique.csv
CREATE INDEX my_idx ON test (name);        -- writes my_idx.index.csv
```

- Auto-named indexes use `<table>__<first_column>.{index|unique}.csv` (`.csv` extension on the table name is stripped).
- Unique indexes may only cover a single column.
- Multi-column indexes are sorted by all listed columns; seeks always match on the **first** column of the index. Trailing key columns can still satisfy `ORDER BY` and residual filters without a separate sort.
- Every index file ends with a **trailing base-table pointer column**: the zero-based row index in the source table. Seeks match on the first column, then read that **last** column to fetch (or cover) source rows. `CREATE INDEX` names it `rowid` by convention, but the engine identifies it by **position** (last column), not by name. Example shapes: `name,birth_date,rowid` or `name,ptr`.

### Automatic use

For a bare column predicate on the first table (e.g. `WHERE name = '…'`), the planner looks for:

1. Beside the table: `<table_stem>__<column>.unique.csv`, then `.index.csv`
   (`table_stem` is the table path with a trailing `.csv` stripped, so
   `data/test.csv` → `data/test__name.index.csv`)
2. `index_write_dir` (if set), then each `index_dirs` entry from
   `~/.config/csvdb/config` (or `CSVDB_CONFIG`), using only the table basename:
   `<dir>/<basename>__<column>.{unique,index}.csv`

Sidecar paths win over config dirs. Explicit `INDEX('file')` / `UNIQUE('file')`
try the given path (and `.csv`), then the same names under those dirs.
`CREATE INDEX` without an explicit path writes under `index_write_dir` when set
(otherwise next to the table).

If found, equality can become an index unique/seek plan; ranges and trailing-`%` `LIKE` patterns can use index range seeks. Selecting only indexed columns may use a covering index seek.

### Config

See `README.md` for the optional `~/.config/csvdb/config` file (`[output]` TTY
defaults, `[paths]` `data_dirs` / `index_dirs` / `index_write_dir`, and
`[defaults] stats`).

### Index hints

```sql
FROM test WHERE INDEX('test__name.index') = 'Claude MILLS';
FROM test WHERE UNIQUE('test__birth_date.unique') >= '0990-03-26' LIMIT 2;
FROM suits WHERE UNIQUE('suits_by_name.unique') = 'hearts';  -- last col need not be named rowid
FROM test WHERE PK(id) = 1000;
```

- `INDEX('file')` / `UNIQUE('file')` — open that index file (`.csv` optional). Matching uses the **first column** of the index. The file **must** end with a column whose values are the corresponding row indices in the source table; after a seek, those last-column values are what get read from the table. The header name of that column does not matter. Hand-built index CSVs need the same layout (`key…,<ptr>`, sorted on the first column).
- `PK(column)` — treat the table itself as a primary-key index on `column`. The column values must be **unique** and the **source file must already be sorted ascending** on that column (binary search on the table file; no sidecar index is required). Here the table row position *is* the rowid — there is no separate pointer column.

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
| `INTERVAL n UNIT` | Fixed-length interval as an integer (see [Intervals](#intervals)) |

### Special fields

| Name | Meaning |
|------|---------|
| `*` / `table.*` | All columns |
| `rowid` / `table.rowid` | Zero-based row index in the source |
| `ROW_NUMBER()` | One-based output row number |

### Operators

**Arithmetic / string**

| Op | Meaning |
|----|---------|
| `+` `-` `*` `/` `%` | Arithmetic (also date/time arithmetic; see below) |
| `\|\|` | String concatenation |

**Comparison / boolean** (in `WHERE` / `ON` / `HAVING` / `FILTER`)

| Op | Meaning |
|----|---------|
| `=` `IS` | Equal |
| `!=` `IS NOT` | Not equal |
| `<` `<=` `>` `>=` | Ordering |
| `LIKE` | Prefix match; trailing `%` is a wildcard (`name LIKE 'Wa%'`). Exact match if no `%`. |
| `BETWEEN a AND b` | Inclusive both ends: `>= a AND <= b` |
| `BETWEEN a ANDX b` | Exclusive upper bound: `>= a AND < b` |
| `IN (…)` | Membership (`x` equals any listed value) |
| `AND` | Conjunction (binds tighter than `OR`) |
| `OR` | Disjunction |

### Operator precedence

From tightest to loosest:

1. Parentheses — scalar expressions and boolean groups
2. Function calls
3. `*` `/` `%`
4. `+` `-`
5. `||`
6. Comparisons (`=` `!=` `<` `<=` `>` `>=` `LIKE` `BETWEEN` `IN` `IS`)
7. `AND`
8. `OR`

Examples:

```sql
SELECT 5 + 3 * 3;                          -- 14
SELECT (5 + 3) * 3;                        -- 24
WHERE value = 1 OR value = 13 AND name = 'Nope';     -- value = 1 OR (value = 13 AND …)
WHERE (value = 1 OR value = 13) AND name != 'Nope';  -- both values, then AND
```

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

`n` may be a plain integer or an `INTERVAL` constant (e.g. `CURRENT_DATE + INTERVAL 1 WEEK`). Day-scale intervals (`DAY`, `WEEK`) produce day counts; second-scale intervals (`HOUR`, `MINUTE`, `SECOND`) produce seconds — matching the `+`/`-` rules above. Calendar units (`MONTH`, `YEAR`, …) are not valid as bare `+`/`-` integers; use `DATE_ADD` / `DATE_SUB`.

### Date / time formats

| Kind | Examples |
|------|----------|
| Date | `YYYY-MM-DD` (also ordinal, week-date, and several alternate spellings; see parser) |
| Datetime (local / unzoned) | `YYYY-MM-DDTHH:MM:SS` |
| Datetime (UTC) | `YYYY-MM-DDTHH:MM:SSZ` |
| Datetime (offset) | `YYYY-MM-DDTHH:MM:SS+09:00`, `…-05:30` — parsed, converted to UTC for storage |
| Time / duration | `HH:MM:SS` (hours may have 2–4 digits) |

UTC results format with a trailing `Z`. Local-offset results (e.g. `NOW(LOCAL)`, `AT_TIME_ZONE(…, LOCAL)`) format as `±HH:MM` (including `+00:00` when the offset is zero). Untagged datetimes are “local” (no zone); `AT_TIME_ZONE` cannot convert them.

### Intervals

```sql
INTERVAL 1 WEEK          -- → 7   (days; for date + n)
INTERVAL 1 HOUR          -- → 3600 (seconds; for datetime/time + n)
INTERVAL 15 MINUTE       -- → 900
INTERVAL 1 MONTH         -- → kept as "1 MONTH" for DATE_ADD / DATE_SUB
INTERVAL('00:15:00')     -- function form: synonym for CAST(… AS INT) → seconds
```

Units (singular or plural): `MILLENNIUM` / `MILLENNIA`, `CENTURY` / `CENTURIES`, `DECADE` / `DECADES`, `YEAR`, `QUARTER`, `MONTH`, `WEEK`, `DAY`, `HOUR`, `MINUTE`, `SECOND`. Calendar units (`MILLENNIUM` … `MONTH`) are for `DATE_ADD` / `DATE_SUB` / `DATE_TRUNC`; only `WEEK`/`DAY`/`HOUR`/`MINUTE`/`SECOND` fold to integers for `+`/`-`.

### Subqueries in expressions

```sql
SELECT (SELECT LISTAGG(name) FROM suits) AS n FROM ranks LIMIT 2;
SELECT (SELECT SUM(value) FROM ranks) + 10 FROM suits;
```

Scalar subqueries must start with `SELECT` or `FROM` inside `(…)`.

### `*` in function arguments

In the `SELECT` list, a function may take `*` as one argument. That expression is rewritten once per table column (at most one `*` per call). `COUNT(*)` is the usual aggregate and is **not** expanded this way.

```sql
-- Equivalent to SELECT LEFT(id, 2), LEFT(name, 2), LEFT(birth_date, 2), LEFT(score, 2) …
FROM test LIMIT 5 SELECT LEFT(*, 2) AS left_;

-- LISTAGG over every column of the subquery’s single row-set shape
FROM (FROM test LIMIT 5) SELECT LISTAGG(*) AS agg_;
```

Any function that accepts that argument position can be used this way (`LEFT`, `LISTAGG`, `DATE_ADD`, maths functions, …).

On a single-column table such as `SEQUENCE` (`value` only), `*` expands to that one column — so the call is evaluated once per **row**, with `*` replaced by `value`:

```sql
-- Same as: FROM SEQUENCE SELECT DATE_ADD('2023-10-31', value) LIMIT 5
FROM SEQUENCE SELECT DATE_ADD('2023-10-31', *) LIMIT 5;
-- 2023-10-31
-- 2023-11-01
-- 2023-11-02
-- …
```

---

## Function reference

Unless noted, a `NULL` (empty) first argument makes the whole call `NULL`.

### Scalar — no / optional arguments

| Function | Returns |
|----------|---------|
| `RANDOM()` | Random integer in `0 … RAND_MAX` |
| `VERSION()` | csvdb version string |
| `TODAY()` | Current date (`YYYY-MM-DD`) |
| `NOW()` / `NOW(UTC)` / `NOW(Z)` | Current instant as UTC (`…Z`) when the local UTC offset is known; otherwise untagged local wall time |
| `NOW(LOCAL)` | Current local wall time with numeric offset (`…+HH:MM`); untagged if the offset cannot be determined. Quoted `'LOCAL'` is also accepted |
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

Concatenation is via `||` (there is no `CONCAT()` call form).

### Scalar — maths

| Function | Signature | Description |
|----------|-----------|-------------|
| `ADD` | `(a, b, …)` | Sum (also dates/times; same as `+`) |
| `SUB` | `(a, b, …)` | Left-associative difference (same as `-`) |
| `MUL` | `(a, b, …)` | Product |
| `DIV` | `(a, b, …)` | Left-associative division: `DIV(36, 6, 2)` → `(36/6)/2` → `3`. `NULL` on divide-by-zero |
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
| `DATE_TRUNC` | `(unit, date\|datetime)` | Truncate to `MILLENNIUM` / `CENTURY` / `DECADE` / `YEAR` / `QUARTER` / `MONTH` / `WEEK` / `DAY` / `HOUR` / `MINUTE` / `SECOND` (bare unit keyword). Coarser year units use the same floor division as `EXTRACT` (e.g. decade of 2023 → `2020-01-01`) |
| `DATE_ADD` | `(date, days)` | Add days to a date |
| `DATE_ADD` | `(date\|datetime, n, unit)` | Add *n* of `unit`. Month/year overflow clamps to last valid day |
| `DATE_ADD` | `(date\|datetime, INTERVAL n unit)` | Same with an interval operand |
| `DATE_SUB` | *(same shapes as `DATE_ADD`)* | Subtract |
| `DATE_DIFF` | `(date1, date2)` | `date1 − date2` in days |
| `AT_TIME_ZONE` | `(datetime, zone)` | Convert a zoned datetime. `zone` is bare `LOCAL` / `UTC` / `Z`, or a quoted offset `'+09:00'` / `'-05:30'` / `'LOCAL'`. Fails on untagged (local) inputs |
| `MAKE_DATE` | `([y [, m [, d]]])` | Build date (defaults: year 0, month/day 1) |
| `MAKE_TIME` | `([h [, m [, s]]])` | Build time (all parts default to 0) |
| `MAKE_DATETIME` | `([y [, m [, d [, h [, mi [, s]]]]]])` | Build datetime (year defaults 0; month/day default 1; time parts default 0) |
| `INTERVAL` | `(x)` | Synonym for `CAST(x AS INT)` (see also bare `INTERVAL n UNIT` literals) |

Examples:

```sql
SELECT DATE_TRUNC(MONTH, '2023-10-31');                 -- 2023-10-01
SELECT DATE_TRUNC(DECADE, '2023-10-31');                -- 2020-01-01
SELECT DATE_TRUNC(CENTURY, '2023-10-31');               -- 2000-01-01
SELECT DATE_TRUNC(MILLENNIUM, '2023-10-31');            -- 2000-01-01
SELECT DATE_ADD('2023-01-31', 1, MONTH);                -- 2023-02-28
SELECT DATE_ADD('2023-01-31', INTERVAL 1 MONTH);        -- 2023-02-28
SELECT DATE_ADD('2023-10-31T12:00:00', 2, HOUR);        -- 2023-10-31T14:00:00
SELECT AT_TIME_ZONE('2023-10-31T03:00:00Z', '+09:00');  -- 2023-10-31T12:00:00+09:00
SELECT AT_TIME_ZONE('2023-10-31T03:00:00Z', LOCAL);
SELECT NOW(), NOW(LOCAL);
```

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
| `MILLENNIUM` | `⌊year / 1000⌋` (integer division) |
| `CENTURY` | `⌊year / 100⌋` |
| `DECADE` | `⌊year / 10⌋` |
| `QUARTER` | Quarter 1–4 |
| `HOUR` | Hour |
| `MINUTE` | Minute |
| `SECOND` | Second |
| `TIME` | Time string `HH:MM:SS` |
| `JULIAN` | Julian day number |
| `DATE` | Date string |
| `DATETIME` | Datetime string |
| `MONTH_STRING` | `YYYY-MM` |
| `WEEK_STRING` | `YYYY-Www` |
| `YEARDAY_STRING` | `YYYY-DDD` (ordinal date) |

### `CAST`

```sql
CAST(expr AS INT)         -- also AS INTERVAL
CAST(expr AS DURATION)
INTERVAL(expr)            -- same as CAST(expr AS INT)
```

| Target | Accepted `expr` | Result |
|--------|-----------------|--------|
| `INT` / `INTERVAL` | Integer string (via `strtol`, so leading digits of floats like `'3.7'` → `3`) | Integer string |
| `INT` / `INTERVAL` | Duration / time `HH:MM:SS` (2–4 digit hours) | Total seconds |
| `DURATION` | Integer string (seconds) | `HH:MM:SS` |
| `DURATION` | Already a time `HH:MM:SS` | Unchanged |

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
| `value` / `rowid` | Integer from 0 up to 10 000 000 − 1 (same column; supports rowid algebra) |

```sql
FROM SEQUENCE WHERE value < 10;
FROM SEQUENCE OFFSET 5 ROWS LIMIT 10;

-- Arithmetic progression via MOD / % (here 3n+2: 2, 5, 8, …)
FROM SEQUENCE WHERE value % 3 = 2 AND value < 20;

-- Multiple MOD predicates combine when congruent (LCM / CRT)
FROM SEQUENCE WHERE value < 30 AND value % 2 = 0 AND value % 3 = 0;

-- Rowid algebra: value + 1 = 10 → value = 9
FROM SEQUENCE WHERE value + 1 = 10;
```

### `CALENDAR`

Virtual date table (one row per Julian day). Prefer predicates on `date` / `julian` so the planner can bound the scan. Equality on `year` plus `month`, `quarter`, `yearday`, or `month`+`day` further tightens the julian window; `weekday = N` steps by 7 within a bounded range.

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

## Dialect notes

1. **Declarative clause order** — write clauses in whatever order is readable.
2. **Optional `SELECT` / `FROM`** — `FROM test LIMIT 5` and `SELECT 1 + 1` are both fine.
3. **Not supported** — `UNION` / `INTERSECT` / `EXCEPT`, `RIGHT JOIN` / `FULL JOIN`.

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

-- Grouping + filter + HAVING
FROM test
WHERE name LIKE 'Adam WE%'
GROUP BY name
HAVING COUNT(*) > 1
SELECT name, COUNT(*);

-- FILTER clause
FROM test SELECT
  COUNT(*) AS all,
  COUNT(*) FILTER (WHERE id % 2 = 0) AS even,
  COUNT(*) FILTER (WHERE id % 2 = 1) AS odd;

-- CTE + VALUES
WITH v AS (VALUES (1),(2),(3))
FROM v AS t(n) WHERE n > 1;
```

See also `README.md` for CLI flags and output formats, and `test/test-cases.sql` for a large set of dialect examples.
