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
    | [INNER|CROSS|LEFT|RIGHT] [OUTER] JOIN <source> [ON <predicate> | USING <expr>]
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
FROM suits RIGHT JOIN ranks ON …;                -- rewritten as ranks LEFT JOIN suits
FROM suits CROSS JOIN ranks;
FROM suits JOIN ranks USING LENGTH(name);        -- equality on same expression
FROM (FROM SEQUENCE LIMIT 10) AS a;
FROM (VALUES ('a',1),('b',2)) AS t (first, second);
```

- Comma joins are cross joins (predicates go in `WHERE` / `ON`).
- Supported join keywords: `JOIN`, `INNER JOIN`, `CROSS JOIN`, `LEFT [OUTER] JOIN`, `RIGHT [OUTER] JOIN`.
- `CROSS JOIN` is a Cartesian product only — `ON` / `USING` are rejected (use `INNER JOIN`).
- `RIGHT JOIN` is rewritten by swapping the two sides into a `LEFT JOIN` (only when the left side is a single table). `SELECT *` column order follows the rewritten `FROM` (preserved side first).
- `FULL OUTER JOIN` is not supported (would need `UNION`).
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
| `EPHEMERIS` | Builtin solar/lunar ephemeris (one row per Julian day) |
| `EPHEMERIS(lat, lon)` | Same, with observer for rise/set times |
| `PERMUTATIONS(n)` | All orderings of `{0..n-1}` (`n` ≤ 10) |
| `COMBINATIONS(n, k)` | All k-subsets of `{0..n-1}` |
| `CRON('spec')` | Expand a five-field cron pattern over a bounded date range |

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
| `I` | Imaginary unit (`i`) |
| `NULL` | Empty / null value |
| `CURRENT_DATE` | Today’s date (`YYYY-MM-DD`) |
| `CURRENT_TIME` | Current time (`HH:MM:SS`) |
| `CSVDB_VERSION` | Version string (also `VERSION()`) |
| `INTERVAL n UNIT` | Fixed-length interval as an integer (see [Intervals](#intervals)) |

Complex values are duck-typed strings in canonical form `a+bi` / `a-bi` (no spaces; pure imag as `4i` / `i` / `-i`; pure real unchanged). Construct with `COMPLEX(re, im)` or `3 + 4 * I`. There is no lexer-level `3+4i` literal.

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
| `IN (…)` | Membership (`x` equals any listed value, or any row of a subquery) |
| `= ANY (subquery)` / `= SOME (…)` | Same as `IN (subquery)`; other compares (`>`, `<`, …) allowed |
| `> ALL (subquery)` (etc.) | True when the comparison holds for every subquery row |
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

`n` may be a plain integer or an `INTERVAL` constant (e.g. `CURRENT_DATE + INTERVAL 1 WEEK`). Day-scale intervals (`DAY`, `WEEK`) produce day counts; second-scale intervals (`HOUR`, `MINUTE`, `SECOND`) produce seconds — matching the `+`/`-` rules above. Calendar-unit intervals (`MONTH`, `YEAR`, `MILLENNIUM`, …) also work with `+`/`-` (e.g. `CURRENT_DATE + INTERVAL 1 MONTH`), or use `DATE_ADD` / `DATE_SUB` explicitly.

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

Units (singular or plural): `MILLENNIUM` / `MILLENNIA`, `CENTURY` / `CENTURIES`, `DECADE` / `DECADES`, `YEAR`, `QUARTER`, `MONTH`, `WEEK`, `DAY`, `HOUR`, `MINUTE`, `SECOND`. Fixed-length units (`WEEK`/`DAY`/`HOUR`/`MINUTE`/`SECOND`) fold to integers for `+`/`-`. Calendar units (`MILLENNIUM` … `MONTH`) are for calendar arithmetic with `+`/`-`, `DATE_ADD` / `DATE_SUB`, and `DATE_TRUNC`.

### Subqueries in expressions

```sql
SELECT (SELECT LISTAGG(name) FROM suits) AS n FROM ranks LIMIT 2;
SELECT (SELECT SUM(value) FROM ranks) + 10 FROM suits;
```

A subquery inside `(…)` may begin with any keyword that can start a query (`SELECT`, `FROM`, `WHERE`, `VALUES`, `WITH`, `TABLE`, `ORDER`, `LIMIT`, …). Whitespace or newlines between `(` and the keyword are allowed.

### `IN (subquery)` / `ANY` / `ALL`

```sql
FROM suits WHERE name IN (SELECT name FROM suits WHERE name = 'hearts');
FROM ranks WHERE value = ANY (SELECT value FROM ranks WHERE value < 5);
FROM ranks WHERE value > ALL (SELECT value FROM ranks WHERE value < 5);
FROM suits AS s WHERE name = ANY (
  SELECT name FROM suits WHERE LENGTH(name) = LENGTH(s.name)
);
```

- `IN (subquery)` is `= ANY (subquery)`.
- `SOME` is a synonym for `ANY`.
- Uncorrelated subqueries are expanded once (to `OR`/`AND` of compares); correlated ones re-run per outer row.
- Empty subquery: `ANY` is false, `ALL` is true.
- Literal `IN ('a','b')` is unchanged (desugared to `OR` of equalities).

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
| `CHR` / `CHAR` | `(codepoint [, …])` | UTF-8 string from one or more Unicode code points. `NULL` arguments are skipped; all-`NULL` → `NULL`. Invalid code points are skipped |
| `CODEPOINT` | `(s)` | Code point of first UTF-8 character |
| `HEX` | `(s)` | Hex digits of each byte (MySQL-style) |
| `TO_HEX` | `(n)` | Integer as `0x…` hex |
| `W1252` | `(s)` | Repair Windows-1252 mis-decoded as UTF-8 |

Concatenation is via `||` (there is no `CONCAT()` / `STRING()` call form).

### Scalar — maths

| Function | Signature | Description |
|----------|-----------|-------------|
| `ADD` | `(a, b, …)` | Sum (also dates/times/complex; same as `+`) |
| `SUB` | `(a, b, …)` | Left-associative difference (same as `-`) |
| `MUL` | `(a, b, …)` | Product |
| `DIV` | `(a, b, …)` | Left-associative division: `DIV(36, 6, 2)` → `(36/6)/2` → `3`. `NULL` on divide-by-zero |
| `MOD` | `(a, b)` | Remainder (`fmod`); real-only (`NULL` if either arg is complex) |
| `POW` | `(a, b)` | Power (complex-capable) |
| `ABS` | `(x)` | Absolute value (modulus for complex) |
| `SQRT` | `(x)` | Square root; negative reals and complex use the principal value in ℂ |
| `FLOOR` | `(x)` | Floor (real-only) |
| `CEIL` / `CEILING` | `(x)` | Ceiling (real-only) |
| `ROUND` | `(x)` or `(x, digits)` | Round; *digits* may be negative (real-only) |
| `EXP` | `(x)` | *e*<sup>*x*</sup> (complex-capable) |
| `LN` | `(x)` | Natural log; `NULL` only for zero. Negative reals / complex use the principal branch |
| `LOG10` | `(x)` | Base-10 log (same domain rules as `LN`) |
| `PI` | `()` | π (approx. `3.1415926536`) |
| `COMPLEX` | `(re, im)` | Construct a complex value (both args real) |
| `RE` | `(z)` | Real part |
| `IM` | `(z)` | Imaginary part |
| `CONJ` | `(z)` | Complex conjugate |
| `ARG` | `(z)` | Argument (`atan2(im, re)`) in radians |

Any of `+ − * / POW EXP LN LOG10 SQRT ABS` promotes to complex when an operand is complex. `=` / `!=` compare by components; `< > <= >=` on complex values are undefined and evaluate false. `ORDER BY` sorts by real, then imag. `SUM` / `AVG` are componentwise; `MIN` / `MAX` ignore complex inputs.

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
| `LISTAGG` | `(expr [, sep])` | Concatenate non-null values; optional *sep* (default `,`) |
| `STDDEV` / `STDDEV_SAMP` | `(expr)` | Sample standard deviation (`√(Σ(x−μ)²/(n−1))`); `NULL` if fewer than 2 non-null reals |
| `STDDEV_POP` | `(expr)` | Population standard deviation (`√(Σ(x−μ)²/n)`); `NULL` if no non-null reals |
| `VARIANCE` / `VAR_SAMP` | `(expr)` | Sample variance (`Σ(x−μ)²/(n−1)`); `NULL` if fewer than 2 non-null reals |
| `VAR_POP` | `(expr)` | Population variance (`Σ(x−μ)²/n`); `NULL` if no non-null reals |
| `MEDIAN` | `(expr)` | Discrete median of non-null reals: sorted value at index `(n−1)/2` (lower middle when *n* is even). Always a value from the set — never an average of two middles. Equivalent to `PERCENTILE(expr, 0.5)` / `PERCENTILE_DISC(expr, 0.5)`. `NULL` if no non-null reals |
| `MODE` | `(expr)` | Most frequent non-null value using string equality (works for text and numbers). Ties → lexicographically smallest. `NULL` if no non-null values |
| `PERCENTILE` / `PERCENTILE_DISC` | `(expr, p)` | Discrete percentile: sorted value at index `⌈n·p⌉−1` (*p*=0 → first). Always a value from the set. `NULL` if no non-null reals or *p* invalid |
| `PERCENTILE_CONT` | `(expr, p)` | Continuous percentile of non-null reals; *p* in `[0, 1]`. Interpolates at position `(n−1)·p`. `NULL` if no non-null reals or *p* invalid |

`STDDEV` / `VARIANCE` / `MEDIAN` / `PERCENTILE*` ignore complex values (same as `MIN` / `MAX`). `MODE` treats values as strings. Wrong arity is an error (`SUM`/`AVG`/… take one expression; `PERCENTILE*` require two; `LISTAGG` allows an optional separator). All aggregates accept `FILTER (WHERE …)`. Aggregates imply grouping when used without `GROUP BY` (single group over the result).

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

### `EPHEMERIS`

Virtual solar/lunar table (one row per Julian day; same key space as `CALENDAR`). Prefer predicates on `date` / `julian` so the planner can bound the scan. Phase flags mark the civil day on which that phase is crossed (vs the previous day).

| Column | Description |
|--------|-------------|
| `julian` / `rowid` | Julian day number |
| `date` | `YYYY-MM-DD` |
| `solarLongitude` | Apparent solar longitude (degrees) |
| `equationOfTime` | Equation of time (minutes) |
| `solarDeclination` | Solar declination (degrees) |
| `moonPhase` | Lunation fraction (`0` = new … `0.5` = full) |
| `moonIllumination` | Illuminated fraction `0`…`1` |
| `moonAgeDays` | Age in days since new moon |
| `lunarLongitude` | Approximate lunar ecliptic longitude (degrees) |
| `isNewMoon` | `1` if new moon occurs on this day |
| `isFullMoon` | `1` if full moon occurs on this day |
| `isFirstQuarter` | `1` if first quarter occurs on this day |
| `isLastQuarter` | `1` if last quarter occurs on this day |
| `sunRise` / `sunSet` | Local untagged datetime (`…T…`); empty without observer |
| `moonRise` / `moonSet` | Local untagged datetime; empty without observer |
| `isLunarEclipse` / `isSolarEclipse` | Approximate eclipse-day flags (syzygy near a node) |

`EPHEMERIS(lat, lon)` sets the observer (degrees; east longitude positive) used for rise/set. Bare `EPHEMERIS` leaves rise/set empty. Eclipse flags do not require an observer.

```sql
FROM EPHEMERIS WHERE date BETWEEN '2024-01-01' AND '2024-01-31'
  AND isFullMoon = 1
SELECT date, moonIllumination;

FROM EPHEMERIS(51.477, 0.0) WHERE date = '2024-06-21'
SELECT date, sunRise, sunSet;
```

### `PERMUTATIONS(n)` / `COMBINATIONS(n, k)`

Lexicographic generators over `{0,1,…,n−1}`. Wide integer columns only (no comma-joined display string). `PERMUTATIONS` allows `0 ≤ n ≤ 10`; `COMBINATIONS` requires `0 ≤ k ≤ n`, `n ≤ 20`, `k ≤ 10`.

| Table | Columns |
|-------|---------|
| `PERMUTATIONS(n)` | `rank` / `rowid`, `p0` … `p{n-1}` |
| `COMBINATIONS(n, k)` | `rank` / `rowid`, `c0` … `c{k-1}` (sorted ascending) |

```sql
FROM PERMUTATIONS(3) SELECT rank, p0, p1, p2;
FROM COMBINATIONS(5, 2) SELECT rank, c0, c1;
```

### `CRON('spec')`

Expands a five-field cron expression (`minute hour dom month dow`) into occurrence rows. **Requires** `date` / `julian` / `datetime` bounds (or the scan aborts). `datetime` uses the untagged `YYYY-MM-DDTHH:MM:SS` form. `weekday` matches `CALENDAR` (Mon=1 … Sun=7). Dow in the cron spec uses 0-or-7=Sunday, 1=Monday (Vixie-style); when both DOM and DOW are restricted, a day matches if either field matches.

| Column | Description |
|--------|-------------|
| `datetime` | `YYYY-MM-DDTHH:MM:SS` |
| `date` | `YYYY-MM-DD` |
| `julian` | Julian day |
| `hour`, `minute` | Components |
| `weekday` | Mon=1 … Sun=7 |
| `expr` | Echo of the cron pattern |

```sql
FROM CRON('0 9 * * 1-5')
WHERE date BETWEEN '2024-01-01' AND '2024-01-07'
SELECT datetime, weekday;
```

---

## Dialect notes

1. **Declarative clause order** — write clauses in whatever order is readable.
2. **Optional `SELECT` / `FROM`** — `FROM test LIMIT 5` and `SELECT 1 + 1` are both fine.
3. **Not supported** — `UNION` / `INTERSECT` / `EXCEPT`, `FULL OUTER JOIN`.

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
