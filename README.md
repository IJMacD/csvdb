# csvdb

An esoteric toy command line app for processing CSV using a custom SQL dialect.

## command line interface

    Usage:
            csvdb <options> "<query>"
            csvdb <options> -f file.sql
            csvdb <options> -f - (expects SQL on stdin)
            csvdb "CREATE [UNIQUE] INDEX [<index_file>] ON <file> (<field>)"
            csvdb "CREATE TABLE <file> AS <query>"
            csvdb "INSERT INTO <file> <query>"
            csvdb "CREATE VIEW <file> AS <query>"
            csvdb -h|--help

    Where <query> is one of:
            SELECT <fields, ...> FROM <file> [JOIN <file> [ON ...]] [WHERE] [ORDER BY] [OFFSET <n>] [FETCH (FIRST|NEXT) <m> ROWS]
            SELECT <fields, ...> FROM (<query>) ...
            VALUES (value, ...), ...
            TABLE <file>
            WITH <name> AS (<query>) FROM <name> ...

            <file> can be a CSV file, which behaves as a table; or a SQL file, which behaves as a view.
            Filetype is determined from the filename extension.
            If an exact filename match cannot be found, csvdb will automatically append '.csv' and then '.sql'
            and attempt to open the file as either a table or a view respectively.
            If <file> is the string 'stdin' then an attempt will be made to read the table data from stdin.

    Options:
            [-h|--help]
            [-f (<filename.sql>|-)] (read SQL from file, '-' for stdin)
            [-E|--explain]
            [-H|--headers] (default)
            [-N|--no-headers]
            [(-F |--format=)(table|tsv|csv|html|json|json_array|sql|sql_values|xml|record)]
            [(-o |--output=)<filename>]
            [--stats] (write timing data to 'stats.csv')

## Features

* Multiple output formats
* Efficient query planner
* Supports indexes
* Builtin calendar table
* `SELECT` clause is optional (defaults to `SELECT *`)
* `FROM` clause is optional (defaults to `FROM stdin`)
* Supports many standard SQL features such as: subqueries, views, `TABLE` clause, `VALUES` clause, CTEs
* Basic table creation, insertion, and temp tables
* Can process multiple queries separated by `;`
* Includes basic REPL
* Includes simple CGI server

## Examples

See more examples of the SQL dialect in the `test/test-cases.sql` file

```sql
-- Basic tests
SELECT name, score FROM test FETCH FIRST ROW ONLY;
SELECT name, score FROM test OFFSET 2 ROWS FETCH NEXT 5 ROWS ONLY;
-- Create indexes
CREATE INDEX ON test (name, birth_date);
CREATE UNIQUE INDEX ON test (birth_date);
-- Test indexes
SELECT name FROM test ORDER BY name FETCH FIRST 5 ROWS ONLY;
-- Test EXTRACT
SELECT birth_date, EXTRACT(YEAR FROM birth_date), EXTRACT(MONTH FROM birth_date), EXTRACT(DAY FROM birth_date), EXTRACT(YEARDAY FROM birth_date) FROM test FETCH FIRST ROW ONLY;
-- Test Functions
SELECT RANDOM() AS "Your lucky number";
SELECT LEFT('Hello there', 4), RIGHT('shampoo', 3), TO_HEX(66), CHR(128169);
-- Test COUNT(*)
SELECT COUNT(*) FROM test WHERE name = 'Walter KELLY'; -- Uses index
-- Test Join to CALENDAR
FROM test, CALENDAR ON date = birth_date WHERE name LIKE 'Walter M%' SELECT name, birth_date, yearday FETCH FIRST 5 ROWS ONLY;
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
-- Test Subqueries
FROM (FROM SEQUENCE OFFSET 5 ROWS LIMIT 10) AS a, (FROM SEQUENCE LIMIT 2) AS b SELECT b.value, a.value;
-- Test VALUES clause
VALUES ('a',1),('b',2),('c',3);
-- Test CTEs
WITH r1 AS (FROM ranks WHERE value < 8 SELECT name, symbol) FROM r1 ORDER BY name;
```
