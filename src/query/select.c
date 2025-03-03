#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include <sys/time.h>
#include <unistd.h>

#include "select.h"
#include "../execute/execute.h"
#include "../evaluate/evaluate.h"
#include "create.h"
#include "token.h"
#include "parse.h"
#include "explain.h"
#include "plan.h"
#include "table.h"
#include "optimise.h"
#include "../db/db.h"
#include "../db/csv-mem.h"
#include "../functions/util.h"
#include "node.h"
#include "../debug.h"
#include "check.h"

#ifdef DEBUG
int query_count = -1;
#endif

struct DB *stdin_db = NULL;

static int information_query(const char *table, FILE *output);

static int populate_tables(struct Query *q);

static int resolveNode(struct Query *query, struct Node *node, enum AliasSearchMode allow_aliases);

static void expandFieldStar(struct Query *query);

static struct Query *makeQuery();

static int find_field(
    struct Query *q,
    const char *text,
    int *table_id,
    int *column_id,
    char *name);

static int process_subquery(
    struct Query *query,
    enum OutputOption options,
    char *output_filename);

static void destroy_query(struct Query *q);

static int wrap_query(
    struct Query *query,
    enum OutputOption inner_options,
    enum OutputOption outer_options,
    FILE *output);

/**
 * returns process exit code; negative for error
 */
int select_query(
    const char *query,
    enum OutputOption output_flags,
    FILE *output,
    const char **end_ptr)
{
    struct Query *q = makeQuery();
    struct timeval stop, start;

    if (output_flags & OUTPUT_OPTION_STATS)
    {
        gettimeofday(&start, NULL);
    }

    if (parseQuery(q, query, end_ptr) < 0)
    {
        return -1;
    }

    // Verbose mode prints out current query
    if (output_flags & OUTPUT_OPTION_VERBOSE)
    {
        size_t query_len = *end_ptr - query;
        fwrite(query, 1, query_len, stderr);
        fprintf(stderr, "\n");
    }

    if (output_flags & OUTPUT_OPTION_STATS)
    {
        gettimeofday(&stop, NULL);

        FILE *fstats = fopen("stats.csv", "a");

        if (fstats)
        {
            fprintf(fstats, "PARSE,%ld\n", dt(stop, start));

            // Will be opened again with a fresh handle in processQuery() and
            // executeQueryPlan()
            fclose(fstats);
        }
    }

    int explain = (q->flags & FLAG_EXPLAIN) || (output_flags & FLAG_EXPLAIN);

    enum OutputOption format = output_flags & OUTPUT_MASK_FORMAT;

    // In order to support different output formats for EXPLAIN we wrap the
    // whole query in a subquery.
    if (format != OUTPUT_FORMAT_COMMA && explain)
    {
        int result = wrap_query(
            q,
            (enum OutputOption)FLAG_EXPLAIN,
            format | (output_flags & OUTPUT_OPTION_HEADERS),
            output);

        destroy_query(q);

        free(q);

        return result;
    }

    // Cannot group and sort in the same query.
    if ((q->flags & FLAG_GROUP) && q->order_count > 0)
    {
#ifdef DEBUG
        if (output_flags & OUTPUT_OPTION_AST)
        {
            debugAST(stdout, q);
            return 0;
        }
#endif

        // We will materialise the GROUP'd query to disk then sort that

        // Make a copy of the struct
        struct Query *q2a = makeQuery();
#ifdef DEBUG
        int id = q2a->id;
#endif
        memcpy(q2a, q, sizeof(*q2a));
#ifdef DEBUG
        q2a->id = id;
#endif
        q2a->order_count = 0;

        struct Table table = {0};

        int result = process_subquery(
            q2a,
            (output_flags & OUTPUT_OPTION_STATS),
            table.name);

        destroy_query(q2a);

        if (result < 0)
        {
            remove(table.name);
            return -1;
        }

        struct Query *q2b = makeQuery();
        q2b->tables = &table;
        q2b->table_count = 1;

        q2b->limit_value = -1;

        memcpy(
            q2b->order_nodes,
            q->order_nodes,
            sizeof(q->order_nodes));

        q2b->order_count = q->order_count;

        result = process_query(q2b, output_flags, output);

        remove(table.name);

        q2b->tables = NULL;

        destroy_query(q2b);

        free(q2a);
        free(q2b);

        //      Why aren't we destroying here?
        // destroy_query(&q);

        return result;
    }

    int result = process_query(q, output_flags, output);

    destroy_query(q);

    free(q);

    return result;
}

/**
 * returns process exit code; negative for error
 */
int process_query(
    struct Query *q,
    enum OutputOption output_flags,
    FILE *output)
{
    int result;
    int auto_stdin = 0;

    // Explain can be specified on the command line so copy that value in just
    // in case.
    if (output_flags & FLAG_EXPLAIN)
    {
        q->flags |= FLAG_EXPLAIN;
    }

    // If there are no tables specified then we default to "FROM stdin".
    // However, the special case of all constant columns means we just want a
    // dummy row query.
    if (q->table_count == 0)
    {
        if (!isatty(fileno(stdin)))
        {
            // If there's something on stdin then default to 'FROM stdin'
            q->tables = calloc(1, sizeof(struct Table));
            q->table_count = 1;
            strcpy(q->tables[0].name, "stdin");
            strcpy(q->tables[0].alias, "stdin");
            auto_stdin = 1;
        }
        // else we hope its a "dummy row" query - we'll find out later
    }
    else if (strcmp(q->tables[0].name, "INFORMATION") == 0)
    {
        if (q->predicate_count < 1)
        {
            return -1;
        }

        q->tables[0].db = NULL;

        result = information_query(
            q->predicate_nodes[0].children[1].field.text,
            output);

        return result;
    }

    /*************************
     * Begin Query processing
     *************************/
    FILE *fstats = NULL;
    struct timeval stop, start;

    if (output_flags & OUTPUT_OPTION_STATS)
    {
        fstats = fopen("stats.csv", "a");

        gettimeofday(&start, NULL);
    }

    if (q->column_count == 0)
    {
        // Allow SELECT to be optional and default to SELECT *
        struct Node *col = allocateColumnNode(q);
        col->function = FUNC_UNITY;
        col->field.index = FIELD_STAR;
        col->field.table_id = -1;
    }

#ifdef DEBUG
    if (debug_verbosity >= 2)
    {
        debugLog(q, "AST");
    }
#endif

    // Populate Tables
    // (including JOIN predicate columns)
    result = populate_tables(q);
    if (result < 0)
    {
        fprintf(stderr, "Unable to populate tables\n");
        return result;
    }

    // Now that tables have been populated we can check if stdin was empty or
    // not. If it was empty then rollback to a tableless query.
    if (auto_stdin && q->tables[0].db->field_count == 0)
    {
        closeDB(q->tables[0].db);
        free(q->tables[0].db);
        q->table_count = 0;
    }

#ifdef DEBUG
    // Pre-optimised FROM nodes
    if (debug_verbosity >= 4)
    {
        fprintf(stderr, "    FROM\n");
        debugFrom(q);
    }
#endif

    for (int i = 1; i < q->table_count; i++)
    {
        if (q->tables[i].join.function != OPERATOR_ALWAYS)
        {
            optimiseCollapseConstantNode(&q->tables[i].join);

            optimiseRowidAlgebra(&q->tables[i].join);

            optimiseOnToWhere(i, &q->tables[i].join, q);
        }
    }

#ifdef DEBUG
    // Pre-optimised SELECT nodes
    if (debug_verbosity >= 4)
    {
        fprintf(stderr, "    SELECT\n");
        debugNodes(q->column_nodes, q->column_count);
    }
#endif

    // Populate SELECT fields first so that aliases can be used in WHERE or
    // ORDER BY clauses
    for (int i = 0; i < q->column_count; i++)
    {
        result = resolveNode(q, &q->column_nodes[i], NO_ALIASES);
        if (result < 0)
        {
            return result;
        }

        optimiseCollapseConstantNode(&q->column_nodes[i]);
    }

    expandFieldStar(q);

#ifdef DEBUG
    // Pre-optimised WHERE nodes
    if (debug_verbosity >= 4 && q->predicate_count > 0)
    {
        fprintf(stderr, "    WHERE\n");
        debugNodes(q->predicate_nodes, q->predicate_count);
    }
#endif

    optimiseFlattenANDPredicates(q);

    // Populate any fields in WHERE clause
    for (int i = 0; i < q->predicate_count; i++)
    {
        struct Node *predicate = &q->predicate_nodes[i];

        for (int j = 0; j < predicate->child_count; j++)
        {
            result = resolveNode(q, &predicate->children[j], ALIASES_FIRST);

            if (result < 0)
            {
                fprintf(stderr, "Unable to resolve WHERE node (%d,%d)\n", i, j);
                return result;
            }
        }

        optimiseRowidAlgebra(predicate);

        optimiseCollapseConstantNode(predicate);

        optimiseUniqueOr(predicate);
    }

    optimiseWhereToOn(q);

#ifdef DEBUG
    // Post-optimised
    if (debug_verbosity >= 2)
    {
        fprintf(stderr, "    FROM\n");
        debugFrom(q);
    }
#endif

#ifdef DEBUG
    // Post-optimised
    if (debug_verbosity >= 2 && q->predicate_count > 0)
    {
        fprintf(stderr, "    WHERE\n");
        debugNodes(q->predicate_nodes, q->predicate_count);
    }
#endif

    // Populate ORDER BY columns
    for (int i = 0; i < q->order_count; i++)
    {
        result = resolveNode(q, &q->order_nodes[i], ALIASES_FIRST);
        if (result < 0)
        {
            fprintf(stderr, "Unable to resolve ORDER BY node %i\n", i);
            return result;
        }
    }

    // Populate GROUP BY columns
    for (int i = 0; i < q->group_count; i++)
    {
        result = resolveNode(q, &q->group_nodes[i], ALIASES_LAST);
        if (result < 0)
        {
            fprintf(stderr, "Unable to resolve GROUP BY node %i\n", i);
            return result;
        }
    }

#ifdef DEBUG
    // Post-optimised
    if (debug_verbosity >= 2)
    {
        fprintf(stderr, "    SELECT\n");
        debugNodes(q->column_nodes, q->column_count);
    }
#endif

    if (output_flags & OUTPUT_OPTION_STATS)
    {
        gettimeofday(&stop, NULL);

        if (fstats)
        {
            fprintf(fstats, "LOAD TABLES,%ld\n", dt(stop, start));
        }

        start = stop;
    }

#ifdef DEBUG
    if (output_flags & OUTPUT_OPTION_AST)
    {
        debugAST(stdout, q);
        return 0;
    }
#endif

    /**********************
     * Make Plan
     **********************/
    struct Plan plan;

    makePlan(q, &plan);

    if (output_flags & OUTPUT_OPTION_STATS)
    {
        gettimeofday(&stop, NULL);

        if (fstats)
        {
            fprintf(fstats, "MAKE PLAN,%ld\n", dt(stop, start));

            // Will be opened again with a fresh handle in executeQueryPlan()
            fclose(fstats);
        }
    }

    if (q->flags & FLAG_EXPLAIN)
    {
        result = explain_select_query(q->tables, &plan, output_flags, output);
        destroyPlan(&plan);
        return result;
    }

    checkPlan(q, &plan);

    result = executeQueryPlan(
        q,
        &plan,
        output_flags,
        output);

    destroyPlan(&plan);

    return result;
}

/**
 * @brief Execute the query, write the results to a temp file and write the tmp
 * filename to the char pointer provieded as `filename`.
 *
 * @param query Query string to process.
 * @param filename Char buffer to write temp filename to. Must be at least 32
 * chars. Consumer is responsible for deleting file from disk when no longer
 * needed.
 * @param end_ptr can be NULL if you don't care about the end of the parsed
 * query
 * @return int 0 for success; -1 for failure
 */
int select_subquery_file(
    const char *query,
    char *filename,
    const char **end_ptr)
{
    struct Query *q = makeQuery();

    int result = parseQuery(q, query, end_ptr);
    if (result < 0)
    {
        return -1;
    }

// DEBUG builds print out current query
#ifdef DEBUG
    if (end_ptr != NULL)
    {
        size_t query_len = *end_ptr - query;
        fwrite(query, 1, query_len, stderr);
        fprintf(stderr, "\n");
    }
    else
    {
        fprintf(stderr, "%s\n", query);
    }
#endif

    result = process_subquery(q, 0, filename);

    destroy_query(q);

    free(q);

    return result;
}

/**
 * @brief Execute the query, write the results to a CSV_MEM database.
 *
 * @param query Query string to process.
 * @param db A DB that will be initialised by CSV_MEM
 * @param end_ptr can be NULL if you don't care about the end of the parsed
 * query
 * @return int 0 for success; -1 for failure
 */
int select_subquery_mem(
    const char *query,
    struct DB *db,
    const char **end_ptr)
{
    struct Query *q = makeQuery();

    int result = parseQuery(q, query, end_ptr);
    if (result < 0)
    {
        return -1;
    }

// DEBUG builds print out current query
#ifdef DEBUG
    if (debug_verbosity >= 1)
    {
        fprintf(stderr, "Subquery: ");
        if (end_ptr != NULL)
        {
            size_t query_len = *end_ptr - query;
            fwrite(query, 1, query_len, stderr);
            fprintf(stderr, "\n");
        }
        else
        {
            fprintf(stderr, "%s\n", query);
        }
    }
#endif

    result = csvMem_fromQuery(db, q);

    destroy_query(q);

    free(q);

    return result;
}

/**
 * @brief Execute the query, write the results to a temp file and write the tmp
 * filename to the char pointer provieded as `filename`.
 *
 * @param query Parsed query to process and execute.
 * @param filename Char buffer to write temp filename to. Must be at least 32
 * chars. Consumer is responsible for deleting file from disk when no longer
 * needed.
 * @return int 0 for success; -1 for failure
 */
static int process_subquery(
    struct Query *query,
    enum OutputOption options,
    char *output_filename)
{
    sprintf(output_filename, "/tmp/csvdb.%d-%d.csv", getpid(), rand());
    FILE *f = fopen(output_filename, "w");

    int result = process_query(
        query,
        options | OUTPUT_OPTION_HEADERS | OUTPUT_FORMAT_COMMA,
        f);

    fclose(f);

    if (result < 0)
    {
        return -1;
    }

    return 0;
}

static int information_query(const char *table, FILE *output)
{
    struct DB db;

    char *resolved = NULL;

    if (openDB(&db, table, &resolved) != 0)
    {
        fprintf(stderr, "File not found: '%s'\n", table);
        return -1;
    }

    fprintf(output, "Table:\t%s\n", table);
    if (resolved != NULL)
    {
        fprintf(output, "Path:\t%s\n", resolved);
        free(resolved);
    }
    fprintf(output, "Fields:\t%d\n", db.field_count);
    fprintf(output, "Records:\t%d\n", getRecordCount(&db));

    fprintf(output, "\n");

    fprintf(output, "field\tindex\n");
    fprintf(output, "-----\t-----\n");

    for (int i = 0; i < db.field_count; i++)
    {
        int have_index = 0;

        // if (findIndex(NULL, table, getFieldName(&db, i), INDEX_ANY, NULL)) {
        //     have_index = 1;
        // }

        fprintf(
            output,
            "%s\t%c\n",
            getFieldName(&db, i),
            have_index ? 'Y' : 'N');
    }

    closeDB(&db);

    return 0;
}

static void destroy_query(struct Query *query)
{
    if (query->predicate_nodes != NULL)
    {
        free(query->predicate_nodes);
        query->predicate_nodes = NULL;
    }

    if (query->tables != NULL)
    {
        for (int i = 0; i < query->table_count; i++)
        {
            if (query->tables[i].db == stdin_db)
            {
                // Don't close special DB created from stdin
                continue;
            }

            closeDB(query->tables[i].db);

            if (query->tables[i].db != NULL)
            {
                // DB might be duplicated. Check if the pointer is shared by any
                // other tables first.
                void *ptr = query->tables[i].db;
                for (int j = i; j < query->table_count; j++)
                {
                    if (query->tables[j].db == ptr)
                    {
                        query->tables[j].db = NULL;
                    }
                }

                free(ptr);
            }
        }

        free(query->tables);
        query->tables = NULL;
    }

    for (int i = 0; i < query->column_count; i++)
    {
        freeNode((struct Node *)&query->column_nodes[i]);
    }

    for (int i = 0; i < query->order_count; i++)
    {
        freeNode(&query->order_nodes[i]);
    }

    for (int i = 0; i < query->group_count; i++)
    {
        freeNode(&query->group_nodes[i]);
    }
}

static int populate_tables(struct Query *q)
{

    for (int i = 0; i < q->table_count; i++)
    {
        struct Table *table = &q->tables[i];

        int found = 0;

        // Perform a sanity check first

        if (table->db != NULL && table->db != DB_SUBQUERY)
        {
            // DB has already been opened for us. This is most probably a
            // VALUES "subquery". There's nothing we need to do.
            found = 1;
        }

        // stdin can only be consumed once per process, check to see if we're
        // trying to open it now, and whether or not it's already been opened
        // for us.
        if (strcmp(table->name, "stdin") == 0 && stdin_db != NULL)
        {
            // It's already open
            table->db = stdin_db;

            found = 1;
        }

        if (found == 0)
        {
            // Try to reuse existing open table next
            for (int j = 0; j < i; j++)
            {
                if (strcmp(q->tables[j].name, table->name) == 0)
                {
                    // Copy pointer
                    table->db = q->tables[j].db;

                    found = 1;
                    break;
                }
            }
        }

        // Handle special kinds of table, info provided by parser

        // Check for subquery first
        if (found == 0 && table->db == DB_SUBQUERY)
        {
            table->db = calloc(1, sizeof(*table->db));

            int result = select_subquery_mem(table->name, table->db, NULL);
            if (result < 0)
            {
                return -1;
            }

            /// TODO: do we want to change table->name to "SUBQUERY" or keep a
            /// record of what the query was?
            // Counter point: subquery can have commas which messes with EXPLAIN
            // output.
            strcpy(table->name, "SUBQUERY");
            // Counter counter point: what if we want to re-evaluate later? e.g.
            // implement correlated subqueries

            found = 1;
        }

        // Must be a regular table
        // Not a special table and not already open
        if (found == 0)
        {
            table->db = calloc(1, sizeof(*table->db));

            // Ability to retrieve real filename for debugging
            char *resolved = NULL;

            if (openDB(table->db, table->name, &resolved) != 0)
            {
                fprintf(stderr, "Unable to use file: '%s'\n", table->name);
                return -1;
            }

            if (strcmp(table->name, "stdin") == 0)
            {
                // If we're going to access stdin more than once per process,
                // then we need to keep a reference to the DB now that it's
                // populated.
                stdin_db = table->db;
            }

            if (resolved != NULL)
            {
                strcpy(table->name, resolved);
                free(resolved);
            }
        }

        int result;

        if (
            table->join.function != OPERATOR_ALWAYS && table->join.function != FUNC_UNITY)
        {
            result = resolveNode(q, &table->join.children[0], NO_ALIASES);
            if (result < 0)
            {
                return result;
            }
            result = resolveNode(q, &table->join.children[1], NO_ALIASES);
            if (result < 0)
            {
                return result;
            }
        }
    }

    return 0;
}

/**
 * @brief given a text string (e.g. name, table.name, *, rowid, table.*,
 * table.rowid) this function will search for a matching field in all tables in
 * the query and write the table_id and index into the `table_id` and
 * `column_id` pointers. If the `name` pointer is not NULL and the input text
 * has a prefix then the suffix (without prefix or dot) will be written to
 * `name`.
 *
 * @param q
 * @param text IN
 * @param table_id OUT
 * @param column_id OUT
 * @param name OUT
 * @return int
 */
static int find_field(
    struct Query *q,
    const char *text,
    int *table_id,
    int *column_id,
    char *name)
{

    int dot_index = str_find_index(text, '.');

    if (*table_id != -1)
    {
        // table_id has already been filled in for us as a hint,
        // we'll search for the column on that table

        if (text[dot_index + 1] == '*')
        {
            *column_id = FIELD_STAR;
        }
        else if (strcmp(text + dot_index + 1, "rowid") == 0)
        {
            *column_id = FIELD_ROW_INDEX;
        }
        else
        {
            *column_id = getTableFieldIndex(&q->tables[*table_id], text + dot_index + 1);
        }

        return 1;
    }

    if (dot_index >= 0)
    {
        char value[MAX_FIELD_LENGTH];

        strncpy(value, text, dot_index);
        value[dot_index] = '\0';

        for (int i = 0; i < q->table_count; i++)
        {
            if (strcmp(q->tables[i].name, value) == 0 ||
                strcmp(q->tables[i].alias, value) == 0)
            {
                *table_id = i;

                if (text[dot_index + 1] == '*')
                {
                    *column_id = FIELD_STAR;
                }
                else if (strcmp(text + dot_index + 1, "rowid") == 0)
                {
                    *column_id = FIELD_ROW_INDEX;
                }
                else
                {
                    *column_id = getTableFieldIndex(&q->tables[i], text + dot_index + 1);

                    if (*column_id != -1 && name != NULL)
                    {
                        // could use getFieldName() but probably not necessary
                        strcpy_overlap(name, text + dot_index + 1);
                    }
                }

                return 1;
            }
        }
    }
    else
    {
        if (strcmp(text, "rowid") == 0)
        {
            // Default to first table
            *table_id = 0;
            *column_id = FIELD_ROW_INDEX;

            return 1;
        }

        for (int i = 0; i < q->table_count; i++)
        {
            *column_id = getTableFieldIndex(&q->tables[i], text);

            if (*column_id != FIELD_UNKNOWN)
            {
                *table_id = i;

                return 1;
            }
        }
    }

    // Couldn't find column
    *column_id = FIELD_UNKNOWN;

    return 0;
}

/**
 * Will resolve a field name to a table_id and column_id.
 * Will also evaluate constant values.
 * Safe to call multiple times on same column.
 *
 * @param query
 * @param node
 * @param allow_aliases 0 = do not search aliases; 1 = search aliases before
 * tables; 2 = search aliases after tables
 * @returns 0 on success
 */
static int resolveNode(struct Query *query, struct Node *node, enum AliasSearchMode allow_aliases)
{
    if (node->filter != NULL)
    {
        int result = resolveNode(query, node->filter, allow_aliases);
        if (result < 0)
            return -1;
    }

    if (node->child_count > 0)
    {
        for (int i = 0; i < node->child_count; i++)
        {
            int result = resolveNode(query, &node->children[i], allow_aliases);
            if (result < 0)
                return -1;
        }

        // If there are children then it means there won't be any fields on this
        // node to resolve.
        return 0;
    }

    // Deal with constants here (strings/numbers)
    if (node->field.index == FIELD_CONSTANT)
    {
        evaluateConstantField(node->field.text, &node->field);
        return 0;
    }

    // If field has already been resolved there's nothing for us to do.
    // If field text is empty there's nothing we can do.
    if (node->field.index != FIELD_UNKNOWN || node->field.text[0] == '\0')
    {
        return 0;
    }

    int result = evaluatePossibleNamedConstantField(node->field.text, &node->field);
    if (result >= 0)
    {
        node->field.index = FIELD_CONSTANT;
        return result;
    }

    // Check for aliases first
    if (
        allow_aliases == 1 && node->function == FUNC_UNITY && node->field.index == FIELD_UNKNOWN && node->field.text[0] != '\0')
    {
        for (int i = 0; i < query->column_count; i++)
        {
            if (
                node != (struct Node *)&query->column_nodes[i] && strcmp(node->field.text, query->column_nodes[i].alias) == 0)
            {
                copyNodeTree(node, (struct Node *)&query->column_nodes[i]);
                return 0;
            }
        }
    }

    // Search all tables for field
    if (find_field(
            query,
            node->field.text,
            &node->field.table_id,
            &node->field.index,
            node->field.text))
    {
        return 0;
    }

    // Check for aliases last
    if (
        allow_aliases == 2 && node->function == FUNC_UNITY && node->field.index == FIELD_UNKNOWN && node->field.text[0] != '\0')
    {
        for (int i = 0; i < query->column_count; i++)
        {
            if (
                node != (struct Node *)&query->column_nodes[i] && strcmp(node->field.text, query->column_nodes[i].alias) == 0)
            {
                copyNodeTree(node, (struct Node *)&query->column_nodes[i]);
                return 0;
            }
        }
    }

    // We couldn't resolve the node/field. If there was no table specified, then
    // that's probably why. Otherwise the field couldn't be found in any of the
    // tables that we do have.

    if (query->table_count == 0)
    {
        fprintf(stderr, "No tables specified\n");
        return -1;
    }

    fprintf(stderr, "Unable to find column '%s'\n", node->field.text);
    return -1;
}

/**
 * @brief To support all combinations of output formats it's sometimes necessary
 * to split the options and wrap one query inside of another. This function is
 * just a wrapper around other functions to achieve that easily.
 *
 * @param query
 * @param inner_options
 * @param outer_options
 * @param output
 * @return int 0 on success
 */
static int wrap_query(
    struct Query *query,
    enum OutputOption inner_options,
    enum OutputOption outer_options,
    FILE *output)
{
    struct Query *q2 = makeQuery();
    struct Table table = {0};

    int result = process_subquery(query, inner_options, table.name);
    if (result < 0)
    {
        return result;
    }

    q2->table_count = 1;
    q2->tables = &table;
    q2->limit_value = -1;

    result = process_query(
        q2,
        outer_options,
        output);

    remove(table.name);

    q2->tables = NULL;
    destroy_query(q2);

    free(q2);

    return result;
}

static struct Query *makeQuery()
{
    struct Query *q = calloc(1, sizeof(*q));

#ifdef DEBUG
    q->id = ++query_count;

    debugLog(q, "Start Query");
#endif

    return q;
}

struct Table *allocateTable(struct Query *q)
{
    q->table_count++;

    if (q->table_count == 1)
    {
        q->tables = calloc(q->table_count, sizeof(struct Table));
    }
    else
    {
        void *ptr = realloc(
            q->tables,
            sizeof(struct Table) * q->table_count);

        if (ptr == NULL)
        {
            fprintf(stderr, "Can't allocate memory\n");
            exit(-1);
        }

        q->tables = ptr;

        // Zero out realloc'd space
        memset(
            q->tables + (q->table_count - 1),
            0,
            sizeof(struct Table));
    }

    return &q->tables[q->table_count - 1];
}

struct Node *allocateColumnNode(struct Query *q)
{
    void *mem;

    if (q->column_count == 0)
    {
        mem = malloc(sizeof(*q->column_nodes));
    }
    else
    {
        mem = realloc(
            q->column_nodes,
            sizeof(*q->column_nodes) * (q->column_count + 1));
    }

    if (mem == NULL)
    {
        fprintf(stderr, "Out of memory\n");
        exit(-1);
    }

    q->column_nodes = mem;

    struct Node *p = &(q->column_nodes[q->column_count++]);

    clearNode(p);

    return p;
}

/**
 * Allocate a new node as one of the predicates on a query object and
 * increments the predicate counter.
 * Does not allocate child nodes on the new predicate node.
 */
struct Node *allocatePredicateNode(struct Query *q)
{
    void *mem;

    if (q->predicate_count == 0)
    {
        mem = malloc(sizeof(*q->predicate_nodes));
    }
    else
    {
        mem = realloc(
            q->predicate_nodes,
            sizeof(*q->predicate_nodes) * (q->predicate_count + 1));
    }

    if (mem == NULL)
    {
        fprintf(stderr, "Out of memory\n");
        exit(-1);
    }

    q->predicate_nodes = mem;

    struct Node *p = &(q->predicate_nodes[q->predicate_count++]);

    clearNode(p);

    return p;
}

/**
 * Expands FIELD_STAR, in FUNC_UNITY, or self-child optimisation, or once in
 * first level of function params.
 */
static void expandFieldStar(struct Query *query)
{
    // Count the total number of fields in all tables and cache the
    // result.
    int table_field_count = 0;
    for (int j = 0; j < query->table_count; j++)
    {
        struct Table *table = &query->tables[j];

        table_field_count += table->db->field_count;
    }

    for (int i = 0; i < query->column_count; i++)
    {
        struct Node *col = &query->column_nodes[i];

        int child_index = NOT_FOUND;

        if (col->function == FUNC_AGG_COUNT)
        {
            // COUNT(*) is special
            continue;
        }

        if (col->field.index == FIELD_STAR)
        {
            // Could be one of:
            // - SELECT *
            // - SELECT FN(*)
            child_index = -1;
        }
        else if (col->function != FUNC_UNITY)
        {
            // Could be one of:
            // - SELECT FN(*, 1)
            // - SELECT FN(1, *)
            // - etc.

            // Only go one level deep
            // Search all children to check there's not more than one '*'
            for (int j = 0; j < col->child_count; j++)
            {
                struct Node *child_col = &col->children[j];

                if (child_col->field.index == FIELD_STAR)
                {
                    if (child_index != NOT_FOUND)
                    {
                        fprintf(
                            stderr,
                            "Cannot deal with more than one '*' in params");
                        exit(-1);
                    }

                    child_index = j;
                }
            }
        }

        if (child_index != NOT_FOUND)
        {

            int new_column_count = query->column_count + table_field_count - 1;

            struct Node *new_cols = malloc(new_column_count * sizeof *col);

            // Copy first half of nodes from old list to new list;
            memcpy(new_cols, query->column_nodes, i * sizeof *col);

            // Create nodes for all fields in all tables
            int index = i;
            for (int j = 0; j < query->table_count; j++)
            {
                struct Table *table = &query->tables[j];

                for (int k = 0; k < table->db->field_count; k++)
                {
                    struct Node *new_col = new_cols + index++;

                    clearNode(new_col);

                    size_t alias_prefix_len = 0;

                    if (strchr(col->alias, '*') == NULL)
                    {
                        strcpy(new_col->alias, col->alias);
                        alias_prefix_len = strlen(col->alias);
                    }

                    if (col->child_count > 0)
                    {
                        // Clone all children
                        copyNodeTree(new_col, col);

                        // Set the field for the relevant node
                        struct Node *child_col = &new_col->children[child_index];
                        child_col->field.table_id = j;
                        child_col->field.index = k;

                        // Avoid overflow
                        const char *fieldName = getTableFieldName(table, k);
                        strncpy(child_col->field.text, fieldName, 31);
                        child_col->field.text[31] = '\0';

                        strncpy(
                            new_col->alias + alias_prefix_len,
                            fieldName,
                            MAX_FIELD_LENGTH - alias_prefix_len);
                    }
                    else
                    {
                        // FN(*) is supported
                        // i.e. gets expanded to FN(a), FN(b), etc.
                        new_col->function = col->function;
                        // Inherit the self-child optimisation if in place
                        new_col->child_count = col->child_count;
                        new_col->field.table_id = j;
                        new_col->field.index = k;

                        // Avoid overflow
                        const char *fieldName = getTableFieldName(table, k);
                        strncpy(new_col->field.text, fieldName, 31);
                        new_col->field.text[31] = '\0';

                        strncpy(
                            new_col->alias + alias_prefix_len,
                            fieldName,
                            MAX_FIELD_LENGTH - alias_prefix_len);
                    }
                }
            }

            // Copy rest of nodes
            memcpy(new_cols + index, query->column_nodes + i + 1, (new_column_count - index) * sizeof *col);

            free(query->column_nodes);

            query->column_nodes = new_cols;

            query->column_count += table_field_count - 1;

            i += table_field_count - 1;
        }
    }
}