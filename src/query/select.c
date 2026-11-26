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
#include "../fatal.h"

#ifdef DEBUG
int query_count = -1;
#endif

struct DB *stdin_db = NULL;

static int information_query(const char *table, FILE *output);

static int populate_tables(struct Query *q);

static int resolveNode(struct Query *query, struct Node *node, enum AliasSearchMode allow_aliases);

static int foldUncorrelatedSubquery(struct Node *node);

static int resolveQuantifiedOperator(
    struct Query *query,
    struct Node *node,
    enum AliasSearchMode allow_aliases);

static int foldUncorrelatedQuantified(struct Node *node);

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

    // Verbose mode prints out current query. end_ptr is NULL for the
    // implicit `SELECT * FROM stdin` invoked when stdin is not a terminal.
    if (output_flags & OUTPUT_OPTION_VERBOSE && end_ptr != NULL)
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

        destroyQuery(q);

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
        // LIMIT/OFFSET apply to the sorted grouped result, not the
        // materialised GROUP BY subquery (which must emit every group).
        q2a->offset_value = 0;
        q2a->limit_value = -1;

        struct Table table = {0};

        int result = process_subquery(
            q2a,
            (output_flags & OUTPUT_OPTION_STATS),
            table.name);

        destroyQuery(q2a);

        if (result < 0)
        {
            remove(table.name);
            return -1;
        }

        struct Query *q2b = makeQuery();
        q2b->tables = &table;
        q2b->table_count = 1;

        q2b->offset_value = q->offset_value;
        q2b->limit_value = q->limit_value;

        memcpy(
            q2b->order_nodes,
            q->order_nodes,
            sizeof(q->order_nodes));

        q2b->order_count = q->order_count;

        result = process_query(q2b, output_flags, output);

        remove(table.name);

        q2b->tables = NULL;

        destroyQuery(q2b);

        free(q2a);
        free(q2b);

        //      Why aren't we destroying here?
        // destroyQuery(&q);

        return result;
    }

    int result = process_query(q, output_flags, output);

    destroyQuery(q);

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
    // dummy row query. Correlated scalar subqueries bind outer columns as
    // constants — never steal the process stdin.
    if (q->table_count == 0)
    {
        if (!q->has_outer_row && !isatty(fileno(stdin)))
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
        // populate_tables may have cached this as stdin_db for reuse across
        // statements. Clear it before free — otherwise the next statement's
        // auto-stdin path reuses a dangling pointer (heap UAF / segfault).
        if (stdin_db == q->tables[0].db)
        {
            stdin_db = NULL;
        }
        closeDB(q->tables[0].db);
        free(q->tables[0].db);
        q->tables[0].db = NULL;
        free(q->tables);
        q->tables = NULL;
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

        // Resolve the whole predicate so OPERATOR_ANY/ALL can fold multi-row
        // subqueries (resolving children alone would scalar-fold the RHS).
        result = resolveNode(q, predicate, ALIASES_FIRST);

        if (result < 0)
        {
            fprintf(stderr, "Unable to resolve WHERE node (%d)\n", i);
            return result;
        }

        optimiseRowidAlgebra(predicate);

        optimiseCollapseConstantNode(predicate);

        optimiseUniqueOr(predicate);

        optimiseMergeOrRanges(predicate);
    }

    // Rowid / K may have introduced AND nodes — flatten again.
    optimiseFlattenANDPredicates(q);

    // Drop tautologies and tighten redundant same-column comparisons.
    optimiseRemoveTrivialPredicates(q);
    optimiseFoldRedundantPredicates(q);

    // Infer b=const from a=b AND a=const (helps indexed joins).
    optimiseTransitivePredicates(q);
    optimiseRemoveTrivialPredicates(q);

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

    // Populate HAVING predicates (aliases from SELECT allowed)
    for (int i = 0; i < q->having_count; i++)
    {
        struct Node *predicate = &q->having_nodes[i];

        result = resolveNode(q, predicate, ALIASES_FIRST);
        if (result < 0)
        {
            fprintf(stderr, "Unable to resolve HAVING node %i\n", i);
            return result;
        }
    }

    // Populate DISTINCT ON columns
    for (int i = 0; i < q->distinct_count; i++)
    {
        result = resolveNode(q, &q->distinct_nodes[i], ALIASES_FIRST);
        if (result < 0)
        {
            fprintf(stderr, "Unable to resolve DISTINCT ON node %i\n", i);
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

    destroyQuery(q);

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

    destroyQuery(q);

    free(q);

    return result;
}

int evaluate_subquery_to_db(
    struct Query *template_query,
    struct Table *outer_tables,
    int outer_table_count,
    RowListIndex outer_row_list,
    int outer_row_index,
    int has_outer_row,
    struct DB *db)
{
    if (template_query == NULL || db == NULL)
    {
        return -1;
    }

    memset(db, 0, sizeof(*db));

    beginRowListPoolNest();

    struct Query *q = cloneQuery(template_query);
    if (q == NULL)
    {
        endRowListPoolNest();
        return -1;
    }

    q->outer_tables = outer_tables;
    q->outer_table_count = outer_table_count;
    q->outer_row_list = outer_row_list;
    q->outer_row_index = outer_row_index;
    q->has_outer_row = has_outer_row;

    int result = csvMem_fromQuery(db, q);
    destroyQuery(q);
    free(q);
    endRowListPoolNest();

    if (result < 0)
    {
        closeDB(db);
        memset(db, 0, sizeof(*db));
        return -1;
    }

    return 0;
}

int evaluate_scalar_subquery(
    struct Query *template_query,
    struct Table *outer_tables,
    int outer_table_count,
    RowListIndex outer_row_list,
    int outer_row_index,
    int has_outer_row,
    char *output,
    int max_length)
{
    output[0] = '\0';

    struct DB db = {0};
    int result = evaluate_subquery_to_db(
        template_query,
        outer_tables,
        outer_table_count,
        outer_row_list,
        outer_row_index,
        has_outer_row,
        &db);
    if (result < 0)
    {
        return -1;
    }

    int rows = getRecordCount(&db);
    if (rows > 0 && db.field_count > 0)
    {
        result = getRecordValue(&db, 0, 0, output, max_length);
    }
    else
    {
        result = 0;
    }

    closeDB(&db);
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

/**
 * Free FUNC_SUBQUERY template Queries in a node tree without freeNode'ing
 * child arrays (safe when optimise has shallow-moved nodes).
 */
static void releaseSubqueryTemplates(struct Node *node)
{
    if (node == NULL)
    {
        return;
    }

    if (node->function == FUNC_SUBQUERY && node->subquery != NULL)
    {
        destroyQuery(node->subquery);
        free(node->subquery);
        node->subquery = NULL;
    }

    if (node->children != NULL && node->child_count > 0)
    {
        for (int i = 0; i < node->child_count; i++)
        {
            releaseSubqueryTemplates(&node->children[i]);
        }
    }

    if (node->filter != NULL)
    {
        releaseSubqueryTemplates(node->filter);
    }
}

void destroyQuery(struct Query *query)
{
    if (query == NULL)
    {
        return;
    }

    if (query->predicate_nodes != NULL)
    {
        // Optimise may shallow-move predicate nodes; do not freeNode the
        // whole tree (double-frees children). Only release owned subquery
        // templates, then free the array like historical destroy_query.
        for (int i = 0; i < query->predicate_count; i++)
        {
            releaseSubqueryTemplates(&query->predicate_nodes[i]);
        }
        free(query->predicate_nodes);
        query->predicate_nodes = NULL;
    }

    if (query->having_nodes != NULL)
    {
        for (int i = 0; i < query->having_count; i++)
        {
            freeNode(&query->having_nodes[i]);
        }
        free(query->having_nodes);
        query->having_nodes = NULL;
    }

    if (query->tables != NULL)
    {
        for (int i = 0; i < query->table_count; i++)
        {
            freeNode(&query->tables[i].join);

            if (query->tables[i].subquery != NULL)
            {
                destroyQuery(query->tables[i].subquery);
                free(query->tables[i].subquery);
                query->tables[i].subquery = NULL;
            }

            if (query->tables[i].db == stdin_db)
            {
                // Don't close special DB created from stdin
                continue;
            }

            // Unresolved table-subquery sentinel — never close/free.
            if (query->tables[i].db == DB_SUBQUERY)
            {
                query->tables[i].db = NULL;
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

    if (query->column_nodes != NULL)
    {
        for (int i = 0; i < query->column_count; i++)
        {
            freeNode(&query->column_nodes[i]);
        }
        free(query->column_nodes);
        query->column_nodes = NULL;
    }

    for (int i = 0; i < query->order_count; i++)
    {
        freeNode(&query->order_nodes[i]);
    }

    for (int i = 0; i < query->group_count; i++)
    {
        freeNode(&query->group_nodes[i]);
    }

    for (int i = 0; i < query->distinct_count; i++)
    {
        freeNode(&query->distinct_nodes[i]);
    }

    query->outer_tables = NULL;
    query->outer_table_count = 0;
    query->has_outer_row = 0;
}

struct Query *cloneQuery(const struct Query *src)
{
    if (src == NULL)
    {
        return NULL;
    }

    struct Query *q = calloc(1, sizeof(*q));
    if (q == NULL)
    {
        return NULL;
    }

#ifdef DEBUG
    q->id = ++query_count;
#endif

    q->flags = src->flags;
    q->offset_value = src->offset_value;
    q->limit_value = src->limit_value;
    // Outer binds are set by the caller after clone.
    q->outer_tables = NULL;
    q->outer_table_count = 0;
    q->outer_row_list = 0;
    q->outer_row_index = 0;
    q->has_outer_row = 0;

    if (src->table_count > 0 && src->tables != NULL)
    {
        q->tables = calloc((size_t)src->table_count, sizeof(*q->tables));
        if (q->tables == NULL)
        {
            free(q);
            return NULL;
        }
        q->table_count = src->table_count;
        for (int i = 0; i < src->table_count; i++)
        {
            strcpy(q->tables[i].name, src->tables[i].name);
            strcpy(q->tables[i].alias, src->tables[i].alias);
            q->tables[i].join_type = src->tables[i].join_type;
            q->tables[i].subquery = NULL;
            // Prefer heap subquery template: clone and mark for re-materialize.
            // Never share opened DB pointers from a prior run.
            if (src->tables[i].subquery != NULL)
            {
                q->tables[i].subquery = cloneQuery(src->tables[i].subquery);
                if (q->tables[i].subquery == NULL)
                {
                    destroyQuery(q);
                    free(q);
                    return NULL;
                }
                q->tables[i].db = DB_SUBQUERY;
            }
            else if (src->tables[i].db == DB_SUBQUERY)
            {
                q->tables[i].db = DB_SUBQUERY;
            }
            else
            {
                q->tables[i].db = NULL;
            }
            clearNode(&q->tables[i].join);
            copyNodeTree(&q->tables[i].join, &src->tables[i].join);
        }
    }

    if (src->column_count > 0 && src->column_nodes != NULL)
    {
        q->column_nodes =
            calloc((size_t)src->column_count, sizeof(*q->column_nodes));
        if (q->column_nodes == NULL)
        {
            destroyQuery(q);
            free(q);
            return NULL;
        }
        q->column_count = src->column_count;
        for (int i = 0; i < src->column_count; i++)
        {
            clearNode(&q->column_nodes[i]);
            copyNodeTree(&q->column_nodes[i], &src->column_nodes[i]);
            // copyNodeTree skips alias; restore it for SELECT labels.
            strcpy(q->column_nodes[i].alias, src->column_nodes[i].alias);
        }
    }

    if (src->predicate_count > 0 && src->predicate_nodes != NULL)
    {
        q->predicate_nodes =
            calloc((size_t)src->predicate_count, sizeof(*q->predicate_nodes));
        if (q->predicate_nodes == NULL)
        {
            destroyQuery(q);
            free(q);
            return NULL;
        }
        q->predicate_count = src->predicate_count;
        for (int i = 0; i < src->predicate_count; i++)
        {
            clearNode(&q->predicate_nodes[i]);
            copyNodeTree(&q->predicate_nodes[i], &src->predicate_nodes[i]);
        }
    }

    if (src->having_count > 0 && src->having_nodes != NULL)
    {
        q->having_nodes =
            calloc((size_t)src->having_count, sizeof(*q->having_nodes));
        if (q->having_nodes == NULL)
        {
            destroyQuery(q);
            free(q);
            return NULL;
        }
        q->having_count = src->having_count;
        for (int i = 0; i < src->having_count; i++)
        {
            clearNode(&q->having_nodes[i]);
            copyNodeTree(&q->having_nodes[i], &src->having_nodes[i]);
        }
    }

    q->order_count = src->order_count;
    for (int i = 0; i < src->order_count; i++)
    {
        clearNode(&q->order_nodes[i]);
        copyNodeTree(&q->order_nodes[i], &src->order_nodes[i]);
        strcpy(q->order_nodes[i].alias, src->order_nodes[i].alias);
    }

    q->group_count = src->group_count;
    for (int i = 0; i < src->group_count; i++)
    {
        clearNode(&q->group_nodes[i]);
        copyNodeTree(&q->group_nodes[i], &src->group_nodes[i]);
    }

    q->distinct_count = src->distinct_count;
    for (int i = 0; i < src->distinct_count; i++)
    {
        clearNode(&q->distinct_nodes[i]);
        copyNodeTree(&q->distinct_nodes[i], &src->distinct_nodes[i]);
    }

    return q;
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

        if (found == 0 && table->db != DB_SUBQUERY)
        {
            // Try to reuse existing open table next (file tables only —
            // pending FROM/CTE subqueries all share the display name
            // "SUBQUERY" and must not share DBs).
            for (int j = 0; j < i; j++)
            {
                if (q->tables[j].db == DB_SUBQUERY)
                {
                    continue;
                }
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
            if (table->db == NULL)
            {
                fprintf(stderr, "Out of memory\n");
                return -1;
            }

            int result;
            if (table->subquery != NULL)
            {
                // Clone + materialize so the template Query stays reusable.
                result = evaluate_subquery_to_db(
                    table->subquery,
                    NULL,
                    0,
                    0,
                    0,
                    0,
                    table->db);
            }
            else
            {
                // Legacy fallback: SQL still in name (should not happen).
                result = select_subquery_mem(table->name, table->db, NULL);
            }
            if (result < 0)
            {
                return -1;
            }

            strcpy(table->name, "SUBQUERY");

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
            result = resolveNode(q, &table->join, NO_ALIASES);
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
/**
 * True if this node tree contains non-deterministic functions that must not
 * be constant-folded across outer rows.
 */
static int nodeIsNondeterministic(struct Node *node)
{
    if (node == NULL)
    {
        return 0;
    }

    if (
        node->function == FUNC_RANDOM ||
        node->function == FUNC_DATE_NOW ||
        node->function == FUNC_DATE_TODAY ||
        node->function == FUNC_DATE_CLOCK)
    {
        return 1;
    }

    if (node->function == FUNC_SUBQUERY && node->subquery != NULL)
    {
        // Conservatively skip folding parents that embed nested scalars.
        return 1;
    }

    if (node->filter != NULL && nodeIsNondeterministic(node->filter))
    {
        return 1;
    }

    if (node->child_count > 0)
    {
        for (int i = 0; i < node->child_count; i++)
        {
            if (nodeIsNondeterministic(&node->children[i]))
            {
                return 1;
            }
        }
    }

    return 0;
}

static int queryIsNondeterministic(struct Query *q)
{
    for (int i = 0; i < q->column_count; i++)
    {
        if (nodeIsNondeterministic(&q->column_nodes[i]))
        {
            return 1;
        }
    }
    for (int i = 0; i < q->predicate_count; i++)
    {
        if (nodeIsNondeterministic(&q->predicate_nodes[i]))
        {
            return 1;
        }
    }
    for (int i = 0; i < q->having_count; i++)
    {
        if (nodeIsNondeterministic(&q->having_nodes[i]))
        {
            return 1;
        }
    }
    for (int i = 0; i < q->order_count; i++)
    {
        if (nodeIsNondeterministic(&q->order_nodes[i]))
        {
            return 1;
        }
    }
    for (int i = 0; i < q->group_count; i++)
    {
        if (nodeIsNondeterministic(&q->group_nodes[i]))
        {
            return 1;
        }
    }
    for (int i = 0; i < q->distinct_count; i++)
    {
        if (nodeIsNondeterministic(&q->distinct_nodes[i]))
        {
            return 1;
        }
    }
    for (int i = 0; i < q->table_count; i++)
    {
        if (nodeIsNondeterministic(&q->tables[i].join))
        {
            return 1;
        }
    }
    return 0;
}

/**
 * Quiet check: does this leaf field fail to resolve on sq's own tables?
 * sq must already have populate_tables() applied.
 */
static int leafNeedsOuter(struct Query *sq, struct Node *node)
{
    if (node->field.index == FIELD_CONSTANT ||
        node->field.index == FIELD_STAR)
    {
        return 0;
    }

    if (node->field.text[0] == '\0')
    {
        return 0;
    }

    // Already bound to a concrete column on this query
    if (
        node->field.table_id >= 0 &&
        node->field.table_id < sq->table_count &&
        (node->field.index >= 0 || node->field.index == FIELD_ROW_INDEX))
    {
        return 0;
    }

    char name_buf[MAX_FIELD_LENGTH];
    strcpy(name_buf, node->field.text);
    struct Field tmp = {0};
    if (evaluatePossibleNamedConstantField(name_buf, &tmp) >= 0)
    {
        return 0;
    }

    int table_id = -1;
    int column_id = FIELD_UNKNOWN;
    if (find_field(sq, node->field.text, &table_id, &column_id, NULL))
    {
        return 0;
    }

    return 1;
}

static int nodeNeedsOuter(struct Query *sq, struct Node *node)
{
    if (node == NULL)
    {
        return 0;
    }

    // Nested scalar subquery: skip folding the parent (handled above via
    // nondeterministic/conservative path). Treat as needing outer to be safe.
    if (node->function == FUNC_SUBQUERY)
    {
        return 1;
    }

    if (node->filter != NULL && nodeNeedsOuter(sq, node->filter))
    {
        return 1;
    }

    if (node->child_count > 0)
    {
        for (int i = 0; i < node->child_count; i++)
        {
            if (nodeNeedsOuter(sq, &node->children[i]))
            {
                return 1;
            }
        }
        return 0;
    }

    return leafNeedsOuter(sq, node);
}

static int queryNeedsOuter(struct Query *sq)
{
    for (int i = 0; i < sq->column_count; i++)
    {
        if (nodeNeedsOuter(sq, &sq->column_nodes[i]))
        {
            return 1;
        }
    }
    for (int i = 0; i < sq->predicate_count; i++)
    {
        if (nodeNeedsOuter(sq, &sq->predicate_nodes[i]))
        {
            return 1;
        }
    }
    for (int i = 0; i < sq->having_count; i++)
    {
        if (nodeNeedsOuter(sq, &sq->having_nodes[i]))
        {
            return 1;
        }
    }
    for (int i = 0; i < sq->order_count; i++)
    {
        if (nodeNeedsOuter(sq, &sq->order_nodes[i]))
        {
            return 1;
        }
    }
    for (int i = 0; i < sq->group_count; i++)
    {
        if (nodeNeedsOuter(sq, &sq->group_nodes[i]))
        {
            return 1;
        }
    }
    for (int i = 0; i < sq->distinct_count; i++)
    {
        if (nodeNeedsOuter(sq, &sq->distinct_nodes[i]))
        {
            return 1;
        }
    }
    for (int i = 0; i < sq->table_count; i++)
    {
        if (nodeNeedsOuter(sq, &sq->tables[i].join))
        {
            return 1;
        }
    }
    return 0;
}

/**
 * True if the template subquery references columns outside its own FROM
 * (i.e. is correlated), or cannot be analysed.
 */
static int subqueryIsCorrelated(struct Query *template_query)
{
    if (template_query == NULL)
    {
        return 1;
    }

    if (queryIsNondeterministic(template_query))
    {
        return 1;
    }

    struct Query *q = cloneQuery(template_query);
    if (q == NULL)
    {
        return 1;
    }

    if (populate_tables(q) < 0)
    {
        destroyQuery(q);
        free(q);
        return 1;
    }

    int correlated = queryNeedsOuter(q);

    destroyQuery(q);
    free(q);

    return correlated;
}

/**
 * If node is an uncorrelated FUNC_SUBQUERY, evaluate once and replace with
 * a constant. Correlated / nondeterministic subqueries are left unchanged.
 * Returns 0 on success (folded or left as-is), -1 on hard failure.
 */
static int foldUncorrelatedSubquery(struct Node *node)
{
    if (node->function != FUNC_SUBQUERY || node->subquery == NULL)
    {
        return 0;
    }

    if (subqueryIsCorrelated(node->subquery))
    {
        return 0;
    }

    char value[MAX_VALUE_LENGTH];
    int result = evaluate_scalar_subquery(
        node->subquery,
        NULL,
        0,
        -1,
        0,
        0,
        value,
        MAX_VALUE_LENGTH);

    if (result < 0)
    {
        // Leave for runtime rather than failing the outer query here.
        return 0;
    }

    if ((int)strlen(value) >= MAX_FIELD_LENGTH)
    {
        fprintf(
            stderr,
            "Uncorrelated subquery result exceeds %d bytes\n",
            MAX_FIELD_LENGTH - 1);
        return -1;
    }

    char alias[MAX_FIELD_LENGTH];
    strcpy(alias, node->alias);

    destroyQuery(node->subquery);
    free(node->subquery);
    node->subquery = NULL;

    freeNode(node);
    clearNode(node);

    node->function = FUNC_UNITY;
    node->field.index = FIELD_CONSTANT;
    node->field.table_id = TABLE_NONE;
    strcpy(node->field.text, value);
    strcpy(node->alias, alias);

    return 0;
}

/**
 * Expand uncorrelated <expr> op ANY|ALL (subquery) into OR/AND of compares
 * against constant RHS values (same shape as literal IN). Empty ANY → NEVER;
 * empty ALL → ALWAYS. Correlated subqueries are left for per-row eval.
 */
static int foldUncorrelatedQuantified(struct Node *node)
{
    if (node->function != OPERATOR_ANY && node->function != OPERATOR_ALL)
    {
        return 0;
    }
    if (node->child_count != 2)
    {
        return 0;
    }

    struct Node *right = &node->children[1];
    if (right->function != FUNC_SUBQUERY || right->subquery == NULL)
    {
        return 0;
    }

    if (subqueryIsCorrelated(right->subquery))
    {
        return 0;
    }

    enum Function compare = quantifiedCompare(node);
    enum Function quant = node->function;

    struct DB db = {0};
    int result = evaluate_subquery_to_db(
        right->subquery,
        NULL,
        0,
        -1,
        0,
        0,
        &db);
    if (result < 0)
    {
        // Leave for runtime.
        return 0;
    }

    int rows = getRecordCount(&db);
    if (rows == 0 || db.field_count <= 0)
    {
        closeDB(&db);
        // Replace in place: free children/subquery, then become NEVER/ALWAYS.
        freeNode(node);
        node->function = (quant == OPERATOR_ANY) ? OPERATOR_NEVER : OPERATOR_ALWAYS;
        node->child_count = 0;
        node->children = NULL;
        node->filter = NULL;
        node->subquery = NULL;
        node->field.index = FIELD_UNKNOWN;
        node->field.table_id = TABLE_NONE;
        node->field.text[0] = '\0';
        node->alias[0] = '\0';
        return 0;
    }

    // Build replacement OR/AND tree from a copy of the left expr.
    struct Node left_copy;
    clearNode(&left_copy);
    copyNodeTree(&left_copy, &node->children[0]);

    struct Node *arms = calloc((size_t)rows, sizeof(*arms));
    if (arms == NULL)
    {
        freeNode(&left_copy);
        closeDB(&db);
        fprintf(stderr, "Out of memory expanding quantified subquery\n");
        return -1;
    }

    int arm_count = 0;
    for (int i = 0; i < rows; i++)
    {
        char value[MAX_VALUE_LENGTH];
        if (getRecordValue(&db, i, 0, value, MAX_VALUE_LENGTH) < 0)
        {
            // Unreadable row (e.g. historic header-only phantom) — skip.
            continue;
        }
        if ((int)strlen(value) >= MAX_FIELD_LENGTH)
        {
            for (int j = 0; j < arm_count; j++)
            {
                freeNode(&arms[j]);
            }
            free(arms);
            freeNode(&left_copy);
            closeDB(&db);
            fprintf(
                stderr,
                "Uncorrelated quantified subquery value exceeds %d bytes\n",
                MAX_FIELD_LENGTH - 1);
            return -1;
        }

        clearNode(&arms[arm_count]);
        struct Node *pair = allocateNodeChildren(&arms[arm_count], 2);
        arms[arm_count].function = compare;
        copyNodeTree(&pair[0], &left_copy);
        pair[1].function = FUNC_UNITY;
        pair[1].field.index = FIELD_CONSTANT;
        pair[1].field.table_id = TABLE_NONE;
        strcpy(pair[1].field.text, value);
        arm_count++;
    }

    closeDB(&db);
    freeNode(&left_copy);

    if (arm_count == 0)
    {
        free(arms);
        freeNode(node);
        node->function = (quant == OPERATOR_ANY) ? OPERATOR_NEVER : OPERATOR_ALWAYS;
        node->child_count = 0;
        node->children = NULL;
        node->filter = NULL;
        node->subquery = NULL;
        node->field.index = FIELD_UNKNOWN;
        node->field.table_id = TABLE_NONE;
        node->field.text[0] = '\0';
        node->alias[0] = '\0';
        return 0;
    }

    freeNode(node);
    clearNode(node);

    if (arm_count == 1)
    {
        // Single arm: promote the compare node itself.
        memcpy(node, &arms[0], sizeof(*node));
        free(arms);
        return 0;
    }

    node->function = (quant == OPERATOR_ANY) ? OPERATOR_OR : OPERATOR_AND;
    node->children = arms;
    node->child_count = arm_count;
    return 0;
}

/**
 * Resolve OPERATOR_ANY / OPERATOR_ALL without scalar-folding the subquery
 * child (multi-row RHS). Uncorrelated quantified forms expand to OR/AND.
 */
static int resolveQuantifiedOperator(
    struct Query *query,
    struct Node *node,
    enum AliasSearchMode allow_aliases)
{
    if (node->child_count != 2)
    {
        fprintf(stderr, "Quantified operator expects two children\n");
        return -1;
    }

    int result = resolveNode(query, &node->children[0], allow_aliases);
    if (result < 0)
    {
        return -1;
    }

    // Do not call foldUncorrelatedSubquery on the RHS — keep FUNC_SUBQUERY
    // for foldUncorrelatedQuantified / runtime multi-row eval.
    struct Node *right = &node->children[1];
    if (right->function != FUNC_SUBQUERY)
    {
        result = resolveNode(query, right, allow_aliases);
        if (result < 0)
        {
            return -1;
        }
    }

    return foldUncorrelatedQuantified(node);
}

static int resolveNode(struct Query *query, struct Node *node, enum AliasSearchMode allow_aliases)
{
    // Uncorrelated scalar subqueries fold to constants; correlated ones are
    // left as FUNC_SUBQUERY for per-row clone/eval.
    if (node->function == FUNC_SUBQUERY)
    {
        return foldUncorrelatedSubquery(node);
    }

    if (node->function == OPERATOR_ANY || node->function == OPERATOR_ALL)
    {
        return resolveQuantifiedOperator(query, node, allow_aliases);
    }

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

    // Correlated subquery: bind unresolved names from the outer row as constants
    if (query->has_outer_row && query->outer_tables != NULL &&
        query->outer_table_count > 0)
    {
        struct Query outer_view = {0};
        outer_view.tables = query->outer_tables;
        outer_view.table_count = query->outer_table_count;

        int outer_table_id = -1;
        int outer_column_id = FIELD_UNKNOWN;

        if (find_field(
                &outer_view,
                node->field.text,
                &outer_table_id,
                &outer_column_id,
                NULL))
        {
            struct Node outer_ref = {0};
            clearNode(&outer_ref);
            outer_ref.function = FUNC_UNITY;
            outer_ref.field.table_id = outer_table_id;
            outer_ref.field.index = outer_column_id;

            char value[MAX_VALUE_LENGTH];
            evaluateNode(
                query->outer_tables,
                query->outer_row_list,
                query->outer_row_index,
                &outer_ref,
                value,
                MAX_VALUE_LENGTH);

            if ((int)strlen(value) >= MAX_FIELD_LENGTH)
            {
                fprintf(
                    stderr,
                    "Correlated binding for '%s' exceeds %d bytes\n",
                    node->field.text,
                    MAX_FIELD_LENGTH - 1);
                return -1;
            }

            node->function = FUNC_UNITY;
            node->child_count = 0;
            node->field.index = FIELD_CONSTANT;
            node->field.table_id = TABLE_NONE;
            strcpy(node->field.text, value);
            return 0;
        }
    }

    // We couldn't resolve the node/field. If there was no table specified, then
    // that's probably why. Otherwise the field couldn't be found in any of the
    // tables that we do have.

    if (query->table_count == 0 && !query->has_outer_row)
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
    destroyQuery(q2);

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
            csvdb_fatal(-1);
        }

        q->tables = ptr;

        // Zero out realloc'd space
        memset(
            q->tables + (q->table_count - 1),
            0,
            sizeof(struct Table));
    }

    struct Table *table = &q->tables[q->table_count - 1];

    // Expression-subquery tables (allocated from parseNode) never go through
    // the FROM-clause path that sets join.function. Leaving it as FUNC_UNITY
    // (zero) makes optimiseOnToWhere promote the bare node into WHERE and
    // crashes the planner when two or more such tables are joined.
    table->join.function = OPERATOR_ALWAYS;

    return table;
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
        csvdb_fatal(-1);
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
        csvdb_fatal(-1);
    }

    q->predicate_nodes = mem;

    struct Node *p = &(q->predicate_nodes[q->predicate_count++]);

    clearNode(p);

    return p;
}

struct Node *allocateHavingNode(struct Query *q)
{
    void *mem;

    if (q->having_count == 0)
    {
        mem = malloc(sizeof(*q->having_nodes));
    }
    else
    {
        mem = realloc(
            q->having_nodes,
            sizeof(*q->having_nodes) * (q->having_count + 1));
    }

    if (mem == NULL)
    {
        fprintf(stderr, "Out of memory\n");
        csvdb_fatal(-1);
    }

    q->having_nodes = mem;

    struct Node *p = &(q->having_nodes[q->having_count++]);

    clearNode(p);

    return p;
}

/**
 * Expands FIELD_STAR, in FUNC_UNITY, or self-child optimisation, or once in
 * first level of function params.
 */
static void expandFieldStar(struct Query *query)
{
    for (int i = 0; i < query->column_count; i++)
    {
        struct Node *col = &query->column_nodes[i];

        int child_index = NOT_FOUND;
        /* Which table's fields to expand: -1 means every table (bare *). */
        int star_table_id = -1;

        if (col->function == FUNC_AGG_COUNT)
        {
            // COUNT(*) is special
            continue;
        }

        if (col->field.index == FIELD_STAR)
        {
            // Could be one of:
            // - SELECT *
            // - SELECT table.*
            // - SELECT FN(*)
            child_index = -1;
            star_table_id = col->field.table_id;
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
                        csvdb_fatal(-1);
                    }

                    child_index = j;
                    star_table_id = child_col->field.table_id;
                }
            }
        }

        if (child_index != NOT_FOUND)
        {
            int j_start = 0;
            int j_end = query->table_count;
            if (star_table_id >= 0)
            {
                j_start = star_table_id;
                j_end = star_table_id + 1;
            }

            int expand_field_count = 0;
            for (int j = j_start; j < j_end; j++)
            {
                expand_field_count += query->tables[j].db->field_count;
            }

            int new_column_count =
                query->column_count + expand_field_count - 1;

            struct Node *new_cols = malloc(new_column_count * sizeof *col);

            // Copy first half of nodes from old list to new list;
            memcpy(new_cols, query->column_nodes, i * sizeof *col);

            // Create nodes for fields in the selected table(s)
            int index = i;
            for (int j = j_start; j < j_end; j++)
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
            memcpy(
                new_cols + index,
                query->column_nodes + i + 1,
                (new_column_count - index) * sizeof *col
            );

            free(query->column_nodes);

            query->column_nodes = new_cols;

            query->column_count += expand_field_count - 1;

            i += expand_field_count - 1;
        }
    }
}
