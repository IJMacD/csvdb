#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include <sys/time.h>
#include <unistd.h>

#include "query.h"
#include "../execute/execute.h"
#include "../evaluate/evaluate.h"
#include "create.h"
#include "token.h"
#include "parse.h"
#include "explain.h"
#include "plan.h"
#include "../db/db.h"
#include "../db/csv-mem.h"
#include "../functions/util.h"

#ifdef DEBUG
int query_count = -1;
#endif

int basic_select_query (
    struct Query *q,
    struct Plan *plan,
    enum OutputOption output_flags,
    FILE * output
);

int information_query (const char *table, FILE * output);

static int populate_tables (struct Query *q, struct DB * dbs);

int resolveNode (struct Query *query, struct Node *node, int allow_aliases);

static int find_field (
    struct Query *q,
    const char *text,
    int *table_id,
    int *column_id,
    char *name
);

static void check_column_aliases (struct Table * table);

static int process_query (
    struct Query *q,
    enum OutputOption output_flags,
    FILE * output
);

static int process_subquery(
    struct Query *query,
    enum OutputOption options,
    char *output_filename
);

static void destroy_query (struct Query *q);

static int wrap_query (
    struct Query *query,
    enum OutputOption inner_options,
    enum OutputOption outer_options,
    FILE *output
);

int runQueries (
    const char *query_string,
    enum OutputOption output_flags,
    FILE * output
) {
    const char *end_ptr = query_string;

    while(*end_ptr != '\0') {
        const char *query_start = end_ptr;
        int result = query(query_start, output_flags, output, &end_ptr);

        if (result < 0) {
            if (query_start != end_ptr) {
                size_t query_len = end_ptr - query_start;
                fprintf(stderr, "Error with query: \n");
                fwrite(query_start, 1, query_len, stderr);
                fprintf(stderr, "\n");
            }

            return result;
        }

        if (query_start == end_ptr) {
            break;
        }
    }

    return 0;
}

int query (
    const char *query,
    enum OutputOption output_flags,
    FILE * output,
    const char **end_ptr
) {
    #ifdef DEBUG
    fprintf(stderr, "Start Query (%d.%d)\n", getpid(), ++query_count);
    #endif

    skipWhitespacePtr(&query);

    if (strncmp(query, "CREATE ", 7) == 0) {
        if (output_flags & FLAG_READ_ONLY) {
            fprintf(stderr, "Tried to CREATE while in read-only mode\n");
            return -1;
        }

        if (output_flags & FLAG_EXPLAIN) {
            return -1;
        }

        return create_query(query, end_ptr);
    }

    if (strncmp(query, "INSERT ", 7) == 0) {
        if (output_flags & FLAG_READ_ONLY) {
            fprintf(stderr, "Tried to INSERT while in read-only mode\n");
            return -1;
        }

        if (output_flags & FLAG_EXPLAIN) {
            return 0;
        }

        return insert_query(query, end_ptr);
    }

    // If we're querying the stats table then we must have stats turned off
    // for this query otherwise they would get overwritten.
    if (
        strcmp(query, "TABLE stats") == 0
        || strncmp(query, "FROM stats", 10) == 0
    ) {
        output_flags &= ~OUTPUT_OPTION_STATS;
    }

    if (output_flags & OUTPUT_OPTION_STATS) {
        // start stats file
        FILE *fstats = fopen("stats.csv", "w");
        fputs("operation,duration\n",fstats);
        fclose(fstats);
    }

    return select_query(query, output_flags, output, end_ptr);
}

int select_query (
    const char *query,
    enum OutputOption output_flags,
    FILE * output,
    const char **end_ptr
) {
    struct Query q = {0};
    struct timeval stop, start;

    if (output_flags & OUTPUT_OPTION_STATS) {
        gettimeofday(&start, NULL);
    }

    if (parseQuery(&q, query, end_ptr) < 0) {
        return -1;
    }

    if (output_flags & OUTPUT_OPTION_VERBOSE) {
        size_t query_len = *end_ptr - query;
        fwrite(query, 1, query_len, stderr);
        fprintf(stderr, "\n");
    }

    if (output_flags & OUTPUT_OPTION_STATS) {
        gettimeofday(&stop, NULL);

        FILE *fstats = fopen("stats.csv", "a");

        fprintf(fstats, "PARSE,%ld\n", dt(stop, start));

        // Will be opened again with a fresh handle in processQuery() and
        // executeQueryPlan()
        fclose(fstats);
    }

    int explain = (q.flags & FLAG_EXPLAIN) || (output_flags & FLAG_EXPLAIN);

    enum OutputOption format = output_flags & OUTPUT_MASK_FORMAT;

    // In order to support different output formats for EXPLAIN we wrap the
    // whole query in a subquery.
    if (format != OUTPUT_FORMAT_COMMA && explain) {
        return wrap_query(
            &q,
            FLAG_EXPLAIN,
            format | (output_flags & OUTPUT_OPTION_HEADERS),
            output
        );
    }

    // Cannot group and sort in the same query.
    if ((q.flags & FLAG_GROUP) && q.order_count > 0)
    {
        // We will materialise the GROUP'd query to disk then sort that

        // Make a copy of the struct
        struct Query q2a = q;
        q2a.order_count = 0;

        struct Table table = {0};

        int result = process_subquery(
            &q2a,
            (output_flags & OUTPUT_OPTION_STATS),
            table.name
        );

        destroy_query(&q2a);

        if (result < 0) {
            remove(table.name);
            return -1;
        }

        struct Query q2b = {0};
        q2b.tables = &table;
        q2b.table_count = 1;

        q2b.limit_value = -1;

        memcpy(
            q2b.order_nodes,
            q.order_nodes,
            sizeof(q.order_nodes)
        );

        q2b.order_count = q.order_count;

        result = process_query(&q2b, output_flags, output);

        remove(table.name);

        q2b.tables = NULL;

        destroy_query(&q2b);

        return result;
    }

    int result = process_query(&q, output_flags, output);

    destroy_query(&q);

    return result;
}

static int process_query (
    struct Query *q,
    enum OutputOption output_flags,
    FILE * output
) {
    int result;

    // Explain can be specified on the command line so copy that value in just
    // in case.
    if (output_flags & FLAG_EXPLAIN) {
        q->flags |= FLAG_EXPLAIN;
    }

    if (q->table_count == 0) {
        // No table was specified.
        // However, if stdin is something more than a tty (i.e pipe or
        // redirected file) then we can default to it.
        if (!isatty(fileno(stdin))) {
            q->tables = calloc(1, sizeof (struct Table));
            q->table_count = 1;
            strcpy(q->tables[0].name, "stdin");
            strcpy(q->tables[0].alias, "stdin");
        }
        else  if (q->column_count > 0) {
            // We could have a constant query which will output a single row
            // Check if any of the fields are non-constant and abort

            for (int i = 0; i < q->column_count; i++) {
                if (isConstantNode((struct Node *)&q->columns[i]) == 0) {
                    fprintf(stderr, "No Tables specified\n");
                    return -1;
                }
            }
        }
        else {
            // We have an empty query.
            // We'll just exit without error
            return 0;
        }
    }
    else if (strcmp(q->tables[0].name, "INFORMATION") == 0) {
        if (q->predicate_count < 1) {
            return -1;
        }

        q->tables[0].db = NULL;

        result = information_query(
            q->predicate_nodes[0].children[1].field.text,
            output
        );

        return result;
    }

    /*************************
     * Begin Query processing
     *************************/
    FILE *fstats = NULL;
    struct timeval stop, start;

    if (output_flags & OUTPUT_OPTION_STATS) {
        fstats = fopen("stats.csv", "a");

        gettimeofday(&start, NULL);
    }

    if (q->column_count == 0) {
        // Allow SELECT to be optional and default to SELECT *
        q->columns[0].function = FUNC_UNITY;
        q->columns[0].field.index = FIELD_STAR;
        q->columns[0].field.table_id = -1;
        q->column_count = 1;
    }

    // Create array on stack to hold DB structs
    struct DB dbs[MAX_TABLE_COUNT] = {0};

    // Populate Tables
    // (including JOIN predicate columns)
    result = populate_tables(q, dbs);
    if (result < 0) {
        fprintf(stderr, "Unable to populate tables\n");
        return result;
    }

    // Populate SELECT Columns
    for (int i = 0; i < q->column_count; i++) {
        result = resolveNode(q, &q->columns[i], 0);
        if (result < 0) {
            fprintf(stderr, "Unable to resolve SELECT column %d\n", i);
            return result;
        }
    }

    // Populate WHERE columns
    for (int i = 0; i < q->predicate_count; i++) {
        result = resolveNode(q, &q->predicate_nodes[i].children[0], 1);
        if (result < 0) {
            fprintf(stderr, "Unable to resolve WHERE node (%d left)\n", i);
            return result;
        }
        result = resolveNode(q, &q->predicate_nodes[i].children[1], 1);
        if (result < 0) {
            fprintf(stderr, "Unable to resolve WHERE node (%i right)\n", i);
            return result;
        }
    }

    // Populate ORDER BY columns
    for (int i = 0; i < q->order_count; i++) {
        result = resolveNode(q, &q->order_nodes[i], 1);
        if (result < 0) {
            fprintf(stderr, "Unable to resolve ORDER BY node %i\n", i);
            return result;
        }
    }

    // Populate GROUP BY columns
    for (int i = 0; i < q->group_count; i++) {
        result = resolveNode(q, &q->group_nodes[i], 1);
        if (result < 0) {
            fprintf(stderr, "Unable to resolve GROUP BY node %i\n", i);
            return result;
        }
    }

    if (output_flags & OUTPUT_OPTION_STATS) {
        gettimeofday(&stop, NULL);

        fprintf(fstats, "LOAD TABLES,%ld\n", dt(stop, start));

        start = stop;
    }

    /**********************
     * Make Plan
     **********************/
    struct Plan plan;

    makePlan(q, &plan);

    if (output_flags & OUTPUT_OPTION_STATS) {
        gettimeofday(&stop, NULL);

        fprintf(fstats, "MAKE PLAN,%ld\n", dt(stop, start));

        // Will be opened again with a fresh handle in executeQueryPlan()
        fclose(fstats);
    }

    if (q->flags & FLAG_EXPLAIN) {
        result =  explain_select_query(q->tables, &plan, output_flags, output);
        destroyPlan(&plan);
        return result;
    }

    result = executeQueryPlan(
        q->tables,
        q->table_count,
        &plan,
        output_flags,
        output
    );

    for (int i = 0; i < q->table_count; i++) {
        closeDB(q->tables[i].db);
    }

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
 * @return int 0 for success; -1 for failure
 */
int select_subquery(const char *query, char *filename) {
    struct Query q = {0};

    int result = parseQuery(&q, query, NULL);
    if (result < 0) {
        return -1;
    }

    result = process_subquery(&q, 0, filename);

    destroy_query(&q);

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
    char *output_filename
) {
    sprintf(output_filename, "/tmp/csvdb.%d-%d.csv", getpid(), rand());
    FILE *f = fopen(output_filename, "w");

    int result = process_query(
        query,
        options | OUTPUT_OPTION_HEADERS | OUTPUT_FORMAT_COMMA,
        f
    );

    fclose(f);

    if (result < 0) {
        return -1;
    }

    return 0;
}

int information_query (const char *table, FILE * output) {
    struct DB db;

    if (openDB(&db, table) != 0) {
        fprintf(stderr, "File not found: '%s'\n", table);
        return -1;
    }

    fprintf(output, "Table:\t%s\n", table);
    fprintf(output, "Fields:\t%d\n", db.field_count);
    fprintf(output, "Records:\t%d\n", getRecordCount(&db));

    fprintf(output, "\n");

    fprintf(output, "field\tindex\n");
    fprintf(output, "-----\t-----\n");

    for (int i = 0; i < db.field_count; i++) {
        int have_index = 0;

        if (findIndex(NULL, table, getFieldName(&db, i), INDEX_ANY)) {
            have_index = 1;
        }

        fprintf(
            output,
            "%s\t%c\n",
            getFieldName(&db, i),
            have_index ? 'Y' : 'N'
        );
    }

    closeDB(&db);

    return 0;
}

static void destroy_query (struct Query *query) {
    if (query->predicate_nodes != NULL) {
        free(query->predicate_nodes);
        query->predicate_nodes = NULL;
    }

    if (query->tables != NULL) {
        free(query->tables);
        query->tables = NULL;
    }

    for (int i = 0; i < query->column_count; i++) {
        freeNode((struct Node *)&query->columns[i]);
    }

    for (int i = 0; i < query->order_count; i++) {
        freeNode(&query->order_nodes[i]);
    }

    for (int i = 0; i < query->group_count; i++) {
        freeNode(&query->group_nodes[i]);
    }
}

static int populate_tables (struct Query *q, struct DB *dbs) {

    for (int i = 0; i < q->table_count; i++) {
        struct Table *table = &q->tables[i];

        int found = 0;

        // Perform a sanity check first

        if (table->db != NULL && table->db != DB_SUBQUERY) {
            // DB has already been opened for us. This is most probably a
            // VALUES "subquery". We need to copy the db to our stack then free
            // the previously opened DB.

            memcpy(&dbs[i], table->db, sizeof(dbs[i]));

            free(table->db);

            table->db = &dbs[i];

            found = 1;
        }

        if (found == 0) {
            // Try to reuse existing open table next
            for (int j = 0; j < i; j++) {
                if (strcmp(q->tables[j].name, table->name) == 0) {
                    // Copy pointer
                    table->db = q->tables[j].db;

                    // Make actual copy of DB for output functions
                    memcpy(&dbs[i], &dbs[j], sizeof (dbs[i]));

                    found = 1;
                    break;
                }
            }
        }

        // Handle special kinds of table, info provided by parser

        // Check for subquery first
        if (found == 0 && table->db == DB_SUBQUERY) {
            char filename[MAX_TABLE_LENGTH];

            int result = select_subquery(table->name, filename);
            if (result < 0) {
                remove(filename);
                return -1;
            }

            struct DB *db = &dbs[i];

            // hand off to CSV Mem
            result = csvMem_openDB(db, filename);

            remove(filename);

            if (result < 0) {
                return -1;
            }

            table->db = &dbs[i];

            found = 1;
        }

        // Must be a regular table
        // Not a special table and not already open
        if (found == 0) {
            if (openDB(&dbs[i], table->name) != 0) {
                fprintf(stderr, "Unable to use file: '%s'\n", table->name);
                return -1;
            }

            table->db = &dbs[i];
        }

        check_column_aliases(table);

        int result;

        if (
            table->join.function != OPERATOR_ALWAYS
            && table->join.function != FUNC_UNITY
        ) {
            result = resolveNode(q, &table->join.children[0], 0);
            if (result < 0) {
                return result;
            }
            result = resolveNode(q, &table->join.children[1], 0);
            if (result < 0) {
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
static int find_field (
    struct Query *q,
    const char *text,
    int *table_id,
    int *column_id,
    char *name
) {

    int dot_index = str_find_index(text, '.');

    if (*table_id != -1) {
        // table_id has already been filled in for us as a hint,
        // we'll search for the column on that table

        if (text[dot_index + 1] == '*') {
            *column_id = FIELD_STAR;
        }
        else if (strcmp(text + dot_index + 1, "rowid") == 0) {
            *column_id = FIELD_ROW_INDEX;
        }
        else {
            struct DB *db = q->tables[*table_id].db;

            *column_id = getFieldIndex(db, text + dot_index + 1);
        }

        return 1;
    }

    if (dot_index >= 0) {
        char value[MAX_FIELD_LENGTH];

        strncpy(value, text, dot_index);
        value[dot_index] = '\0';

        for (int i = 0; i < q->table_count; i++) {
            if (strcmp(q->tables[i].name, value) == 0 ||
                strcmp(q->tables[i].alias, value) == 0)
            {
                *table_id = i;

                if (text[dot_index + 1] == '*') {
                    *column_id = FIELD_STAR;
                }
                else if (strcmp(text + dot_index + 1, "rowid") == 0) {
                    *column_id = FIELD_ROW_INDEX;
                }
                else {
                    struct DB *db = q->tables[i].db;

                    *column_id = getFieldIndex(db, text + dot_index + 1);

                    if (*column_id != -1 && name != NULL) {
                        // could use getFieldName() but probably not necessary
                        strcpy_overlap(name, text + dot_index + 1);
                    }
                }

                return 1;
            }
        }
    }
    else {
        if (strcmp(text, "rowid") == 0) {
            // Default to first table
            *table_id = 0;
            *column_id = FIELD_ROW_INDEX;

            return 1;
        }

        for (int i = 0; i < q->table_count; i++) {
            struct DB *db = q->tables[i].db;

            *column_id = getFieldIndex(db, text);

            if (*column_id != FIELD_UNKNOWN) {
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
 * @returns 0 on success
 */
int resolveNode (struct Query *query, struct Node *node, int allow_aliases) {
    if (node->child_count > 0) {
        for (int i = 0; i < node->child_count; i++) {
            int result = resolveNode(query, &node->children[i], allow_aliases);
            if (result < 0) return -1;
        }
        return 0;
    }

    // Check for aliases first
    if (
        allow_aliases
        && node->function == FUNC_UNITY
        && node->field.index == FIELD_UNKNOWN
        && node->field.text[0] != '\0'
    ) {
        for (int i = 0; i < query->column_count; i++) {
            if (
                node != (struct Node *)&query->columns[i]
                && strcmp(node->field.text, query->columns[i].alias) == 0
            ) {
                copyNodeTree(node, (struct Node *)&query->columns[i]);
                return 0;
            }
        }
    }

    if (node->field.index == FIELD_CONSTANT) {
        evaluateConstantField(node->field.text, &node->field);
    }
    else if (
        node->field.index == FIELD_UNKNOWN
        && node->field.text[0] != '\0'
    ) {
        if (!find_field(
                query,
                node->field.text,
                &node->field.table_id,
                &node->field.index,
                node->field.text
            )
        ) {
            fprintf(stderr, "Unable to find column '%s'\n", node->field.text);
            return -1;
        }
    }

    return 0;
}

/**
 * @brief Support column aliasing in FROM clause
 * e.g. `FROM table (alias1, alias2)`
 *
 * @param table
 */
static void check_column_aliases (struct Table * table) {
    int alias_len = strlen(table->alias);

    if (table->alias[alias_len + 1] == '(') {
        char * c = table->alias + alias_len + 2;
        char * d = table->db->fields;
        while (*c != '\0') {
            if (*c == ','){
                *d++ = '\0';
            }
            else if (*c == ' '){
                // No-op
            }
            else if (*c == ')') {
                break;
            }
            else {
                *d++ = *c;
            }
            c++;
        }
        *d = '\0';
    }
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
static int wrap_query (
    struct Query *query,
    enum OutputOption inner_options,
    enum OutputOption outer_options,
    FILE *output
) {
    struct Query q2 = {0};
    struct Table table = {0};

    int result = process_subquery(query, inner_options, table.name);
    if (result < 0) {
        return result;
    }

    q2.table_count = 1;
    q2.tables = &table;
    q2.limit_value = -1;

    result = process_query(
        &q2,
        outer_options,
        output
    );

    remove(table.name);

    q2.tables = NULL;
    destroy_query(&q2);

    return result;
}

/**
 * @brief Copies a node tree recursively to avoid double FREE
 * Will malloc for children
 *
 * @param dest
 * @param src
 * @return void
 */
void copyNodeTree (struct Node *dest, struct Node *src) {
    memcpy(&dest->field, &src->field, sizeof(dest->field));

    dest->function = src->function;

    dest->child_count = src->child_count;

    dest->children = NULL;

    if (src->children != NULL && src->child_count > 0) {

        dest->children = malloc(sizeof(*dest) * src->child_count);

        for (int i = 0; i < src->child_count; i++) {
            copyNodeTree(&dest->children[i], &src->children[i]);
        }
    }
}

void freeNode (struct Node *node) {
    if (node->child_count > 0) {
        for (int i = 0; i < node->child_count; i++) {
            freeNode(&node->children[i]);
        }
    }

    if (node->children != NULL) {
        free(node->children);
        node->children = NULL;
    }
}
