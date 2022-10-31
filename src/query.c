#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include <unistd.h>

#include "query.h"
#include "execute.h"
#include "create.h"
#include "parse.h"
#include "explain.h"
#include "plan.h"
#include "db.h"
#include "db-csv-mem.h"
#include "util.h"

int basic_select_query (struct Query *q, struct Plan *plan, enum OutputOption output_flags, FILE * output);

int information_query (const char *table, FILE * output);

static int populateTables (struct Query *q, struct DB * dbs);

static int findColumn (struct Query *q, const char *text, int *table_id, int *column_id);

static void checkColumnAliases (struct Table * table);

static int process_query (struct Query *q, enum OutputOption output_flags, FILE * output);

extern char *process_name;

int query (const char *query, enum OutputOption output_flags, FILE * output) {
    #ifdef DEBUG
    fprintf(stderr, "Start Query (%d)\n", getpid());
    #endif

    if (strncmp(query, "CREATE ", 7) == 0) {
        if (output_flags & FLAG_READ_ONLY) {
            fprintf(stderr, "Tried to CREATE while in read-only mode\n");
            return -1;

        }
        return create_query(query);
    }

    if (strncmp(query, "INSERT ", 7) == 0) {
        if (output_flags & FLAG_READ_ONLY) {
            fprintf(stderr, "Tried to INSERT while in read-only mode\n");
            return -1;
        }

        return insert_query(query);
    }

    int format = output_flags & OUTPUT_MASK_FORMAT;
    int is_escaped_output = format == OUTPUT_FORMAT_JSON
        || format == OUTPUT_FORMAT_JSON_ARRAY
        || format == OUTPUT_FORMAT_SQL_INSERT
        || format == OUTPUT_FORMAT_XML
        || format == OUTPUT_FORMAT_TABLE;

    // In order to support concat for these output formats
    // we wrap the whole query in a subquery
    if (is_escaped_output && strstr(query, "||") != NULL)
    {
        char query2[1024];
        sprintf(query2, "FROM (%s)", query);

        return select_query(query2, output_flags, output);
    }

    // In order to support different output formats for EXPLAIN we wrap the
    // whole query in a subquery. This EXPLAIN has come from the command line.
    // (There could also be an EXPLAIN keyword in the query which will probably
    // confuse the parser.)
    if (format != OUTPUT_FORMAT_COMMA && output_flags & FLAG_EXPLAIN)
    {
        char query2[1024];
        sprintf(query2, "FROM (EXPLAIN %s)", query);

        output_flags &= ~FLAG_EXPLAIN;

        return select_query(query2, output_flags, output);
    }

    return select_query(query, output_flags, output);
}

int select_query (const char *query, enum OutputOption output_flags, FILE * output) {
    struct Query q = {0};

    // fprintf(stderr, "sizeof(struct Query) = %ld\n", sizeof(q));

    if (parseQuery(&q, query) < 0) {
        fprintf(stderr, "Parsing query\n");
        return -1;
    }

    int format = output_flags & OUTPUT_MASK_FORMAT;
    // In order to support different output formats for EXPLAIN we wrap the
    // whole query in a subquery. This EXPLAIN has come from the start of the
    // query.
    if (format != OUTPUT_FORMAT_COMMA && q.flags & FLAG_EXPLAIN)
    {
        char query2[1024];
        sprintf(query2, "FROM (%s)", query);

        output_flags &= ~FLAG_EXPLAIN;

        return select_query(query2, output_flags, output);
    }

    // Cannot group and sort in the same query.
    if (q.flags & FLAG_GROUP && q.flags & FLAG_ORDER)
    {
        // We will materialise the GROUP'd query to disk then sort that

        char tmpfile_name[255];
        sprintf(tmpfile_name, "/tmp/csvdb.%d-%d.csv", getpid(), rand());
        FILE *tmpfile = fopen(tmpfile_name, "w");

        struct Query q2 = q;
        q2.flags &= ~FLAG_ORDER;
        q2.order_count = 0;

        int result = process_query(&q2, OUTPUT_OPTION_HEADERS | OUTPUT_FORMAT_COMMA, tmpfile);

        fclose(tmpfile);

        if (result < 0) {
            return -1;
        }

        if (q.order_node[0].function != FUNC_UNITY) {
            fprintf(stderr, "Cannot do ORDER BY when GROUP BY uses a function\n");
            return -1;
        }

        char query2[1024];
        int len = sprintf(query2, "FROM \"%s\" ORDER BY %s %s", tmpfile_name, q.order_node[0].fields[0].text, q.order_direction[0] == ORDER_ASC ? "ASC" : "DESC");
        char *c = query2 + len;
        for (int i = 1; i < q.order_count; i++) {
            if (q.order_node[i].function != FUNC_UNITY) {
                fprintf(stderr, "Cannot do ORDER BY when GROUP BY uses a function\n");
                return -1;
            }
            len = sprintf(c, ", %s %s", q.order_node[i].fields[0].text, q.order_direction[i] == ORDER_ASC ? "ASC" : "DESC");
            c += len;
        }

        result = select_query(query2, output_flags, output);

        remove(tmpfile_name);

        return result;
    }

    return process_query(&q, output_flags, output);
}

void destroyQuery (struct Query *query) {
    if (query->predicate_count > 0) {
        free(query->predicates);
        query->predicates = NULL;
    }

    if (query->table_count > 0) {
        free(query->tables);
    }
}

static int process_query (struct Query *q, enum OutputOption output_flags, FILE * output)
{
    int result;

    // Explain can be specified on the command line so copy that value in jsut
    // in case.
    if (output_flags & FLAG_EXPLAIN) {
        q->flags |= FLAG_EXPLAIN;
    }

    if (q->table_count == 0) {
        // No table was specified.
        // However, if stdin is something more than a tty (i.e pipe or redirected file)
        // then we can default to it.
        if (!isatty(fileno(stdin))) {
            q->tables = calloc(1, sizeof (struct Table));
            q->table_count = 1;
            strcpy(q->tables[0].name, "stdin");
            strcpy(q->tables[0].alias, "stdin");
        }
        else {
            // We could have a constant query which will output a single row
            // Check if any of the fields are non-constant and abort

            // TODO: now that there are multi-field columns we need to check
            // *all* fields .
            for (int i = 0; i < q->column_count; i++) {
                struct Field * field = q->columns[0].fields;
                if (field->index != FIELD_CONSTANT) {
                    fprintf(stderr, "No Tables specified\n");
                    return -1;
                }
            }
        }
    }
    else if (strcmp(q->tables[0].name, "INFORMATION") == 0) {
        if (q->predicate_count < 1) {
            return -1;
        }

        q->tables[0].db = NULL;

        result = information_query(q->predicates[0].right.fields[0].text, output);
        destroyQuery(q);
        return result;
    }

    /*************************
     * Begin Query processing
     *************************/

    // Create array on stack to hold DB structs
    struct DB dbs[MAX_TABLE_COUNT];

    // Populate Tables
    // (including JOIN predicate columns)
    result = populateTables(q, dbs);
    if (result < 0) {
        return result;
    }

    // Populate SELECT Columns
    for (int i = 0; i < q->column_count; i++) {
        result = populateColumnNode(q, &q->columns[i]);
        if (result < 0) {
            return result;
        }
    }

    // Populate WHERE columns
    for (int i = 0; i < q->predicate_count; i++) {
        result = populateColumnNode(q, &q->predicates[i].left);
        if (result < 0) {
            return result;
        }
        result = populateColumnNode(q, &q->predicates[i].right);
        if (result < 0) {
            return result;
        }
    }

    /**********************
     * Make Plan
     **********************/
    struct Plan plan;

    makePlan(q, &plan);

    if (q->flags & FLAG_EXPLAIN) {
        result =  explain_select_query(q, &plan, output_flags, output);
        destroyQuery(q);
        destroyPlan(&plan);
        return result;
    }

    result = executeQueryPlan(q, &plan, output_flags, output);
    destroyQuery(q);
    destroyPlan(&plan);
    return result;
}

int information_query (const char *table, FILE * output) {
    struct DB db;

    if (openDB(&db, table) != 0) {
        fprintf(stderr, "File not found: '%s'\n", table);
        return -1;
    }

    fprintf(output, "Table:\t%s\n", table);
    fprintf(output, "Fields:\t%d\n", db.field_count);
    fprintf(output, "Records:\t%d\n", db.record_count);

    fprintf(output, "\n");

    fprintf(output, "field\tindex\n");
    fprintf(output, "-----\t-----\n");

    for (int i = 0; i < db.field_count; i++) {
        int have_index = 0;

        if (findIndex(NULL, table, getFieldName(&db, i), INDEX_ANY)) {
            have_index = 1;
        }

        fprintf(output, "%s\t%c\n", getFieldName(&db, i), have_index ? 'Y' : 'N');
    }

    closeDB(&db);

    return 0;
}

static int populateTables (struct Query *q, struct DB *dbs) {

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

        if (found == 0) {
            // Check for subquery first
            if (table->db == DB_SUBQUERY) {
                char *cmd = malloc(MAX_TABLE_LENGTH * 2);

                // Construct command line for sub-process
                sprintf(cmd, "%s -0 -H -F csv \"%s\"", process_name, table->name);

                FILE *f = popen(cmd, "r");
                free(cmd);

                struct DB *db = &dbs[i];

                // Leave a note for csvMem to close the stream
                db->file = STREAM_PROC;

                // hand off to CSV Mem
                int result = csvMem_makeDB(db, f);

                if (result < 0) {
                    return -1;
                }

                table->db = &dbs[i];

                found = 1;
            }
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

        checkColumnAliases(table);

        int result;

        if (table->join.op != OPERATOR_ALWAYS) {
            result = populateColumnNode(q, &table->join.left);
            if (result < 0) {
                return result;
            }
            result = populateColumnNode(q, &table->join.right);
            if (result < 0) {
                return result;
            }
        }
    }

    return 0;
}

static int findColumn (struct Query *q, const char *text, int *table_id, int *column_id) {

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
 * Will resolve a column name to a table_id and column_id.
 * Safe to call multiple times on same column.
 *
 * @returns 0 on success
 */
int populateColumnNode (struct Query * query, struct ColumnNode * column) {
    struct Field * field1 = column->fields + 0;
    struct Field * field2 = column->fields + 1;

    if (field1->index == FIELD_UNKNOWN && field1->text[0] != '\0') {
        if (!findColumn(query, field1->text, &field1->table_id, &field1->index)) {
            fprintf(stderr, "Unable to find column '%s'\n", field1->text);
            return -1;
        }
    }

    if (field2->index == FIELD_UNKNOWN && field2->text[0] != '\0') {
        if (!findColumn(query, field2->text, &field2->table_id, &field2->index)) {
            fprintf(stderr, "Unable to find column '%s'\n", field2->text);
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
static void checkColumnAliases (struct Table * table) {
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