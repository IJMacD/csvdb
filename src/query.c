#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include <unistd.h>

#include "query.h"
#include "db.h"
#include "db-csv-mem.h"
#include "parse.h"
#include "predicates.h"
#include "function.h"
#include "indices.h"
#include "evaluate.h"
#include "sort.h"
#include "output.h"
#include "limits.h"
#include "create.h"
#include "explain.h"
#include "util.h"
#include "plan.h"
#include "result.h"
#include "debug.h"
#include "date.h"

int basic_select_query (struct Query *q, struct Plan *plan, int output_flags, FILE * output);

int information_query (const char *table, FILE * output);

static int populateTables (struct Query *q, struct DB * dbs);

static int populateColumnNode (struct Query * query, struct ColumnNode * column);

static int findColumn (struct Query *q, const char *text, int *table_id, int *column_id);

static void checkColumnAliases (struct Table * table);

extern char *process_name;

int query (const char *query, int output_flags, FILE * output) {
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

int select_query (const char *query, int output_flags, FILE * output) {
    int result;
    struct Query q = {0};

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

    // Explain can be specified on the command line so copy that value in jsut
    // in case.
    if (output_flags & FLAG_EXPLAIN) {
        q.flags |= FLAG_EXPLAIN;
    }

    if (q.table_count == 0) {
        // No table was specified.
        // However, if stdin is something more than a tty (i.e pipe or redirected file)
        // then we can default to it.
        if (!isatty(fileno(stdin))) {
            q.tables = calloc(1, sizeof (struct Table));
            q.table_count = 1;
            strcpy(q.tables[0].name, "stdin");
            strcpy(q.tables[0].alias, "stdin");
        }
        else {
            // We could have a constant query which will output a single row
            // Check if any of the fields are non-constant and abort

            // TODO: now that there are multi-field columns we need to check
            // *all* fields .
            for (int i = 0; i < q.column_count; i++) {
                struct Field * field = q.columns[0].fields;
                if (field->index != FIELD_CONSTANT) {
                    fprintf(stderr, "No Tables specified\n");
                    return -1;
                }
            }
        }
    }
    else if (strcmp(q.tables[0].name, "INFORMATION") == 0) {
        if (q.predicate_count < 1) {
            return -1;
        }

        q.tables[0].db = NULL;

        result = information_query(q.predicates[0].right.fields[0].text, output);
        destroyQuery(&q);
        return result;
    }

    /*************************
     * Begin Query processing
     *************************/

    // Create array on stack to hold DB structs
    struct DB dbs[MAX_TABLE_COUNT];

    // Populate Tables
    // (including JOIN predicate columns)
    result = populateTables(&q, dbs);
    if (result < 0) {
        return result;
    }

    // Populate SELECT Columns
    for (int i = 0; i < q.column_count; i++) {
        result = populateColumnNode(&q, &q.columns[i]);
        if (result < 0) {
            return result;
        }
    }

    // Populate WHERE columns
    for (int i = 0; i < q.predicate_count; i++) {
        result = populateColumnNode(&q, &q.predicates[i].left);
        if (result < 0) {
            return result;
        }
        result = populateColumnNode(&q, &q.predicates[i].right);
        if (result < 0) {
            return result;
        }
    }

    /**********************
     * Make Plan
     **********************/
    struct Plan plan;

    makePlan(&q, &plan);

    if (q.flags & FLAG_EXPLAIN) {
        result =  explain_select_query(&q, &plan, output_flags, output);
        destroyQuery(&q);
        return result;
    }

    result = basic_select_query(&q, &plan, output_flags, output);
    destroyQuery(&q);
    return result;
}

int basic_select_query (
    struct Query *q,
    struct Plan *plan,
    int output_flags,
    FILE * output
) {
    /*************************
     * Output headers
     ************************/
    printPreamble(output, NULL, q->columns, q->column_count, output_flags);

    if (output_flags & OUTPUT_OPTION_HEADERS) {
        printHeaderLine(output, q->tables, q->table_count, q->columns, q->column_count, output_flags);
    }

    // struct ResultSet results;

    // results.list_count = 1;

    struct RowList row_list;

    // results.row_lists = &row_list;

    if (q->table_count == 0) {
        // Just a single output row
        makeRowList(&row_list, 0, 0);
        row_list.row_count = 1;
    } else {
        // Provision enough result space for maximum of all rows in first table
        makeRowList(&row_list, 1, q->tables[0].db->record_count);
    }

    for (int i = 0; i < plan->step_count; i++) {
        struct PlanStep *s = &plan->steps[i];

        if (s->type == PLAN_PK || s->type == PLAN_PK_RANGE) {
            #ifdef DEBUG
            fprintf(stderr, "Q%d: PLAN_PK\n", getpid());
            #endif
            // First table
            struct Table * table = q->tables;
            struct Predicate *p = &s->predicates[0];
            indexPrimaryScan(table->db, p->op, p->right.fields[0].text, &row_list, s->limit);
        }
        else if (s->type == PLAN_UNIQUE || s->type == PLAN_UNIQUE_RANGE) {
            #ifdef DEBUG
            fprintf(stderr, "Q%d: PLAN_UNIQUE\n", getpid());
            #endif
            // First table
            struct Table * table = q->tables;
            struct Predicate *p = &s->predicates[0];
            struct DB index_db;
            if (findIndex(&index_db, table->name, p->left.fields[0].text, INDEX_UNIQUE) == 0) {
                fprintf(stderr, "Unable to find unique index on column '%s' on table '%s'\n", p->left.fields[0].text, table->name);
                return -1;
            }
            // Find which column in the index table contains the rowids of the primary table
            int rowid_col = getFieldIndex(&index_db, "rowid");
            indexUniqueScan(&index_db, rowid_col, p->op, p->right.fields[0].text, &row_list, s->limit);
        }
        else if (s->type == PLAN_INDEX_RANGE) {
            #ifdef DEBUG
            fprintf(stderr, "Q%d: PLAN_INDEX_RANGE\n", getpid());
            #endif
            // First table
            struct Table * table = q->tables;
            struct Predicate *p = &s->predicates[0];
            struct DB index_db;
            if (findIndex(&index_db, table->name, p->left.fields[0].text, INDEX_ANY) == 0) {
                fprintf(stderr, "Unable to find index on column '%s' on table '%s'\n", p->left.fields[0].text, table->name);
                return -1;
            }
            // Find which column in the index table contains the rowids of the primary table
            int rowid_col = getFieldIndex(&index_db, "rowid");
            indexScan(&index_db, rowid_col, p->op, p->right.fields[0].text, &row_list, s->limit);
        }
        else if (s->type == PLAN_TABLE_ACCESS_FULL) {
            #ifdef DEBUG
            fprintf(stderr, "Q%d: PLAN_TABLE_ACCESS_FULL\n", getpid());
            #endif
            // First table
            struct Table * table = q->tables;

            if (s->predicate_count > 0) {
                fullTableScan(table->db, &row_list, s->predicates, s->predicate_count, s->limit);
            }
            else {
                fullTableAccess(table->db, &row_list, s->limit);
            }
        }
        else if (s->type == PLAN_TABLE_ACCESS_ROWID) {
            #ifdef DEBUG
            fprintf(stderr, "Q%d: PLAN_TABLE_ACCESS_ROWID\n", getpid());
            #endif
            int source_count = row_list.row_count;

            row_list.row_count = 0;

            for (int i = 0; i < source_count; i++) {
                int match = 1;

                for (int j = 0; j < s->predicate_count; j++) {
                    struct Predicate * p = s->predicates + j;

                    char value_left[MAX_VALUE_LENGTH] = {0};
                    char value_right[MAX_VALUE_LENGTH] = {0};

                    evaluateNode(q, &row_list, i, &p->left, value_left, MAX_VALUE_LENGTH);
                    evaluateNode(q, &row_list, i, &p->right, value_right, MAX_VALUE_LENGTH);

                    if (!evaluateExpression(p->op, value_left, value_right)) {
                        match = 0;
                        break;
                    }
                }

                if (match) {
                    // Add to result set
                    copyResultRow(&row_list, &row_list, i);
                }
            }
        }
        else if (s->type == PLAN_CROSS_JOIN) {
            #ifdef DEBUG
            fprintf(stderr, "Q%d: PLAN_CROSS_JOIN\n", getpid());
            #endif
            struct RowList new_list;

            struct DB *next_db = q->tables[row_list.join_count].db;

            int new_length = row_list.row_count * next_db->record_count;

            makeRowList(&new_list, row_list.join_count + 1, new_length);

            for (int i = 0; i < row_list.row_count; i++) {
                for (int j = 0; j < next_db->record_count; j++) {
                    appendJoinedRowID(&new_list, &row_list, i, j);
                }
            }

            overwriteRowList(&row_list, &new_list);
        }
        else if (s->type == PLAN_CONSTANT_JOIN) {
            #ifdef DEBUG
            fprintf(stderr, "Q%d: PLAN_CONSTANT_JOIN\n", getpid());
            #endif
            // Sanity check
            struct Predicate *p = s->predicates + 0;
            if (p->left.fields[0].table_id != row_list.join_count &&
                p->right.fields[0].table_id != row_list.join_count)
            {
                fprintf(stderr, "Cannot perform constant join (Join Index: %d)\n", row_list.join_count);
                return -1;
            }

            struct DB *next_db = q->tables[row_list.join_count].db;

            struct RowList tmp_list;

            makeRowList(&tmp_list, 1, next_db->record_count);

            // This is a constant join so we'll just populate the table once
            // Hopefully it won't be the whole table since we have a predicate
            fullTableScan(next_db, &tmp_list, s->predicates, s->predicate_count, -1);

            struct RowList new_list;

            int new_length = row_list.row_count * tmp_list.row_count;

            // Now we know how many rows are to be joined we can make the new row list
            makeRowList(&new_list, row_list.join_count + 1, new_length);

            // For each row in the original row list, join every row of the tmp row list
            for (int i = 0; i < row_list.row_count; i++) {
                for (int j = 0; j < tmp_list.row_count; j++) {
                    int rowid = getRowID(&tmp_list, 0, j);
                    appendJoinedRowID(&new_list, &row_list, i, rowid);
                }
            }

            overwriteRowList(&row_list, &new_list);

            destroyRowList(&tmp_list);
        }
        else if (s->type == PLAN_LOOP_JOIN) {
            #ifdef DEBUG
            fprintf(stderr, "Q%d: PLAN_LOOP_JOIN\n", getpid());
            #endif
            struct Table *table = &q->tables[row_list.join_count];
            struct DB *next_db = table->db;

            int new_length = row_list.row_count * next_db->record_count;

            struct RowList new_list;

            makeRowList(&new_list, row_list.join_count + 1, new_length);

            struct RowList tmp_list;

            // Prepare a temporary list that can hold every record in the table
            makeRowList(&tmp_list, 1, next_db->record_count);

            // Table ID being joined here
            int table_id = row_list.join_count;

            for (int i = 0; i < row_list.row_count; i++) {
                // Make a local copy of predicate
                struct Predicate p = s->predicates[0];

                // Fill in value as constant from outer tables
                if (p.left.fields[0].table_id < table_id) {
                    evaluateNode(q, &row_list, i, &p.left, p.left.fields[0].text, MAX_FIELD_LENGTH);
                    p.left.fields[0].index = FIELD_CONSTANT;
                    p.left.function = FUNC_UNITY;

                    // We're only passing one table to fullTableScan so predicate will be on first table
                    p.right.fields[0].table_id = 0;
                }
                // Fill in value as constant from outer tables
                else if (p.right.fields[0].table_id < table_id) {
                    evaluateNode(q, &row_list, i, &p.right, p.right.fields[0].text, MAX_FIELD_LENGTH);
                    p.right.fields[0].index = FIELD_CONSTANT;
                    p.right.function = FUNC_UNITY;

                    // We're only passing one table to fullTableScan so predicate will be on first table
                    p.left.fields[0].table_id = 0;
                }

                tmp_list.row_count = 0;

                // Populate the temp list with all rows which match our special predicate
                fullTableScan(next_db, &tmp_list, &p, 1, -1);

                // Append each row we've just found to the main row_list
                for (int j = 0; j < tmp_list.row_count; j++) {
                    int rowid = getRowID(&tmp_list, 0, j);
                    appendJoinedRowID(&new_list, &row_list, i, rowid);
                }

                if (table->join_type == JOIN_LEFT && tmp_list.row_count == 0) {
                    // Add NULL to list
                    appendJoinedRowID(&new_list, &row_list, i, ROWID_NULL);
                }
            }

            overwriteRowList(&row_list, &new_list);

            destroyRowList(&tmp_list);
        }
        else if (s->type == PLAN_UNIQUE_JOIN) {
            #ifdef DEBUG
            fprintf(stderr, "Q%d: PLAN_UNIQUE_JOIN\n", getpid());
            #endif
            // Table ID being joined here
            int table_id = row_list.join_count;

            struct Table *table = &q->tables[table_id];

            struct RowList new_list;

            makeRowList(&new_list, row_list.join_count + 1, row_list.row_count);

            struct RowList tmp_list;

            makeRowList(&tmp_list, 1, 1);

            struct Predicate * p = &s->predicates[0];

            struct ColumnNode * outer;
            struct ColumnNode * inner;

            if (p->left.fields[0].table_id == table_id) {
                outer = &p->right;
                inner = &p->left;
            } else if (p->right.fields[0].table_id == table_id) {
                outer = &p->left;
                inner = &p->right;
            } else {
                fprintf(stderr, "Unable to perform UNIQUE JOIN\n");
                return -1;
            }

            if (outer->fields[0].table_id >= table_id) {
                fprintf(stderr, "Unable to perform UNIQUE JOIN\n");
                return -1;
            }

            struct DB index_db = {0};
            if (findIndex(&index_db, q->tables[table_id].name, inner->fields[0].text, INDEX_UNIQUE) == 0) {
                fprintf(stderr, "Couldn't find unique index on '%s(%s)'\n", q->tables[table_id].name, inner->fields[0].text);
                return -1;
            }

            for (int i = 0; i < row_list.row_count; i++) {
                char value[MAX_VALUE_LENGTH];

                // Fill in value as constant from outer tables
                evaluateNode(q, &row_list, i, outer, value, MAX_FIELD_LENGTH);

                // Hang on... won't this only work for calendar?
                int rowid = pkSearch(&index_db, value);

                if (rowid != RESULT_NO_ROWS) {
                    tmp_list.row_count = 0;
                    appendJoinedRowID(&new_list, &row_list, i, rowid);
                }
                else if (table->join_type == JOIN_LEFT) {
                    // Add NULL rowid
                    appendJoinedRowID(&new_list, &row_list, i, ROWID_NULL);
                }
            }

            overwriteRowList(&row_list, &new_list);

            destroyRowList(&tmp_list);
        }
        else if (s->type == PLAN_SORT) {
            #ifdef DEBUG
            fprintf(stderr, "Q%d: PLAN_SORT\n", getpid());
            #endif

            int table_id = -1;
            int field_index;

            if (!findColumn(q, s->predicates[0].left.fields[0].text, &table_id, &field_index)) {
                fprintf(stderr, "Sort column not found: %s\n", s->predicates[0].left.fields[0].text);
                return -1;
            }

            // debugRowList(&row_list, 2);

            struct DB *db = q->tables[table_id].db;

            struct RowList tmp;

            makeRowList(&tmp, row_list.join_count, row_list.row_count);

            sortResultRows(db, table_id, field_index, s->predicates[0].op, &row_list, &tmp);
            // To implement better sort later
            // sortResultRows(db, order_fields, s->predicate_count, &row_list, &tmp);

            overwriteRowList(&row_list, &tmp);

            // debugRowList(&row_list, 2);
        }
        else if (s->type == PLAN_REVERSE) {
            #ifdef DEBUG
            fprintf(stderr, "Q%d: PLAN_REVERSE\n", getpid());
            #endif
            reverseRowList(&row_list);
        }
        else if (s->type == PLAN_SLICE) {
            #ifdef DEBUG
            fprintf(stderr, "Q%d: PLAN_SLICE\n", getpid());
            #endif
            // Offset is taken care of in PLAN_SELECT

            // Apply limit (including offset rows - which will be omitted later)
            if (s->limit >= 0 && s->limit < row_list.row_count) {
                row_list.row_count = s->limit;
            }
        }
        else if (s->type == PLAN_GROUP) {
            #ifdef DEBUG
            fprintf(stderr, "Q%d: PLAN_GROUP\n", getpid());
            #endif
            // NOP
        }
        else if (s->type == PLAN_SELECT) {
            #ifdef DEBUG
            fprintf(stderr, "Q%d: PLAN_SELECT\n", getpid());
            #endif
            /*******************
             * Output result set
             *******************/

            // Aggregate functions will print just one row
            if (q->flags & FLAG_GROUP) {
                printResultLine(output, q->tables, q->table_count, q->columns, q->column_count, row_list.row_count > 0 ? q->offset_value : RESULT_NO_ROWS, &row_list, output_flags);
            }
            else for (int i = q->offset_value; i < row_list.row_count; i++) {
                printResultLine(output, q->tables, q->table_count, q->columns, q->column_count, i, &row_list, output_flags);
            }
        }
        else {
            fprintf(stderr, "Unimplemented OP code: %d\n", s->type);
            return -1;
        }

        #ifdef DEBUG
            debugRowList(&row_list, 1);
        #endif
    }

    printPostamble(output, NULL, q->columns, q->column_count, row_list.row_count, output_flags);

    destroyPlan(plan);

    destroyRowList(&row_list);

    for (int i = 0; i < q->table_count; i++) {
        closeDB(q->tables[i].db);
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
 * Flesh out column nodes in SELECT clause and WHERE clause
 *
 * Will resolve a column name to a table_id and column_id
 *
 * Will also pre-fill constant values so they can be used in comparisons later
 *
 * @returns 0 on success
 */
static int populateColumnNode (struct Query * query, struct ColumnNode * column) {
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

    // RANDOM() is non-pure so cannot be evaluated early
    if (column->function == FUNC_RANDOM) {
        return 0;
    }

    // Pre-populate - Is this necessary?
    // if (field1->index == FIELD_CONSTANT) {
    //     // Fill in constant values such as CURRENT_DATE

    //     evaluateConstantNode(field1, field1->text, MAX_FIELD_LENGTH);
    // }

    // if (field2->index == FIELD_CONSTANT) {
    //     // Fill in constant values such as CURRENT_DATE

    //     evaluateConstantNode(field2, field2->text, MAX_FIELD_LENGTH);
    // }

    // if (field1->index == FIELD_CONSTANT && field2->index == FIELD_CONSTANT) {
    //     // Evaluate any functions on the column

    //     evaluateFunction(field1->text, NULL, column, -1);
    //     column->function = FUNC_UNITY;
    // }

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