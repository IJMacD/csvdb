#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include <unistd.h>

#include "query.h"
#include "db.h"
#include "parse.h"
#include "predicates.h"
#include "function.h"
#include "indices.h"
#include "filter.h"
#include "sort.h"
#include "output.h"
#include "limits.h"
#include "create.h"
#include "explain.h"
#include "util.h"
#include "plan.h"
#include "result.h"
#include "debug.h"

int select_query (const char *query, int output_flags, FILE * output);

int basic_select_query (struct Query *q, struct Plan *plan, int output_flags, FILE * output);

int information_query (const char *table, FILE * output);

static int populateTables (struct Query *q, struct DB * dbs);

static int populateColumns (struct Query *q);

int query (const char *query, int output_flags, FILE * output) {
    if (strncmp(query, "CREATE ", 7) == 0) {
        return create_query(query);
    }

    return select_query(query, output_flags, output);
}

int select_query (const char *query, int output_flags, FILE * output) {
    int result;
    struct Query q = {0};

    if (parseQuery(&q, query) < 0) {
        fprintf(stderr, "Error parsing query\n");
        return -1;
    }

    // Explain can be specified on the command line
    if (output_flags & FLAG_EXPLAIN) {
        q.flags |= FLAG_EXPLAIN;
    }

    if (q.table_count == 0) {
        // No table was specified.
        // However, if stdin is something more than a tty (i.e pipe or redirected file)
        // then we can default to it.
        if (!isatty(fileno(stdin))) {
            q.tables = malloc(sizeof (struct Table));
            q.table_count = 1;
            strcpy(q.tables[0].name, "stdin");
        }
        else {
            // We could have a constant query which will output a single row

            int all_constant = 1;
            for (int i = 0; i < q.column_count; i++) {
                if (q.columns[i].field != FIELD_CONSTANT) {
                    all_constant = 0;
                    break;
                }
            }

            if (all_constant) {
                struct Plan plan;
                plan.step_count = 1;

                struct PlanStep * step = plan.steps;
                step->type = PLAN_SELECT;
                step->predicate_count = 0;

                result = basic_select_query(&q, &plan, output_flags, output);
                destroyQuery(&q);
                return result;
            } else {
                fprintf(stderr, "Table not specified\n");
                return -1;
            }
        }
    }

    if (strcmp(q.tables[0].name, "INFORMATION") == 0) {
        if (q.predicate_count < 1) {
            return -1;
        }

        result = information_query(q.predicates[0].right.text, output);
        destroyQuery(&q);
        return result;
    }

    /*************************
     * Begin Query processing
     *************************/

    struct DB dbs[TABLE_MAX_COUNT];

    if (q.table_count == 0) {
        fprintf(stderr, "No tables\n");
        exit(-1);
    }

    // Populate tables
    result = populateTables(&q, dbs);
    if (result < 0) {
        return result;
    }

    // Populate predicate columns
    for (int i = 0; i < q.predicate_count; i++) {
        populateColumnNode(&q, &q.predicates[i].left);
        populateColumnNode(&q, &q.predicates[i].right);
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
    struct DB *dbs = NULL;

    // We know that the dbs storage is on the stack and starts at the db
    // pointed to by the first table.
    if (q->table_count > 0) {
        dbs = q->tables[0].db;
    }

    /*************************
     * Output headers
     ************************/
    printPreamble(output, NULL, q->columns, q->column_count, output_flags);

    if (output_flags & OUTPUT_OPTION_HEADERS) {
        printHeaderLine(output, dbs, q->table_count, q->columns, q->column_count, output_flags);
    }

    struct ResultSet results;

    results.list_count = 1;

    struct RowList row_list;

    results.row_lists = &row_list;

    if (q->table_count == 0) {
        // Just a single output row
        makeRowList(&row_list, 0, 1);
    } else {
        // Provision enough result space for maximum of all rows in first table
        makeRowList(&row_list, 1, q->tables[0].db->record_count);
    }

    for (int i = 0; i < plan->step_count; i++) {
        struct PlanStep s = plan->steps[i];

        if (s.type == PLAN_PK_UNIQUE || s.type == PLAN_PK_RANGE) {
            // First table
            struct Table * table = q->tables;
            struct Predicate p = s.predicates[0];
            primaryKeyScan(table->db, p.left.text, p.op, p.right.text, &row_list);
        }
        else if (s.type == PLAN_INDEX_UNIQUE) {
            // First table
            struct Table * table = q->tables;
            struct Predicate p = s.predicates[0];
            indexUniqueScan(table->name, p.left.text, p.op, p.right.text, &row_list);
        }
        else if (s.type == PLAN_INDEX_RANGE) {
            // First table
            struct Table * table = q->tables;
            struct Predicate p = s.predicates[0];
            indexRangeScan(table->name, p.left.text, p.op, p.right.text, &row_list);
        }
        else if (s.type == PLAN_TABLE_ACCESS_FULL) {
            // First table
            struct Table * table = q->tables;

            // Optmisation: If we have a limit and no sorting just stop
            // fetching rows after we reach the limit.
            // Should this be done in the plan?
            int limit = -1;
            if (!(q->flags & FLAG_ORDER)) {
                limit = q->limit_value + q->offset_value;
            }

            if (s.predicate_count > 0) {
                fullTableScan(table->db, &row_list, s.predicates, s.predicate_count, limit);
            }
            else {
                fullTableAccess(table->db, &row_list, limit);
            }
        }
        else if (s.type == PLAN_TABLE_ACCESS_ROWID) {
            for (int i = 0; i < s.predicate_count; i++) {
                struct Predicate p = s.predicates[i];
                filterRows(q, &row_list, &p, &row_list);
            }
        }
        else if (s.type == PLAN_CROSS_JOIN) {
            struct RowList new_list;

            struct DB *next_db = q->tables[row_list.join_count].db;

            int new_length = row_list.row_count * next_db->record_count;

            makeRowList(&new_list, row_list.join_count + 1, new_length);

            for (int i = 0; i < row_list.row_count; i++) {
                for (int j = 0; j < next_db->record_count; j++) {
                    appendJoinedRowID(&new_list, &row_list, i, j);
                }
            }

            copyRowList(&row_list, &new_list);
        }
        else if (s.type == PLAN_CONSTANT_JOIN) {
            // Sanity check
            struct Predicate *p = s.predicates + 0;
            if (p->left.table_id != row_list.join_count &&
                p->right.table_id != row_list.join_count)
            {
                fprintf(stderr, "Cannot perform constant join (Join Index: %d)\n", row_list.join_count);
                exit(-1);
            }

            struct DB *next_db = q->tables[row_list.join_count].db;

            struct RowList tmp_list;

            makeRowList(&tmp_list, 1, next_db->record_count);

            // This is a constant join so we'll just populate the table once
            // Hopefully it won't be the whole table since we have a predicate
            fullTableScan(next_db, &tmp_list, s.predicates, s.predicate_count, -1);

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

            copyRowList(&row_list, &new_list);

            destroyRowList(&tmp_list);
        }
        else if (s.type == PLAN_INNER_JOIN) {
            struct DB *next_db = q->tables[row_list.join_count].db;

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
                struct Predicate p = s.predicates[0];

                // Fill in value as constant from outer tables
                if (p.left.table_id < table_id) {
                    evaluateNode(q, &row_list, i, &p.left, p.left.text, FIELD_MAX_LENGTH);
                    p.left.field = FIELD_CONSTANT;
                }

                // Fill in value as constant from outer tables
                if (p.right.table_id < table_id) {
                    evaluateNode(q, &row_list, i, &p.right, p.right.text, FIELD_MAX_LENGTH);
                    p.right.field = FIELD_CONSTANT;
                }

                tmp_list.row_count = 0;

                // Populate the temp list with all rows which match our special predicate
                fullTableScan(next_db, &tmp_list, &p, 1, -1);

                // Append each row we've just found to the main row_list
                for (int j = 0; j < tmp_list.row_count; j++) {
                    int rowid = getRowID(&tmp_list, 0, j);
                    appendJoinedRowID(&new_list, &row_list, i, rowid);
                }
            }

            copyRowList(&row_list, &new_list);

            destroyRowList(&tmp_list);
        }
        else if (s.type == PLAN_SORT) {

            int table_id = -1;
            int field_index;

            findColumn(q, s.predicates[0].left.text, &table_id, &field_index);

            if (field_index == FIELD_UNKNOWN) {
                fprintf(stderr, "Sort column not found: %s\n", s.predicates[0].left.text);
                exit(-1);
            }

            // debugRowList(&row_list);

            struct DB *db = q->tables[table_id].db;

            struct RowList tmp;

            makeRowList(&tmp, row_list.join_count, row_list.row_count);

            sortResultRows(db, table_id, field_index, s.predicates[0].op, &row_list, &tmp);

            copyRowList(&row_list, &tmp);

            // debugRowList(&row_list);
        }
        else if (s.type == PLAN_REVERSE) {
            if (row_list.join_count > 1) {
                fprintf(stderr, "Not Implemented: Unable to reverse joined rows\n");
                exit(-1);
            }

            reverse_array(row_list.row_ids, row_list.row_count);
        }
        else if (s.type == PLAN_SLICE) {
            // s.param1 is offset
            // s.param2 is limit

            // Offset is taken care of in PLAN_SELECT

            // Apply limit (including offset rows - which will be omitted later)
            if (s.param2 >= 0 && s.param2 < row_list.row_count) {
                row_list.row_count = s.param1 + s.param2;
            }
        }
        else if (s.type == PLAN_GROUP) {
            // NOP
        }
        else if (s.type == PLAN_SELECT) {
            /*******************
             * Output result set
             *******************/

            int result = populateColumns(q);
            if (result < 0) {
                return result;
            }

            // Aggregate functions will print just one row
            if (q->flags & FLAG_GROUP) {
                printResultLine(output, dbs, q->table_count, q->columns, q->column_count, row_list.row_count > 0 ? q->offset_value : RESULT_NO_ROWS, &row_list, output_flags);
            }
            else for (int i = q->offset_value; i < row_list.row_count; i++) {
                printResultLine(output, dbs, q->table_count, q->columns, q->column_count, i, &row_list, output_flags);
            }
        }
        else {
            fprintf(stderr, "Unimplemented OP code: %d\n", s.type);
            return -1;
        }
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

    struct DB index_db;

    for (int i = 0; i < db.field_count; i++) {
        int have_index = 0;

        if (findIndex(&index_db, table, getFieldName(&db, i), INDEX_ANY) == 0) {
            have_index = 1;
            closeDB(&index_db);
        }

        fprintf(output, "%s\t%c\n", getFieldName(&db, i), have_index ? 'Y' : 'N');
    }

    closeDB(&db);

    return 0;
}

static int populateColumns (struct Query * q) {
    // Fill in selected table id and column indexes
    for (int i = 0; i < q->column_count; i++) {
        struct ColumnNode *column = &(q->columns[i]);

        if (column->field == FIELD_UNKNOWN) {
            findColumn(q, column->text, &column->table_id, &column->field);

            if (column->field == FIELD_UNKNOWN) {
                fprintf(stderr, "Field '%s' not found\n", column->text);
                return -1;
            }
        }
    }

    return 0;
}

static int populateTables (struct Query *q, struct DB *dbs) {

    for (int i = 0; i < q->table_count; i++) {
        struct Table *table = &q->tables[i];

        int found = 0;

        // Try to reuse existing open table
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

        if (found == 1) {
            continue;
        }

        if (openDB(&dbs[i], table->name) != 0) {
            fprintf(stderr, "File not found: '%s'\n", table->name);
            return -1;
        }

        table->db = &dbs[i];
    }

    return 0;
}

void findColumn (struct Query *q, const char *text, int *table_id, int *column_id) {

    int dot_index = str_find_index(text, '.');

    if (dot_index >= 0) {
        char value[FIELD_MAX_LENGTH];

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

                return;
            }
        }
    }
    else {
        if (strcmp(text, "rowid") == 0) {
            // Default to first table
            *table_id = 0;
            *column_id = FIELD_ROW_INDEX;

            return;
        }

        for (int i = 0; i < q->table_count; i++) {
            struct DB *db = q->tables[i].db;

            *column_id = getFieldIndex(db, text);

            if (*column_id != FIELD_UNKNOWN) {
                *table_id = i;

                return;
            }
        }
    }

    // Couldn't find column
    *column_id = FIELD_UNKNOWN;
}

void populateColumnNode (struct Query * query, struct ColumnNode * column) {
    if (column->field == FIELD_UNKNOWN) {
        findColumn(query, column->text, &column->table_id, &column->field);
    }
}

int evaluateNode (struct Query * query, struct RowList *rowlist, int index, struct ColumnNode * column, char * value, int max_length) {
    if (column->field == FIELD_ROW_INDEX) {
        sprintf(value, "%d", index);
    } else if (column->field == FIELD_CONSTANT) {
        strcpy(value, column->text);
    } else if (column->field >= 0) {
        int row_id_left = getRowID(rowlist, column->table_id, index);
        getRecordValue(query->tables[column->table_id].db, row_id_left, column->field, value, max_length);
    } else {
        fprintf(stderr, "Cannot evaluate predicate column: %s\n", column->text);
        exit(-1);
    }

    return 0;
}