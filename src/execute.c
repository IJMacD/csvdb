#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/time.h>
#include <unistd.h>

#include "structs.h"
#include "query.h"
#include "output.h"
#include "result.h"
#include "indices.h"
#include "db.h"
#include "evaluate.h"
#include "predicates.h"
#include "sort.h"
#include "debug.h"

int executeQueryPlan (
    struct Query *q,
    struct Plan *plan,
    enum OutputOption output_flags,
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

    FILE *fstats = NULL;
    struct timeval stop, start;

    if (output_flags & OUTPUT_OPTION_STATS) {
        fstats = fopen("stats.csv", "a");

        gettimeofday(&start, NULL);
    }

    struct ResultSet *result_set = createResultSet();

    int row_count = 0;

    for (int i = 0; i < plan->step_count; i++) {
        struct PlanStep *s = &plan->steps[i];

        switch (s->type) {
            case PLAN_DUMMY_ROW: {
                // Just a single output row
                int row_list = createRowList(0, 0);
                getRowList(row_list)->row_count = 1;
                pushRowList(result_set, row_list);
                break;
            }

            case PLAN_PK:
            case PLAN_PK_RANGE: {
                #ifdef DEBUG
                fprintf(stderr, "Q%d: PLAN_PK\n", getpid());
                #endif

                int record_count = getRecordCount(q->tables[0].db);
                int row_list = createRowList(1, record_count);
                pushRowList(result_set, row_list);

                // First table
                struct Table * table = q->tables;
                struct Predicate *p = &s->predicates[0];
                indexPrimaryScan(table->db, p->op, p->right.fields[0].text, getRowList(row_list), s->limit);
                break;
            }

            case PLAN_UNIQUE:
            case PLAN_UNIQUE_RANGE: {
                #ifdef DEBUG
                fprintf(stderr, "Q%d: PLAN_UNIQUE\n", getpid());
                #endif

                int record_count = getRecordCount(q->tables[0].db);
                int row_list = createRowList(1, record_count);
                pushRowList(result_set, row_list);

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
                indexUniqueScan(&index_db, rowid_col, p->op, p->right.fields[0].text, getRowList(row_list), s->limit);
                break;
            }

            case PLAN_INDEX_RANGE: {
                #ifdef DEBUG
                fprintf(stderr, "Q%d: PLAN_INDEX_RANGE\n", getpid());
                #endif

                int record_count = getRecordCount(q->tables[0].db);
                int row_list = createRowList(1, record_count);
                pushRowList(result_set, row_list);

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
                indexScan(&index_db, rowid_col, p->op, p->right.fields[0].text, getRowList(row_list), s->limit);
                break;
            }

            case PLAN_TABLE_SCAN:
            {
                #ifdef DEBUG
                fprintf(stderr, "Q%d: PLAN_TABLE_ACCESS_FULL\n", getpid());
                #endif

                int record_count = getRecordCount(q->tables[0].db);
                int row_list = createRowList(1, record_count);
                pushRowList(result_set, row_list);

                // First table
                struct Table * table = q->tables;

                if (s->predicate_count > 0) {
                    fullTableScan(table->db, getRowList(row_list), s->predicates, s->predicate_count, s->limit);
                }
                else {
                    fullTableAccess(table->db, getRowList(row_list), s->limit);
                }
                break;
            }

            case PLAN_TABLE_ACCESS_ROWID: {
                #ifdef DEBUG
                fprintf(stderr, "Q%d: PLAN_TABLE_ACCESS_ROWID\n", getpid());
                #endif

                // We'll just recycle the same RowList
                int row_list = popRowList(result_set);

                int source_count = getRowList(row_list)->row_count;

                getRowList(row_list)->row_count = 0;

                for (int i = 0; i < source_count; i++) {
                    int match = 1;

                    for (int j = 0; j < s->predicate_count; j++) {
                        struct Predicate * p = s->predicates + j;

                        char value_left[MAX_VALUE_LENGTH] = {0};
                        char value_right[MAX_VALUE_LENGTH] = {0};

                        evaluateNode(q, getRowList(row_list), i, &p->left, value_left, MAX_VALUE_LENGTH);
                        evaluateNode(q, getRowList(row_list), i, &p->right, value_right, MAX_VALUE_LENGTH);

                        if (!evaluateExpression(p->op, value_left, value_right)) {
                            match = 0;
                            break;
                        }
                    }

                    if (match) {
                        // Add to result set
                        copyResultRow(getRowList(row_list), getRowList(row_list), i);

                        if (s->limit > -1 && getRowList(row_list)->row_count >= s->limit) {
                            break;
                        }
                    }
                }

                pushRowList(result_set, row_list);
                break;
            }

            case PLAN_CROSS_JOIN: {
                #ifdef DEBUG
                fprintf(stderr, "Q%d: PLAN_CROSS_JOIN\n", getpid());
                #endif

                int row_list = popRowList(result_set);

                struct DB *next_db = q->tables[getRowList(row_list)->join_count].db;

                int record_count = getRecordCount(next_db);

                int new_length = getRowList(row_list)->row_count * record_count;

                int new_list = createRowList(getRowList(row_list)->join_count + 1, new_length);

                for (int i = 0; i < getRowList(row_list)->row_count; i++) {
                    int done = 0;

                    for (int j = 0; j < record_count; j++) {
                        appendJoinedRowID(getRowList(new_list), getRowList(row_list), i, j);

                        if (s->limit > -1 && getRowList(new_list)->row_count >= s->limit) {
                            done = 1;
                            break;
                        }
                    }

                    if (done) break;
                }

                destroyRowList(getRowList(row_list));

                pushRowList(result_set, new_list);
                break;
            }

            case PLAN_CONSTANT_JOIN: {
                #ifdef DEBUG
                fprintf(stderr, "Q%d: PLAN_CONSTANT_JOIN\n", getpid());
                #endif

                int row_list = popRowList(result_set);

                // Sanity check
                struct Predicate *p = s->predicates + 0;
                if (p->left.fields[0].table_id != getRowList(row_list)->join_count &&
                    p->right.fields[0].table_id != getRowList(row_list)->join_count)
                {
                    fprintf(stderr, "Cannot perform constant join (Join Index: %d)\n", getRowList(row_list)->join_count);
                    return -1;
                }

                struct DB *next_db = q->tables[getRowList(row_list)->join_count].db;

                int record_count = getRecordCount(next_db);
                int tmp_list = createRowList(1, record_count);

                // This is a constant join so we'll just populate the table once
                // Hopefully it won't be the whole table since we have a predicate
                fullTableScan(next_db, getRowList(tmp_list), s->predicates, s->predicate_count, -1);

                int new_length = getRowList(row_list)->row_count * getRowList(tmp_list)->row_count;

                // Now we know how many rows are to be joined we can make the new row list
                int new_list = createRowList(getRowList(row_list)->join_count + 1, new_length);

                // For each row in the original row list, join every row of the tmp row list
                for (int i = 0; i < getRowList(row_list)->row_count; i++) {
                    int done = 0;

                    for (int j = 0; j < getRowList(tmp_list)->row_count; j++) {
                        int rowid = getRowID(getRowList(tmp_list), 0, j);
                        appendJoinedRowID(getRowList(new_list), getRowList(row_list), i, rowid);

                        if (s->limit > -1 && getRowList(new_list)->row_count >= s->limit) {
                            done = 1;
                            break;
                        }
                    }
                    if (done) break;
                }

                destroyRowList(getRowList(row_list));
                destroyRowList(getRowList(tmp_list));

                pushRowList(result_set, new_list);
                break;
            }

            case PLAN_LOOP_JOIN: {
                #ifdef DEBUG
                fprintf(stderr, "Q%d: PLAN_LOOP_JOIN\n", getpid());
                #endif

                int row_list = popRowList(result_set);

                struct Table *table = &q->tables[getRowList(row_list)->join_count];
                struct DB *next_db = table->db;

                int record_count = getRecordCount(next_db);

                int new_length = getRowList(row_list)->row_count * record_count;

                int new_list = createRowList(getRowList(row_list)->join_count + 1, new_length);

                // Prepare a temporary list that can hold every record in the table
                int tmp_list = createRowList(1, record_count);

                // Table ID being joined here
                int table_id = getRowList(row_list)->join_count;

                for (int i = 0; i < getRowList(row_list)->row_count; i++) {
                    int done = 0;

                    // Make a local copy of predicate
                    struct Predicate p = s->predicates[0];

                    // Fill in value as constant from outer tables
                    if (p.left.fields[0].table_id < table_id) {
                        evaluateNode(q, getRowList(row_list), i, &p.left, p.left.fields[0].text, MAX_FIELD_LENGTH);
                        p.left.fields[0].index = FIELD_CONSTANT;
                        p.left.function = FUNC_UNITY;

                        // We're only passing one table to fullTableScan so predicate will be on first table
                        p.right.fields[0].table_id = 0;
                    }
                    // Fill in value as constant from outer tables
                    else if (p.right.fields[0].table_id < table_id) {
                        evaluateNode(q, getRowList(row_list), i, &p.right, p.right.fields[0].text, MAX_FIELD_LENGTH);
                        p.right.fields[0].index = FIELD_CONSTANT;
                        p.right.function = FUNC_UNITY;

                        // We're only passing one table to fullTableScan so predicate will be on first table
                        p.left.fields[0].table_id = 0;
                    }

                    getRowList(tmp_list)->row_count = 0;

                    // Populate the temp list with all rows which match our special predicate
                    fullTableScan(next_db, getRowList(tmp_list), &p, 1, -1);

                    // Append each row we've just found to the main getRowList(row_list)
                    for (int j = 0; j < getRowList(tmp_list)->row_count; j++) {
                        int rowid = getRowID(getRowList(tmp_list), 0, j);
                        appendJoinedRowID(getRowList(new_list), getRowList(row_list), i, rowid);

                        if (s->limit > -1 && getRowList(new_list)->row_count >= s->limit) {
                            done = 1;
                            break;
                        }
                    }

                    if (done) break;

                    if (table->join_type == JOIN_LEFT && getRowList(tmp_list)->row_count == 0) {
                        // Add NULL to list
                        appendJoinedRowID(getRowList(new_list), getRowList(row_list), i, ROWID_NULL);
                    }
                }

                destroyRowList(getRowList(row_list));
                destroyRowList(getRowList(tmp_list));

                pushRowList(result_set, new_list);
                break;
            }

            case PLAN_UNIQUE_JOIN: {
                #ifdef DEBUG
                fprintf(stderr, "Q%d: PLAN_UNIQUE_JOIN\n", getpid());
                #endif

                int row_list = popRowList(result_set);

                // Table ID being joined here
                int table_id = getRowList(row_list)->join_count;

                struct Table *table = &q->tables[table_id];

                int new_list = createRowList(getRowList(row_list)->join_count + 1, getRowList(row_list)->row_count);

                int tmp_list = createRowList(1, 1);

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

                for (int i = 0; i < getRowList(row_list)->row_count; i++) {
                    char value[MAX_VALUE_LENGTH];

                    // Fill in value as constant from outer tables
                    evaluateNode(q, getRowList(row_list), i, outer, value, MAX_FIELD_LENGTH);

                    // Hang on... won't this only work for calendar?
                    int rowid = pkSearch(&index_db, value);

                    if (rowid != RESULT_NO_ROWS) {
                        getRowList(tmp_list)->row_count = 0;
                        appendJoinedRowID(getRowList(new_list), getRowList(row_list), i, rowid);
                    }
                    else if (table->join_type == JOIN_LEFT) {
                        // Add NULL rowid
                        appendJoinedRowID(getRowList(new_list), getRowList(row_list), i, ROWID_NULL);
                    }

                    if (s->limit > -1 && getRowList(new_list)->row_count >= s->limit) {
                        break;
                    }
                }

                destroyRowList(getRowList(row_list));
                destroyRowList(getRowList(tmp_list));

                pushRowList(result_set, new_list);
                break;
            }

            case PLAN_SORT: {
                #ifdef DEBUG
                fprintf(stderr, "Q%d: PLAN_SORT\n", getpid());
                #endif

                struct ColumnNode *col = &s->predicates[0].left;

                if (populateColumnNode(q, col) != 0) {
                    fprintf(stderr, "Sort column not found: %s\n", s->predicates[0].left.fields[0].text);
                    return -1;
                }

                // debugRowList(row_list, 2);

                int row_list = popRowList(result_set);

                int new_list = createRowList(getRowList(row_list)->join_count, getRowList(row_list)->row_count);

                sortResultRows(q, col, s->predicates[0].op, getRowList(row_list), getRowList(new_list));
                // To implement better sort later
                // sortResultRows(db, order_node, s->predicate_count, getRowList(row_list), getRowList(new_list));

                destroyRowList(getRowList(row_list));

                pushRowList(result_set, new_list);

                // debugRowList(getRowList(row_list), 2);
                break;
            }

            case PLAN_REVERSE: {
                #ifdef DEBUG
                fprintf(stderr, "Q%d: PLAN_REVERSE\n", getpid());
                #endif
                int row_list = popRowList(result_set);
                reverseRowList(getRowList(row_list), s->limit);
                pushRowList(result_set, row_list);
                break;
            }

            case PLAN_SLICE: {
                #ifdef DEBUG
                fprintf(stderr, "Q%d: PLAN_SLICE\n", getpid());
                #endif
                // Offset is taken care of in PLAN_SELECT

                int row_list = popRowList(result_set);

                // Apply limit (including offset rows - which will be omitted later)
                if (s->limit >= 0 && s->limit < getRowList(row_list)->row_count) {
                    getRowList(row_list)->row_count = s->limit;
                }

                pushRowList(result_set, row_list);
                break;
            }

            case PLAN_GROUP: {
                #ifdef DEBUG
                fprintf(stderr, "Q%d: PLAN_GROUP\n", getpid());
                #endif

                // Important! PLAN_GROUP assumes rows are already sorted in
                // GROUP BY order

                if (populateColumnNode(q, &s->predicates[0].left) != 0) {
                    fprintf(stderr, "GROUP BY column not found: %s\n", s->predicates[0].left.fields[0].text);
                    return -1;
                }

                char values[2][MAX_VALUE_LENGTH] = {0};

                int row_list = popRowList(result_set);

                int limit = getRowList(row_list)->row_count;
                if (s->limit > -1 && s->limit < limit) {
                    limit = s->limit;
                }

                int count = 0;

                int curr_list = -1;

                for (int i = 0; i < getRowList(row_list)->row_count; i++) {
                    char *curr_value = values[i%2];
                    char *prev_value = values[(i+1)%2];

                    evaluateNode(q, getRowList(row_list), i, &s->predicates[0].left, curr_value, MAX_VALUE_LENGTH);

                    if (strcmp(prev_value, curr_value)) {
                        if (count >= limit) {
                            break;
                        }

                        curr_list = createRowList(getRowList(row_list)->join_count, getRowList(row_list)->row_count - i);
                        pushRowList(result_set, curr_list);
                        count++;
                    }

                    copyResultRow(getRowList(curr_list), getRowList(row_list), i);
                }

                destroyRowList(getRowList(row_list));

                break;
            }

            case PLAN_SELECT: {
                #ifdef DEBUG
                fprintf(stderr, "Q%d: PLAN_SELECT\n", getpid());
                #endif
                /*******************
                 * Output result set
                 *******************/

                struct RowList *row_list;

                while ((row_list = getRowList(popRowList(result_set)))) {

                    // Aggregate functions will print just one row
                    if (q->flags & FLAG_GROUP) {
                        printResultLine(output, q->tables, q->table_count, q->columns, q->column_count, row_list->row_count > 0 ? q->offset_value : RESULT_NO_ROWS, row_list, output_flags);
                        row_count++;
                    }
                    else for (int i = q->offset_value; i < row_list->row_count; i++) {
                        printResultLine(output, q->tables, q->table_count, q->columns, q->column_count, i, row_list, output_flags);
                        row_count++;
                    }

                    destroyRowList(row_list);
                }

                break;
            }
            default:
                fprintf(stderr, "Unimplemented OP code: %d\n", s->type);
                return -1;
        }

        if (output_flags & OUTPUT_OPTION_STATS) {
            gettimeofday(&stop, NULL);

            fprintf(fstats, "STEP %d,%ld\n", i, dt(stop, start));

            start = stop;
        }

        #ifdef DEBUG
            debugResultSet(result_set);

            int row_list = popRowList(result_set);
            debugRowList(getRowList(row_list), 1);
            pushRowList(result_set, row_list);
        #endif
    }

    if (output_flags & OUTPUT_OPTION_STATS) {
        fclose(fstats);
    }

    printPostamble(output, NULL, q->columns, q->column_count, row_count, output_flags);

    // destroyRowListPool();

    free(result_set);

    for (int i = 0; i < q->table_count; i++) {
        closeDB(q->tables[i].db);
    }

    return 0;
}