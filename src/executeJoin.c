#include <stdlib.h>

#include "structs.h"
#include "result.h"
#include "db.h"
#include "evaluate.h"
#include "indices.h"

/**
 * @brief Every row of left table is unconditionally joined to every
 * row of right table.
 *
 * @param query
 * @param step
 * @param result_set
 * @return int
 */
int executeCrossJoin (struct Query *query, struct PlanStep *step, struct ResultSet *result_set) {

    RowListIndex row_list = popRowList(result_set);

    // Defined to be this table join ID on left
    int table_id = step->predicates[0].left.fields[0].table_id;

    struct DB *next_db = query->tables[table_id].db;

    int record_count = getRecordCount(next_db);

    int new_length = getRowList(row_list)->row_count * record_count;

    RowListIndex new_list = createRowList(getRowList(row_list)->join_count + 1, new_length);

    for (int i = 0; i < getRowList(row_list)->row_count; i++) {
        int done = 0;

        for (int j = 0; j < record_count; j++) {
            appendJoinedRowID(getRowList(new_list), getRowList(row_list), i, j);

            if (step->limit > -1 && getRowList(new_list)->row_count >= step->limit) {
                done = 1;
                break;
            }
        }

        if (done) break;
    }

    destroyRowList(row_list);

    pushRowList(result_set, new_list);

    return 0;
}

/**
 * @brief Join type where each row on left is joined to an identical
 * set of rows from right table. The right hand table only
 * needs to be evaulated once.
 *
 * @param query
 * @param step
 * @param result_set
 * @return int 0 on success
 */
int executeConstantJoin (struct Query *query, struct PlanStep *step, struct ResultSet *result_set) {

    RowListIndex row_list = popRowList(result_set);

    // Defined to be this table join ID on left
    int table_id = step->predicates[0].left.fields[0].table_id;

    // Sanity check
    struct Predicate *p = step->predicates + 0;
    if (p->left.fields[0].table_id != table_id &&
        p->right.fields[0].table_id != table_id)
    {
        fprintf(stderr, "Cannot perform constant join (Join Index: %d)\n", table_id);
        return -1;
    }

    struct DB *next_db = query->tables[table_id].db;

    int record_count = getRecordCount(next_db);
    RowListIndex tmp_list = createRowList(1, record_count);

    // This is a constant join so we'll just populate the table once
    // Hopefully it won't be the whole table since we have a predicate
    fullTableAccess(next_db, getRowList(tmp_list), step->predicates, step->predicate_count, -1);

    int old_count = getRowList(row_list)->row_count;
    int tmp_count = getRowList(tmp_list)->row_count;

    int new_length = old_count * tmp_count;

    // Check for LEFT JOIN
    if (tmp_count == 0 && query->tables[table_id].join_type == JOIN_LEFT) {
        new_length = old_count;
    }

    // Now we know how many rows are to be joined we can make the
    // new row list.
    RowListIndex new_list = createRowList(getRowList(row_list)->join_count + 1, new_length);

    // For each row in the original row list, join every row of the
    // tmp row list.
    for (int i = 0; i < old_count; i++) {
        int done = 0;

        // If it's a LEFT JOIN and the right table was empty we
        // need to add each row to the new row list with NULLs for
        // the right table
        if (tmp_count == 0 && query->tables[table_id].join_type == JOIN_LEFT) {
            appendJoinedRowID(getRowList(new_list), getRowList(row_list), i, ROWID_NULL);

            if (step->limit > -1 && getRowList(new_list)->row_count >= step->limit) {
                done = 1;
                break;
            }
        }
        else for (int j = 0; j < tmp_count; j++) {
            int rowid = getRowID(getRowList(tmp_list), 0, j);
            appendJoinedRowID(getRowList(new_list), getRowList(row_list), i, rowid);

            if (step->limit > -1 && getRowList(new_list)->row_count >= step->limit) {
                done = 1;
                break;
            }
        }
        if (done) break;
    }

    destroyRowList(row_list);
    destroyRowList(tmp_list);

    pushRowList(result_set, new_list);

    return 0;
}


/**
 * @brief Every row of left table is tested with predicates against
 * every row of right table and only added to the result set
 * if all predicates compare true.
 *
 * @param query
 * @param step
 * @param result_set
 * @return int
 */
int executeLoopJoin (struct Query *query, struct PlanStep *step, struct ResultSet *result_set) {

    RowListIndex row_list = popRowList(result_set);

    // Defined to be this table join ID on left
    int table_id = step->predicates[0].left.fields[0].table_id;

    struct Table *table = &query->tables[table_id];
    struct DB *next_db = table->db;

    int record_count = getRecordCount(next_db);

    int new_length = getRowList(row_list)->row_count * record_count;

    RowListIndex new_list = createRowList(getRowList(row_list)->join_count + 1, new_length);

    // Prepare a temporary list that can hold every record in the table
    RowListIndex tmp_list = createRowList(1, record_count);

    for (int i = 0; i < getRowList(row_list)->row_count; i++) {
        int done = 0;

        // Make a local copy of predicate
        struct Predicate p = step->predicates[0];

        // Fill in right value as constant from outer tables
        if (p.right.fields[0].table_id < table_id) {
            evaluateNode(query, getRowList(row_list), i, &p.right, p.right.fields[0].text, MAX_FIELD_LENGTH);
            p.right.fields[0].index = FIELD_CONSTANT;
            p.right.function = FUNC_UNITY;

            // We're only passing one table to fullTableScan so predicate will be on first table
            p.left.fields[0].table_id = 0;
        }
        else {
            fprintf(stderr, "Limitation of RowList: tables must be joined in order specified.\n");
            exit(-1);
        }

        getRowList(tmp_list)->row_count = 0;

        // Populate the temp list with all rows which match our special predicate
        fullTableAccess(next_db, getRowList(tmp_list), &p, 1, -1);

        // Append each row we've just found to the main getRowList(row_list)
        for (int j = 0; j < getRowList(tmp_list)->row_count; j++) {
            int rowid = getRowID(getRowList(tmp_list), 0, j);
            appendJoinedRowID(getRowList(new_list), getRowList(row_list), i, rowid);

            if (step->limit > -1 && getRowList(new_list)->row_count >= step->limit) {
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

    destroyRowList(row_list);
    destroyRowList(tmp_list);

    pushRowList(result_set, new_list);

    return 0;
}

/**
 * @brief Join type where each row on left is joined to exactly 0 or 1
 * row on the right table using a unique index.
 *
 * @param query
 * @param step
 * @param result_set
 * @return int 0 on success
 */
int executeUniqueJoin (struct Query *query, struct PlanStep *step, struct ResultSet *result_set) {

    RowListIndex row_list = popRowList(result_set);

    // Defined to be this table join ID on left
    int table_id = step->predicates[0].left.fields[0].table_id;

    struct Table *table = &query->tables[table_id];

    RowListIndex new_list = createRowList(getRowList(row_list)->join_count + 1, getRowList(row_list)->row_count);

    RowListIndex tmp_list = createRowList(1, 1);

    struct Predicate * p = &step->predicates[0];

    struct ColumnNode * outer;
    struct ColumnNode * inner;

    if (p->left.fields[0].table_id == table_id) {
        outer = &p->right;
        inner = &p->left;
    } else {
        fprintf(stderr, "UNIQUE JOIN table must be on left\n");
        return -1;
    }

    if (outer->fields[0].table_id >= table_id) {
        fprintf(stderr, "Unable to perform UNIQUE JOIN\n");
        return -1;
    }

    struct DB index_db = {0};
    if (findIndex(&index_db, query->tables[table_id].name, inner->fields[0].text, INDEX_UNIQUE) == 0) {
        fprintf(stderr, "Couldn't find unique index on '%s(%s)'\n", query->tables[table_id].name, inner->fields[0].text);
        return -1;
    }

    for (int i = 0; i < getRowList(row_list)->row_count; i++) {
        char value[MAX_VALUE_LENGTH];

        // Fill in value as constant from outer tables
        evaluateNode(query, getRowList(row_list), i, outer, value, MAX_FIELD_LENGTH);

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

        if (step->limit > -1 && getRowList(new_list)->row_count >= step->limit) {
            break;
        }
    }

    destroyRowList(tmp_list);
    destroyRowList(row_list);

    pushRowList(result_set, new_list);

    return 0;
}

/**
 * @brief Join type where each row on left is joined to 0 or more rows on the
 * right table using an index to search for matching rows.
 *
 * @param query
 * @param step
 * @param result_set
 * @return int 0 on success
 */
int executeIndexJoin (struct Query *query, struct PlanStep *step, struct ResultSet *result_set) {

    RowListIndex row_list = popRowList(result_set);

    // Defined to be this table join ID on left
    int table_id = step->predicates[0].left.fields[0].table_id;

    struct Table *table = &query->tables[table_id];

    RowListIndex new_list = createRowList(getRowList(row_list)->join_count + 1, getRowList(row_list)->row_count);

    RowListIndex tmp_list = createRowList(1, getRecordCount(table->db));

    struct Predicate * p = &step->predicates[0];

    struct ColumnNode * outer;
    struct ColumnNode * inner;

    if (p->left.fields[0].table_id == table_id) {
        outer = &p->right;
        inner = &p->left;
    } else {
        fprintf(stderr, "UNIQUE JOIN table must be on left\n");
        return -1;
    }

    if (outer->fields[0].table_id >= table_id) {
        fprintf(stderr, "Unable to perform UNIQUE JOIN\n");
        return -1;
    }

    struct DB index_db = {0};
    if (findIndex(&index_db, query->tables[table_id].name, inner->fields[0].text, INDEX_ANY) == 0) {
        fprintf(stderr, "Couldn't find index on '%s(%s)'\n", query->tables[table_id].name, inner->fields[0].text);
        return -1;
    }

    // Find which column in the index table contains the rowids of the primary table
    int rowid_col = getFieldIndex(&index_db, "rowid");

    for (int i = 0; i < getRowList(row_list)->row_count; i++) {
        char value[MAX_VALUE_LENGTH];
        int done = 0;

        // Fill in value as constant from outer tables
        evaluateNode(query, getRowList(row_list), i, outer, value, MAX_FIELD_LENGTH);

        int sub_row_count = indexSeek(&index_db, rowid_col, p->op, value, getRowList(tmp_list), -1);

        if (sub_row_count > 0) {
            for (int j = 0; j < sub_row_count; j++) {
                int rowid = getRowID(getRowList(tmp_list), 0, j);
                appendJoinedRowID(getRowList(new_list), getRowList(row_list), i, rowid);

                if (step->limit > -1 && getRowList(new_list)->row_count >= step->limit) {
                    done = 1;
                    break;
                }
            }
        }
        else if (table->join_type == JOIN_LEFT) {
            // Add NULL rowid
            appendJoinedRowID(getRowList(new_list), getRowList(row_list), i, ROWID_NULL);
        }

        if (done || (step->limit > -1 && getRowList(new_list)->row_count >= step->limit)) {
            break;
        }

        // clear tmp_list
        getRowList(tmp_list)->row_count = 0;
    }

    destroyRowList(tmp_list);
    destroyRowList(row_list);

    pushRowList(result_set, new_list);

    return 0;
}