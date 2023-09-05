#include <stdlib.h>

#include "../structs.h"
#include "../query/query.h"
#include "../query/node.h"
#include "../query/result.h"
#include "../db/db.h"
#include "../evaluate/evaluate.h"
#include "../db/indices.h"
#include "../debug.h"

static void replaceTableID (struct Node *node, int table_id);

/**
 * @brief Every row of left table is unconditionally joined to every
 * row of right table.
 *
 * @param tables
 * @param step
 * @param result_set
 * @return int
 */
int executeCrossJoin (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
) {

    RowListIndex list_id = popRowList(result_set);

    int table_id = getRowList(list_id)->join_count;

    struct DB *next_db = tables[table_id].db;

    int record_count = getRecordCount(next_db);

    int new_length = getRowList(list_id)->row_count * record_count;

    RowListIndex new_list = createRowList(
        getRowList(list_id)->join_count + 1,
        new_length
    );

    for (int i = 0; i < getRowList(list_id)->row_count; i++) {
        int done = 0;

        for (int j = 0; j < record_count; j++) {
            appendJoinedRowID(getRowList(new_list), getRowList(list_id), i, j);

            if (
                step->limit > -1
                && getRowList(new_list)->row_count >= step->limit
            ) {
                done = 1;
                break;
            }
        }

        if (done) break;
    }

    destroyRowList(list_id);

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
int executeConstantJoin (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
) {

    RowListIndex list_id = popRowList(result_set);

    // Tables must be joined in same order as specified
    int next_table_id = getRowList(list_id)->join_count;

    struct DB *next_db = tables[next_table_id].db;

    if (next_db == NULL) {
        fprintf(stderr, "Unknown DB in JOIN\n");
        exit(-1);
    }

    int record_count = getRecordCount(next_db);
    RowListIndex tmp_list = createRowList(1, record_count);

    // We need to replace all references to table_id with table_id = 0 since
    // we're passing to fullTableAccess which might execute the node an we're
    // only passing a single table. This should be safe since constant join
    // should only have exactly one table.
    for (int i = 0; i < step->node_count; i++) {
        struct Node *node = &step->nodes[i];
        replaceTableID(node, 0);
    }

    // This is a constant join so we'll just populate the table once
    // Hopefully it won't be the whole table since we have a predicate
    fullTableAccess(
        next_db,
        tmp_list,
        step->nodes,
        step->node_count,
        -1
    );

    int old_count = getRowList(list_id)->row_count;
    int tmp_count = getRowList(tmp_list)->row_count;

    int new_length = old_count * tmp_count;

    // Check for LEFT JOIN
    if (tmp_count == 0 && tables[next_table_id].join_type == JOIN_LEFT) {
        new_length = old_count;
    }

    // Now we know how many rows are to be joined we can make the
    // new row list.
    RowListIndex new_list = createRowList(
        getRowList(list_id)->join_count + 1,
        new_length
    );

    // For each row in the original row list, join every row of the
    // tmp row list.
    for (int i = 0; i < old_count; i++) {
        int done = 0;

        // If it's a LEFT JOIN and the right table was empty we
        // need to add each row to the new row list with NULLs for
        // the right table
        if (tmp_count == 0 && tables[next_table_id].join_type == JOIN_LEFT) {
            appendJoinedRowID(
                getRowList(new_list),
                getRowList(list_id),
                i,
                ROWID_NULL
            );

            if (
                step->limit > -1
                && getRowList(new_list)->row_count >= step->limit
            ) {
                done = 1;
                break;
            }
        }
        else for (int j = 0; j < tmp_count; j++) {
            int rowid = getRowID(getRowList(tmp_list), 0, j);
            appendJoinedRowID(
                getRowList(new_list),
                getRowList(list_id),
                i,
                rowid
            );

            if (
                step->limit > -1
                && getRowList(new_list)->row_count >= step->limit
            ) {
                done = 1;
                break;
            }
        }
        if (done) break;
    }

    destroyRowList(list_id);
    destroyRowList(tmp_list);

    pushRowList(result_set, new_list);

    return 0;
}


/**
 * @brief Every row of left table is tested with nodes against
 * every row of right table and only added to the result set
 * if all nodes compare true.
 *
 * @param query
 * @param step
 * @param result_set
 * @return int
 */
int executeLoopJoin (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
) {

    RowListIndex list_id = popRowList(result_set);

    int table_id = getRowList(list_id)->join_count;

    struct Table *table = &tables[table_id];
    struct DB *next_db = table->db;

    int record_count = getRecordCount(next_db);

    int new_length = getRowList(list_id)->row_count * record_count;

    RowListIndex new_list = createRowList(
        getRowList(list_id)->join_count + 1,
        new_length
    );

    // Prepare a temporary list that can hold every record in the table
    RowListIndex tmp_list = createRowList(1, record_count);

    for (int i = 0; i < getRowList(list_id)->row_count; i++) {
        int done = 0;

        // Make a local copy of predicate
        struct Node p;
        copyNodeTree(&p, &step->nodes[0]);

        struct Node *right_node = &p.children[1];
        struct Field *right_field =
            (right_node->function == FUNC_UNITY || right_node->child_count == -1)
            ? &right_node->field : &right_node->children[0].field;

        // Fill in right value as constant from outer tables
        if (right_field->table_id < table_id) {
            // replace right node with constant value from outer table
            evaluateNode(
                tables,
                list_id,
                i,
                right_node,
                right_node->field.text,
                MAX_FIELD_LENGTH
            );
            right_node->field.index = FIELD_CONSTANT;
            right_node->function = FUNC_UNITY;

            // We're only passing one table to fullTableScan so predicate will
            // be on first table
            struct Node *left_node = &p.children[0];
            struct Field *left_field =
                (left_node->function == FUNC_UNITY || left_node->child_count == -1)
                ? &left_node->field : &left_node->children[0].field;

            left_field->table_id = 0;
        }
        else {
            fprintf(stderr, "Limitation of RowList: tables must be joined in "
                "order specified.\n");
            exit(-1);
        }

        getRowList(tmp_list)->row_count = 0;

        // Populate the temp list with all rows which match our special
        // predicate
        fullTableAccess(next_db, tmp_list, &p, 1, -1);

        freeNode(&p);

        // Append each row we've just found to the main getRowList(row_list)
        for (int j = 0; j < getRowList(tmp_list)->row_count; j++) {
            int rowid = getRowID(getRowList(tmp_list), 0, j);

            appendJoinedRowID(
                getRowList(new_list),
                getRowList(list_id),
                i,
                rowid
            );

            if (
                step->limit > -1
                && getRowList(new_list)->row_count >= step->limit
            ) {
                done = 1;
                break;
            }
        }

        if (done) break;

        if (
            table->join_type == JOIN_LEFT
            && getRowList(tmp_list)->row_count == 0
        ) {
            // Add NULL to list
            appendJoinedRowID(
                getRowList(new_list),
                getRowList(list_id),
                i,
                ROWID_NULL
            );
        }
    }

    destroyRowList(list_id);
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
int executeUniqueJoin (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
) {

    RowListIndex list_id = popRowList(result_set);

    int table_id = getRowList(list_id)->join_count;

    struct Table *table = &tables[table_id];

    RowListIndex new_list = createRowList(
        getRowList(list_id)->join_count + 1,
        getRowList(list_id)->row_count
    );

    RowListIndex tmp_list = createRowList(1, 1);

    struct Node * p = &step->nodes[0];

    struct Node * outer;
    struct Node * inner;

    if (p->children[0].field.table_id != table_id) {
        fprintf(stderr, "UNIQUE JOIN table must be on left\n");
        return -1;
    }

    outer = &p->children[1];
    inner = &p->children[0];

    if (outer->field.table_id >= table_id) {
        fprintf(stderr, "Unable to perform UNIQUE JOIN\n");
        return -1;
    }

    struct DB index_db = {0};
    if (
        findIndex(
            &index_db,
            tables[table_id].name,
            inner,
            INDEX_UNIQUE,
            NULL
        ) == 0
    ) {
        fprintf(
            stderr,
            "Couldn't find unique index on '%s(%s)'\n",
            tables[table_id].name,
            inner->field.text
        );
        return -1;
    }

    int rowid_field = getFieldIndex(&index_db, "rowid");

    for (int i = 0; i < getRowList(list_id)->row_count; i++) {
        char value[MAX_VALUE_LENGTH];
        int output_status;

        // debugNode(outer);

        // Fill in value as constant from outer tables
        evaluateNode(
            tables,
            list_id,
            i,
            outer,
            value,
            MAX_FIELD_LENGTH
        );

        int index_rowid = uniqueIndexSearch(&index_db, value, &output_status);
        getRecordValue(
            &index_db,
            index_rowid,
            rowid_field,
            value,
            MAX_VALUE_LENGTH
        );
        int rowid = atoi(value);

        if (rowid != RESULT_NO_ROWS) {
            getRowList(tmp_list)->row_count = 0;
            appendJoinedRowID(
                getRowList(new_list),
                getRowList(list_id),
                i,
                rowid
            );
        }
        else if (table->join_type == JOIN_LEFT) {
            // Add NULL rowid
            appendJoinedRowID(
                getRowList(new_list),
                getRowList(list_id),
                i,
                ROWID_NULL
            );
        }

        if (
            step->limit > -1
            && getRowList(new_list)->row_count >= step->limit
        ) {
            break;
        }
    }

    closeDB(&index_db);

    destroyRowList(tmp_list);
    destroyRowList(list_id);

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
int executeIndexJoin (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
) {

    RowListIndex list_id = popRowList(result_set);

    int table_id = getRowList(list_id)->join_count;

    struct Table *table = &tables[table_id];

    RowListIndex new_list = createRowList(
        getRowList(list_id)->join_count + 1,
        getRowList(list_id)->row_count
    );

    RowListIndex tmp_list = createRowList(1, getRecordCount(table->db));

    struct Node * p = &step->nodes[0];

    struct Node * outer;
    struct Node * inner;

    if (p->children[0].field.table_id == table_id) {
        outer = &p->children[1];
        inner = &p->children[0];
    } else {
        fprintf(stderr, "UNIQUE JOIN table must be on left\n");
        return -1;
    }

    if (outer->field.table_id >= table_id) {
        fprintf(stderr, "Unable to perform UNIQUE JOIN\n");
        return -1;
    }

    struct DB index_db = {0};
    if (
        findIndex(
            &index_db,
            tables[table_id].name,
            inner,
            INDEX_ANY,
            NULL
        ) == 0
    ) {
        fprintf(
            stderr,
            "Couldn't find index on '%s(%s)'\n",
            tables[table_id].name,
            inner->field.text
        );
        return -1;
    }

    // Find which column in the index table contains the rowids of the primary
    // table
    int rowid_col = getFieldIndex(&index_db, "rowid");

    for (int i = 0; i < getRowList(list_id)->row_count; i++) {
        char value[MAX_VALUE_LENGTH];
        int done = 0;

        // Fill in value as constant from outer tables
        evaluateNode(
            tables,
            list_id,
            i,
            outer,
            value,
            MAX_FIELD_LENGTH
        );

        int sub_row_count = indexSeek(
            &index_db,
            rowid_col,
            p->function,
            value,
            getRowList(tmp_list),
            -1
        );

        if (sub_row_count > 0) {
            for (int j = 0; j < sub_row_count; j++) {
                int rowid = getRowID(getRowList(tmp_list), 0, j);

                appendJoinedRowID(
                    getRowList(new_list),
                    getRowList(list_id),
                    i,
                    rowid
                );

                if (
                    step->limit > -1
                    && getRowList(new_list)->row_count >= step->limit
                ) {
                    done = 1;
                    break;
                }
            }
        }
        else if (table->join_type == JOIN_LEFT) {
            // Add NULL rowid
            appendJoinedRowID(
                getRowList(new_list),
                getRowList(list_id),
                i,
                ROWID_NULL
            );
        }

        if (
            done
            || (
                step->limit > -1
                && getRowList(new_list)->row_count >= step->limit
            )
        ) {
            break;
        }

        // clear tmp_list
        getRowList(tmp_list)->row_count = 0;
    }

    closeDB(&index_db);

    destroyRowList(tmp_list);
    destroyRowList(list_id);

    pushRowList(result_set, new_list);

    return 0;
}

static void replaceTableID (struct Node *node, int table_id) {
    for (int i = 0; i < node->child_count; i++) {
        struct Node *child = &node->children[0];
        replaceTableID(child, table_id);
    }

    node->field.table_id = table_id;
}