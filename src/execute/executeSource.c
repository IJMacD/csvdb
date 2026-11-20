#include <stdlib.h>

#include "../structs.h"
#include "../query/result.h"
#include "../evaluate/evaluate.h"
#include "../functions/util.h"
#include "../db/db.h"
#include "../db/indices.h"

/**
 * If step carries two complementary range predicates on the same column,
 * fill row_list via indexBoundedSeek. Returns 1 if handled, 0 otherwise.
 * Any nodes after the seek bound(s) are residual index filters.
 */
static int tryBoundedIndexSeek (
    struct DB *index_db,
    int rowid_column,
    struct PlanStep *step,
    struct RowList *row_list
) {
    int seek_count = indexStepSeekNodeCount(step->nodes, step->node_count);
    if (seek_count < 2)
    {
        return 0;
    }

    struct Node *a = &step->nodes[0];
    struct Node *b = &step->nodes[1];
    int a_lo = a->function == OPERATOR_GE || a->function == OPERATOR_GT;
    int a_hi = a->function == OPERATOR_LE || a->function == OPERATOR_LT;

    struct Node *lo = a_lo ? a : b;
    struct Node *hi = a_hi ? a : b;

    struct Node *filters =
        (step->node_count > seek_count) ? &step->nodes[seek_count] : NULL;
    int filter_count = step->node_count - seek_count;

    indexBoundedSeek(
        index_db,
        rowid_column,
        lo->function,
        lo->children[1].field.text,
        hi->function,
        hi->children[1].field.text,
        row_list,
        step->limit,
        filters,
        filter_count);

    return 1;
}

static void indexStepFilters (
    struct PlanStep *step,
    int seek_count,
    struct Node **filters_out,
    int *filter_count_out
) {
    if (step->node_count > seek_count) {
        *filters_out = &step->nodes[seek_count];
        *filter_count_out = step->node_count - seek_count;
    } else {
        *filters_out = NULL;
        *filter_count_out = 0;
    }
}

int executeSourceDummyRow (
    __attribute__((unused)) struct Table *tables,
    __attribute__((unused)) struct PlanStep *step,
    struct ResultSet *result_set
) {
    RowListIndex row_list = createRowList(0, 0);
    getRowList(row_list)->row_count = 1;
    pushRowList(result_set, row_list);
    return 0;
}

int executeSourcePK (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
) {
    int record_count = (step->limit > -1)
        ? step->limit : getRecordCount(tables[0].db);

    RowListIndex row_list = createRowList(1, record_count);
    pushRowList(result_set, row_list);

    // First table
    struct Table * table = tables;
    struct Node *p = &step->nodes[0];

    if (!tryBoundedIndexSeek(
            table->db, FIELD_ROW_INDEX, step, getRowList(row_list)))
    {
        struct Node *filters = NULL;
        int filter_count = 0;
        indexStepFilters(step, 1, &filters, &filter_count);

        indexUniqueSeek(
            table->db,
            FIELD_ROW_INDEX,
            p->function,
            p->children[1].field.text,
            getRowList(row_list),
            step->limit,
            filters,
            filter_count
        );
    }

    return 0;
}

int executeSourceUnique (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
) {
    // First table
    struct Table * table = tables;
    struct Node *p = &step->nodes[0];
    struct DB index_db;

    struct Node *predicate_node;
    enum Function op;
    const char *predicate_value = NULL;

    if (p->function == FUNC_UNITY)
    {
        // If we just have a single raw Node then it's a full index scan
        op = OPERATOR_ALWAYS;
        predicate_node = p;
    }
    else if ((p->function & MASK_FUNC_FAMILY) == FUNC_FAM_OPERATOR)
    {
        // If we have an operator Node then it's a partial index scan
        op = p->function;
        predicate_node = &p->children[0];
        predicate_value = p->children[1].field.text;
    }
    else
    {
        fprintf(stderr, "Indices on functions are not supported\n");
        return -1;
    }

    if (
        findIndex(
            &index_db,
            table->name,
            predicate_node,
            INDEX_UNIQUE,
            NULL) == 0)
    {
        fprintf(
            stderr,
            "Unable to find unique index on column '%s' on table '%s'\n",
            predicate_node->field.text,
            table->name);
        return -1;
    }

    int record_count = (step->limit > -1)
        ? step->limit : (
            (p->function == OPERATOR_EQ) ? 1 : getRecordCount(&index_db)
        );

    RowListIndex row_list = createRowList(1, record_count);
    pushRowList(result_set, row_list);

    // Trailing column is the base-table rowid (name irrelevant)
    int rowid_col = indexRowidColumn(&index_db);

    // Fill in RowIDs from index (dual-bound BETWEEN when seek_count == 2)
    if (!tryBoundedIndexSeek(
            &index_db, rowid_col, step, getRowList(row_list)))
    {
        int seek_count = indexStepSeekNodeCount(step->nodes, step->node_count);
        if (seek_count < 1) {
            seek_count = 1;
        }
        struct Node *filters = NULL;
        int filter_count = 0;
        indexStepFilters(step, seek_count, &filters, &filter_count);

        indexUniqueSeek(
            &index_db,
            rowid_col,
            op,
            predicate_value,
            getRowList(row_list),
            step->limit,
            filters,
            filter_count);
    }

    closeDB(&index_db);

    return 0;
}

int executeSourceIndexSeek (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
) {
    // First table
    struct Table * table = tables;
    struct Node *p = &step->nodes[0];
    struct DB index_db;

    if (
        findIndex(
            &index_db,
            table->name,
            &p->children[0],
            INDEX_ANY,
            NULL
        ) == 0
    ) {
        fprintf(
            stderr,
            "Unable to find index on column '%s' on table '%s'\n",
            p->children[0].field.text,
            table->name
        );
        return -1;
    }

    int record_count =  getRecordCount(&index_db);
    if (step->limit > -1) {
        record_count = MIN(record_count, step->limit);
    }

    RowListIndex row_list = createRowList(1, record_count);
    pushRowList(result_set, row_list);

    // Trailing column is the base-table rowid (name irrelevant)
    int rowid_col = indexRowidColumn(&index_db);
    if (!tryBoundedIndexSeek(
            &index_db, rowid_col, step, getRowList(row_list)))
    {
        int seek_count = indexStepSeekNodeCount(step->nodes, step->node_count);
        if (seek_count < 1) {
            seek_count = 1;
        }
        struct Node *filters = NULL;
        int filter_count = 0;
        indexStepFilters(step, seek_count, &filters, &filter_count);

        indexSeek(
            &index_db,
            rowid_col,
            p->function,
            p->children[1].field.text,
            getRowList(row_list),
            step->limit,
            filters,
            filter_count
        );
    }

    closeDB(&index_db);

    return 0;
}

int executeSourceCoveringIndexSeek (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
) {
    // Table *is* the index
    struct Table *table = tables;
    struct Node *p = &step->nodes[0];
    struct DB *index_db = table->db;

    int record_count = (step->limit > -1)
        ? step->limit : getRecordCount(index_db);

    RowListIndex row_list = createRowList(1, record_count);
    pushRowList(result_set, row_list);

    if (!tryBoundedIndexSeek(
            index_db, FIELD_ROW_INDEX, step, getRowList(row_list)))
    {
        int seek_count = indexStepSeekNodeCount(step->nodes, step->node_count);
        if (seek_count < 1) {
            seek_count = 1;
        }
        struct Node *filters = NULL;
        int filter_count = 0;
        indexStepFilters(step, seek_count, &filters, &filter_count);

        indexCoveringSeek(
            index_db,
            p->function,
            p->children[1].field.text,
            getRowList(row_list),
            step->limit,
            filters,
            filter_count
        );
    }

    return 0;
}

int executeSourceIndexScan (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
) {
    // First table
    struct Table * table = tables;
    struct Node *p = &step->nodes[0];
    struct DB index_db;

    if (
        findIndex(
            &index_db,
            table->name,
            p,
            INDEX_ANY,
            NULL
        ) == 0
    ) {
        fprintf(
            stderr,
            "Unable to find index on column '%s' on table '%s'\n",
            p->field.text,
            table->name
        );
        return -1;
    }

    int record_count = (step->limit > -1)
    ? step->limit : getRecordCount(&index_db);

    RowListIndex row_list = createRowList(1, record_count);
    pushRowList(result_set, row_list);

    // Trailing column is the base-table rowid (name irrelevant)
    int rowid_col = indexRowidColumn(&index_db);
    // INDEX SCAN nodes are order columns (UNITY), not filters
    indexScan(
        &index_db, rowid_col, getRowList(row_list), step->limit, NULL, 0);

    closeDB(&index_db);

    return 0;
}

/**
 * @brief Sequentially access every row of the table applying the predicate
 * nodes to each row accessed.
 *
 * @param query
 * @param step
 * @param result_set
 * @return int
 */
int executeSourceTableFull (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
) {
    int record_count = (step->limit >= 0)
        ? step->limit : getRecordCount(tables[0].db);

    RowListIndex row_list = createRowList(1, record_count);
    pushRowList(result_set, row_list);

    // First table
    struct Table * table = tables;

    fullTableAccess(
        table->db,
        row_list,
        step->nodes,
        step->node_count,
        step->limit
    );

    return 0;
}

/**
 * @brief Iterate a range of rowids adding each one to the RowList.
 * The range of rowids is determined by rowid predicates.
 * Any nodes on this step must ONLY be rowid nodes.
 *
 * @param query
 * @param step
 * @param result_set
 * @return int
 */
int executeSourceTableScan (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
) {
    // First table
    struct Table * table = tables;

    int start_rowid = 0;
    int limit = step->limit;

    if (step->node_count > 0) {
        if (step->node_count > 1) {
            fprintf(
                stderr,
                "Unable to do FULL TABLE SCAN with more than one predicate\n"
            );
            return -1;
        }

        if (!isConstantNode(&step->nodes[0].children[1])) {
            fprintf(
                stderr,\
                "Cannot compare rowid against non-constant value\n"
            );
            return -1;
        }

        char right_value[MAX_VALUE_LENGTH] = {0};
        evaluateConstantNode(&step->nodes[0].children[1], right_value);

        if (!is_numeric(right_value)) {
            return 0;
        }

        int right_val = atoi(right_value);

        enum Function function = step->nodes[0].function;

        int op_limit = -1;

        if (function == OPERATOR_EQ) {
            start_rowid = right_val;
            op_limit = 1;
        }
        else if (function == OPERATOR_LT) {
            start_rowid = 0;
            op_limit = right_val;
        }
        else if (function == OPERATOR_LE) {
            start_rowid = 0;
            op_limit = right_val + 1;
        }
        else if (function == OPERATOR_GT) {
            start_rowid = right_val + 1;
            op_limit = -1;
        }
        else if (function == OPERATOR_GE) {
            start_rowid = right_val;
            op_limit = -1;
        }
        else {
            fprintf(
                stderr,
                "Unable to do FULL TABLE SCAN with operator %d\n",
                function
            );
            return -1;
        }

        if (op_limit >= 0) {
            if (limit < 0) {
                limit = op_limit;
            }
            else {
                limit = MIN(limit, op_limit);
            }
        }
    }

    int record_count = getRecordCount(tables[0].db);

    if (limit >= 0) {
        record_count = MIN(record_count, limit);
    }

    RowListIndex row_list = createRowList(1, record_count);
    pushRowList(result_set, row_list);

    fullTableScan(table->db, row_list, start_rowid, record_count);

    return 0;
}
