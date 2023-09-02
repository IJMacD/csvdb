#include <stdlib.h>

#include "../structs.h"
#include "../query/result.h"
#include "../evaluate/evaluate.h"
#include "../functions/util.h"
#include "../db/db.h"
#include "../db/indices.h"

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

    indexPrimarySeek(
        table->db,
        p->function,
        p->children[1].field.text,
        getRowList(row_list),
        step->limit
    );

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

    if (
        findIndex(
            &index_db,
            table->name,
            p->children[0].field.text,
            INDEX_UNIQUE
        ) == 0
    ) {
        fprintf(
            stderr,
            "Unable to find unique index on column '%s' on table '%s'\n",
            p->children[0].field.text,
            table->name
        );
        return -1;
    }

    int record_count = (step->limit > -1)
        ? step->limit : (
            (p->function == OPERATOR_EQ) ? 1 : getRecordCount(&index_db)
        );

    RowListIndex row_list = createRowList(1, record_count);
    pushRowList(result_set, row_list);

    // Find which column in the index table contains the rowids of the primary
    // table
    int rowid_col = getFieldIndex(&index_db, "rowid");
    indexUniqueSeek(
        &index_db,
        rowid_col,
        p->function,
        p->children[1].field.text,
        getRowList(row_list),
        step->limit
    );

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
            p->children[0].field.text,
            INDEX_ANY
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

    int record_count = (step->limit > -1)
        ? step->limit : getRecordCount(&index_db);

    RowListIndex row_list = createRowList(1, record_count);
    pushRowList(result_set, row_list);

    // Find which column in the index table contains the rowids of the primary
    // table
    int rowid_col = getFieldIndex(&index_db, "rowid");
    indexSeek(
        &index_db,
        rowid_col,
        p->function,
        p->children[1].field.text,
        getRowList(row_list),
        step->limit
    );

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

    indexCoveringSeek(
        index_db,
        p->function,
        p->children[1].field.text,
        getRowList(row_list),
        step->limit
    );

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
            p->field.text,
            INDEX_ANY
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

    // Find which column in the index table contains the rowids of the primary
    // table
    int rowid_col = getFieldIndex(&index_db, "rowid");
    indexScan(&index_db, rowid_col, getRowList(row_list), step->limit);

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

    int record_count = limit;

    if (record_count < 0) {
        record_count = getRecordCount(tables[0].db) - start_rowid;
    }

    RowListIndex row_list = createRowList(1, record_count);
    pushRowList(result_set, row_list);

    fullTableScan(table->db, row_list, start_rowid, limit);

    return 0;
}