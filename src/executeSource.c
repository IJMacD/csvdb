#include <stdlib.h>

#include "structs.h"
#include "result.h"
#include "db.h"
#include "indices.h"

int executeSourceDummyRow (
    __attribute__((unused)) struct Query *query,
    __attribute__((unused)) struct PlanStep *step,
    struct ResultSet *result_set
) {
    RowListIndex row_list = createRowList(0, 0);
    getRowList(row_list)->row_count = 1;
    pushRowList(result_set, row_list);
    return 0;
}

int executeSourcePK (
    struct Query *query,
    struct PlanStep *step,
    struct ResultSet *result_set
) {
    int record_count = (step->limit > -1)
        ? step->limit : getRecordCount(query->tables[0].db);

    RowListIndex row_list = createRowList(1, record_count);
    pushRowList(result_set, row_list);

    // First table
    struct Table * table = query->tables;
    struct Predicate *p = &step->predicates[0];

    indexPrimarySeek(
        table->db,
        p->op,
        p->right.fields[0].text,
        getRowList(row_list),
        step->limit
    );

    return 0;
}

int executeSourceUnique (
    struct Query *query,
    struct PlanStep *step,
    struct ResultSet *result_set
) {
    // First table
    struct Table * table = query->tables;
    struct Predicate *p = &step->predicates[0];
    struct DB index_db;

    if (
        findIndex(
            &index_db,
            table->name,
            p->left.fields[0].text,
            INDEX_UNIQUE
        ) == 0
    ) {
        fprintf(
            stderr,
            "Unable to find unique index on column '%s' on table '%s'\n",
            p->left.fields[0].text,
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
    indexUniqueSeek(
        &index_db,
        rowid_col,
        p->op,
        p->right.fields[0].text,
        getRowList(row_list),
        step->limit
    );

    return 0;
}

int executeSourceIndexSeek (
    struct Query *query,
    struct PlanStep *step,
    struct ResultSet *result_set
) {
    // First table
    struct Table * table = query->tables;
    struct Predicate *p = &step->predicates[0];
    struct DB index_db;

    if (
        findIndex(
            &index_db,
            table->name,
            p->left.fields[0].text,
            INDEX_ANY
        ) == 0
    ) {
        fprintf(
            stderr,
            "Unable to find index on column '%s' on table '%s'\n",
            p->left.fields[0].text,
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
        p->op,
        p->right.fields[0].text,
        getRowList(row_list),
        step->limit
    );

    return 0;
}

int executeSourceIndexScan (
    struct Query *query,
    struct PlanStep *step,
    struct ResultSet *result_set
) {
    // First table
    struct Table * table = query->tables;
    struct Predicate *p = &step->predicates[0];
    struct DB index_db;

    if (
        findIndex(
            &index_db,
            table->name,
            p->left.fields[0].text,
            INDEX_ANY
        ) == 0
    ) {
        fprintf(
            stderr,
            "Unable to find index on column '%s' on table '%s'\n",
            p->left.fields[0].text,
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

    return 0;
}

/**
 * @brief Sequentially access every row of the table applying the
 * predicates to each row accessed.
 *
 * @param query
 * @param step
 * @param result_set
 * @return int
 */
int executeSourceTableFull (
    struct Query *query,
    struct PlanStep *step,
    struct ResultSet *result_set
) {
    int record_count = (step->limit >= 0)
        ? step->limit : getRecordCount(query->tables[0].db);

    RowListIndex row_list = createRowList(1, record_count);
    pushRowList(result_set, row_list);

    // First table
    struct Table * table = query->tables;

    fullTableAccess(
        table->db,
        getRowList(row_list),
        step->predicates,
        step->predicate_count,
        step->limit
    );

    return 0;
}

/**
 * @brief Iterate a range of rowids adding each one to the RowList.
 * Any predicates on this step must ONLY be rowid predicates.
 *
 * @param query
 * @param step
 * @param result_set
 * @return int
 */
int executeSourceTableScan (
    struct Query *query,
    struct PlanStep *step,
    struct ResultSet *result_set
) {
    // First table
    struct Table * table = query->tables;

    int start_rowid = 0;
    int limit = step->limit;

    if (step->predicate_count > 0) {
        if (step->predicate_count > 1) {
            fprintf(
                stderr,
                "Unable to do FULL TABLE SCAN with more than one predicate\n"
            );
            return -1;
        }

        if (step->predicates[0].right.fields[0].index != FIELD_CONSTANT) {
            fprintf(
                stderr,
                "Cannot compare rowid against non-constant value\n"
            );
            return -1;
        }

        int right_val = atoi(step->predicates[0].right.fields[0].text);
        enum Operator op = step->predicates[0].op;

        if (op == OPERATOR_EQ) {
            start_rowid = right_val;
            limit = 1;
        }
        else if (op == OPERATOR_LT) {
            start_rowid = 0;
            limit = limit > -1 ? MIN(limit, right_val) : right_val;
        }
        else if (op == OPERATOR_LE) {
            start_rowid = 0;
            limit = limit > -1 ? MIN(limit, right_val + 1) : (right_val + 1);
        }
        else if (op == OPERATOR_GT) {
            start_rowid = right_val + 1;
            limit = -1;
        }
        else if (op == OPERATOR_GE) {
            start_rowid = right_val;
            limit = -1;
        }
        else {
            fprintf(
                stderr,
                "Unable to do FULL TABLE SCAN with operator %d\n",
                op
            );
            return -1;
        }
    }

    int record_count = limit;

    if (record_count < 0) {
        record_count = getRecordCount(query->tables[0].db) - start_rowid;
    }

    RowListIndex row_list = createRowList(1, record_count);
    pushRowList(result_set, row_list);

    fullTableScan(table->db, getRowList(row_list), start_rowid, limit);

    return 0;
}