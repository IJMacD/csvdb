#include "../structs.h"
#include "../query/result.h"
#include "../evaluate/evaluate.h"
#include "../evaluate/predicates.h"

/**
 * @brief Every row of result set is checked against nodes and
 * those which pass are added to the output result set.
 *
 * @param tables
 * @param step
 * @param result_set
 * @return int
 */
int executeTableAccessRowid (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
) {
    // We'll just recycle the same RowList
    RowListIndex row_list = popRowList(result_set);

    int source_count = getRowList(row_list)->row_count;

    getRowList(row_list)->row_count = 0;

    for (int i = 0; i < source_count; i++) {
        int match = evaluateOperatorNodeListAND(
            tables,
            row_list,
            i,
            step->nodes,
            step->node_count
        );

        if (match) {
            // Add to result set
            copyResultRow(getRowList(row_list), getRowList(row_list), i);

            if (
                step->limit > -1
                && getRowList(row_list)->row_count >= step->limit
            ) {
                break;
            }
        }
    }

    pushRowList(result_set, row_list);

    return 0;
}

int executeSlice (
    __attribute__((unused)) struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
) {
    // Offset is taken care of in PLAN_SELECT

    RowListIndex row_list = popRowList(result_set);

    // Apply limit (including offset rows - which will be omitted later)
    if (step->limit >= 0 && step->limit < getRowList(row_list)->row_count) {
        getRowList(row_list)->row_count = step->limit;
    }

    pushRowList(result_set, row_list);

    return 0;
}

int executeOffset (
    __attribute__((unused)) struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
) {
    // Offset is taken care of in PLAN_SELECT

    RowListIndex src_list = popRowList(result_set);

    int join_count = getRowList(src_list)->join_count;
    int row_count = getRowList(src_list)->row_count;

    RowListIndex dest_list = createRowList(join_count, row_count);

    for (int i = step->limit; i < row_count; i++) {
        copyResultRow(getRowList(dest_list), getRowList(src_list), i);
    }

    pushRowList(result_set, dest_list);

    destroyRowList(src_list);

    return 0;
}
