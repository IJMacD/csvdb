#include "../structs.h"
#include "../query/result.h"
#include "../evaluate/evaluate.h"
#include "../evaluate/predicates.h"

/**
 * @brief Every row of result set is checked against predicates and
 * those which pass are added to the output result set.
 *
 * @param query
 * @param step
 * @param result_set
 * @return int
 */
int executeTableAccessRowid (
    struct Query *query,
    struct PlanStep *step,
    struct ResultSet *result_set
) {
    // We'll just recycle the same RowList
    RowListIndex row_list = popRowList(result_set);

    int source_count = getRowList(row_list)->row_count;

    getRowList(row_list)->row_count = 0;

    for (int i = 0; i < source_count; i++) {
        int match = 1;

        for (int j = 0; j < step->predicate_count; j++) {
            struct Predicate * p = step->predicates + j;

            char value_left[MAX_VALUE_LENGTH] = {0};
            char value_right[MAX_VALUE_LENGTH] = {0};

            evaluateNode(
                query->tables,
                getRowList(row_list),
                i,
                &p->left,
                value_left,
                MAX_VALUE_LENGTH
            );
            evaluateNode(
                query->tables,
                getRowList(row_list),
                i,
                &p->right,
                value_right,
                MAX_VALUE_LENGTH
            );

            if (!evaluateExpression(p->op, value_left, value_right)) {
                match = 0;
                break;
            }
        }

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
    __attribute__((unused)) struct Query *query,
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