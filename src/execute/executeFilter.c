#include <string.h>
#include <stdlib.h>

#include "../structs.h"
#include "../query/result.h"
#include "../evaluate/evaluate.h"
#include "../evaluate/predicates.h"
#include "../functions/util.h"

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
                && getRowList(row_list)->row_count >= (unsigned)step->limit
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
    if (step->limit >= 0 && (unsigned)step->limit < getRowList(row_list)->row_count) {
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
    int offset = step->limit;

    // After GROUP / GROUP SORTED each group is its own row list on the stack
    // (oldest at index 0). Offset must skip group lists, not rows inside the
    // newest list.
    if (
        result_set->count > 1 &&
        getRowList(result_set->row_list_indices[result_set->count - 1])->group)
    {
        if (offset >= result_set->count)
        {
            while (result_set->count > 0)
            {
                destroyRowList(popRowList(result_set));
            }
            return 0;
        }

        for (int i = 0; i < offset; i++)
        {
            destroyRowList(result_set->row_list_indices[i]);
        }

        memmove(
            result_set->row_list_indices,
            result_set->row_list_indices + offset,
            sizeof(*result_set->row_list_indices) * (result_set->count - offset)
        );
        result_set->count -= offset;

        return 0;
    }

    RowListIndex src_list = popRowList(result_set);

    int join_count = getRowList(src_list)->join_count;
    int row_count = getRowList(src_list)->row_count;

    RowListIndex dest_list = createRowList(join_count, row_count);

    for (int i = offset; i < row_count; i++) {
        copyResultRow(getRowList(dest_list), getRowList(src_list), i);
    }

    pushRowList(result_set, dest_list);

    destroyRowList(src_list);

    return 0;
}

/**
 * Filter group RowLists on the ResultSet stack using HAVING predicates.
 * Aggregates in the predicates are evaluated over each group list.
 */
int executeHaving (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
) {
    int write = 0;

    for (int i = 0; i < result_set->count; i++)
    {
        RowListIndex list_id = result_set->row_list_indices[i];

        // Aggregates ignore the row index; use 0 as a stand-in.
        int match = evaluateOperatorNodeListAND(
            tables,
            list_id,
            0,
            step->nodes,
            step->node_count
        );

        if (match)
        {
            result_set->row_list_indices[write++] = list_id;
        }
        else
        {
            destroyRowList(list_id);
        }
    }

    result_set->count = write;

    return 0;
}

static int rowsEqualOnNodes (
    struct Table *tables,
    RowListIndex list_id,
    int index_a,
    int index_b,
    struct Node *nodes,
    int node_count
) {
    char value_a[MAX_VALUE_LENGTH];
    char value_b[MAX_VALUE_LENGTH];

    for (int i = 0; i < node_count; i++)
    {
        evaluateNode(
            tables,
            list_id,
            index_a,
            &nodes[i],
            value_a,
            MAX_VALUE_LENGTH
        );
        evaluateNode(
            tables,
            list_id,
            index_b,
            &nodes[i],
            value_b,
            MAX_VALUE_LENGTH
        );

        int result;
        if (is_complex(value_a) || is_complex(value_b))
        {
            result = compare_complex(value_a, value_b);
        }
        else if (is_numeric(value_a) && is_numeric(value_b))
        {
            result = compare_numeric(value_a, value_b);
        }
        else
        {
            result = strcmp(value_a, value_b);
        }

        if (result != 0)
        {
            return 0;
        }
    }

    return 1;
}

/**
 * Collapse adjacent duplicate rows (list must already be sorted on the same
 * keys). Keeps the first row of each peer group — DISTINCT ON semantics.
 */
int executeDistinct (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
) {
    RowListIndex src_list = popRowList(result_set);

    int join_count = getRowList(src_list)->join_count;
    int row_count = getRowList(src_list)->row_count;

    RowListIndex dest_list = createRowList(join_count, row_count);

    int prev_kept = -1;

    for (int i = 0; i < row_count; i++)
    {
        if (
            prev_kept < 0 ||
            !rowsEqualOnNodes(
                tables,
                src_list,
                prev_kept,
                i,
                step->nodes,
                step->node_count
            )
        )
        {
            copyResultRow(getRowList(dest_list), getRowList(src_list), i);
            prev_kept = i;
        }
    }

    pushRowList(result_set, dest_list);

    destroyRowList(src_list);

    return 0;
}
