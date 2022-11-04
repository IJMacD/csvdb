#include <stdlib.h>
#include <string.h>

#include "structs.h"
#include "result.h"
#include "sort-quick.h"
#include "evaluate.h"

int executeSort (struct Query *query, struct PlanStep *step, struct ResultSet *result_set) {
    // debugRowList(row_list, 2);

    RowListIndex row_list = popRowList(result_set);

    enum Order sort_directions[10];
    struct ColumnNode *columns = malloc(sizeof(*columns) * step->predicate_count);

    for (int i = 0; i < step->predicate_count && i < 10; i++) {
        memcpy(columns + i, &step->predicates[i].left, sizeof(*columns));
        sort_directions[i] = step->predicates[i].op;
    }

    sortQuick(query, columns, step->predicate_count, sort_directions, getRowList(row_list));

    pushRowList(result_set, row_list);

    // debugRowList(getRowList(row_list), 2);

    return 0;
}

int executeReverse (__attribute__((unused)) struct Query *query, struct PlanStep *step, struct ResultSet *result_set) {

    RowListIndex row_list = popRowList(result_set);
    reverseRowList(getRowList(row_list), step->limit);
    pushRowList(result_set, row_list);

    return 0;
}

int executeGroup (struct Query *query, struct PlanStep *step, struct ResultSet *result_set) {

    // Important! PLAN_GROUP requires rows are already sorted in
    // GROUP BY order

    char values[2][MAX_VALUE_LENGTH] = {0};

    RowListIndex row_list = popRowList(result_set);

    int limit = getRowList(row_list)->row_count;
    if (step->limit > -1 && step->limit < limit) {
        limit = step->limit;
    }

    int count = 0;

    RowListIndex curr_list = -1;

    struct ColumnNode *col = &step->predicates[0].left;

    int join_count = getRowList(row_list)->join_count;
    int row_count = getRowList(row_list)->row_count;

    // debugRowList(getRowList(row_list), 2);

    for (int i = 0; i < getRowList(row_list)->row_count; i++) {
        char *curr_value = values[i%2];
        char *prev_value = values[(i+1)%2];

        evaluateNode(query, getRowList(row_list), i, col, curr_value, MAX_VALUE_LENGTH);

        if (strcmp(prev_value, curr_value)) {
            if (count >= limit) {
                break;
            }

            curr_list = createRowList(join_count, row_count - i);
            pushRowList(result_set, curr_list);
            count++;
        }

        copyResultRow(getRowList(curr_list), getRowList(row_list), i);
    }

    destroyRowList(getRowList(row_list));

    return 0;
}