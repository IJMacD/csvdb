#include <string.h>
#include <stdlib.h>

#include "../structs.h"
#include "../evaluate/evaluate.h"
#include "../query/result.h"
#include "../functions/util.h"

struct SortContext {
    struct Table *tables;
    struct Node *nodes;
    int node_count;
    enum Order *sort_directions;
    struct RowList *row_list;
};

static void quickSort (struct SortContext *context, int lo, int hi);
static int partition (struct SortContext *context, int lo, int hi);
static void swap (struct SortContext *context, int index_a, int index_b);
static int compare (struct SortContext *context, int index_a, int index_b);

void sortQuick (
    struct Table *tables,
    struct Node *nodes,
    int node_count,
    enum Order *sort_directions,
    struct RowList *row_list
) {
    struct SortContext context = {
        .tables = tables,
        .nodes = nodes,
        .node_count = node_count,
        .sort_directions = sort_directions,
        .row_list = row_list,
    };

    quickSort(&context, 0, row_list->row_count - 1);
}

static void quickSort (struct SortContext *context, int lo, int hi) {
    if (lo >= 0 && hi >= 0 && lo < hi) {
        int pivot = partition(context, lo, hi);
        quickSort(context, lo, pivot);
        quickSort(context, pivot + 1, hi);
    }
}

static int partition (struct SortContext *context, int lo, int hi) {
    int pivot = lo;

    int i = lo - 1;

    int j = hi + 1;

    while (1) {
        do {
            i++;
        } while (compare(context, i, pivot) < 0);

        do {
            j--;
        } while (compare(context, j, pivot) > 0);

        if (i >= j) {
            return j;
        }

        swap(context, i, j);
    }
}

static void swap (struct SortContext *context, int index_a, int index_b) {
    swapRows(context->row_list, index_a, index_b);
}

static int compare (struct SortContext *context, int index_a, int index_b) {
    char value_a[MAX_VALUE_LENGTH];
    char value_b[MAX_VALUE_LENGTH];
    int result;

    for (int i = 0; i < context->node_count; i++) {
        evaluateNode(
            context->tables,
            context->row_list,
            index_a,
            &context->nodes[i],
            value_a,
            MAX_VALUE_LENGTH
        );
        evaluateNode(
            context->tables,
            context->row_list,
            index_b,
            &context->nodes[i],
            value_b,
            MAX_VALUE_LENGTH
        );

        if (is_numeric(value_a) && is_numeric(value_b)) {
            result = atol(value_a) - atol(value_b);
        }
        else {
            result = strcmp(value_a, value_b);
        }

        if (result != 0) {
            if (context->sort_directions[i] == ORDER_DESC) {
                result *= -1;
            }

            return result;
        }
    }

    return 0;
}