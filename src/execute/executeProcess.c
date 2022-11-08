#include <stdlib.h>
#include <string.h>

#include "../structs.h"
#include "../query/result.h"
#include "../sort/sort-quick.h"
#include "../evaluate/evaluate.h"

int executeSort (
    struct Query *query,
    struct PlanStep *step,
    struct ResultSet *result_set
) {
    // debugRowList(row_list, 2);

    RowListIndex row_list = popRowList(result_set);

    enum Order sort_directions[10];
    struct Node *nodes = malloc(sizeof(*nodes) * step->predicate_count);

    for (int i = 0; i < step->predicate_count && i < 10; i++) {
        memcpy(nodes + i, &step->predicates[i].left, sizeof(*nodes));
        sort_directions[i] = (enum Order)step->predicates[i].op;
    }

    sortQuick(
        query->tables,
        nodes,
        step->predicate_count,
        sort_directions,
        getRowList(row_list)
    );

    pushRowList(result_set, row_list);

    // debugRowList(getRowList(row_list), 2);

    return 0;
}

int executeReverse (
    __attribute__((unused)) struct Query *query,
    struct PlanStep *step,
    struct ResultSet *result_set
) {

    RowListIndex row_list = popRowList(result_set);
    reverseRowList(getRowList(row_list), step->limit);
    pushRowList(result_set, row_list);

    return 0;
}

/**
 * @brief Group rows which are already sorted in the correct order. Probably
 * about the same speed but this can offer moderate memory usage improvements.
 *
 * @param query
 * @param step
 * @param result_set
 * @return int 0 for success
 */
int executeGroupSorted (
    struct Query *query,
    struct PlanStep *step,
    struct ResultSet *result_set
) {

    // Important! PLAN_GROUP_SORTED requires rows are already sorted in
    // GROUP BY order

    if (step->predicate_count > 1) {
        fprintf(
            stderr,
            "Unable to do sorted group by with more than one predicate.\n"
        );
        exit(-1);
    }

    char values[2][MAX_VALUE_LENGTH] = {0};

    RowListIndex row_list = popRowList(result_set);

    int limit = getRowList(row_list)->row_count;
    if (step->limit > -1 && step->limit < limit) {
        limit = step->limit;
    }

    int count = 0;

    RowListIndex curr_list = -1;

    struct Node *col = &step->predicates[0].left;

    int join_count = getRowList(row_list)->join_count;
    int row_count = getRowList(row_list)->row_count;

    // debugRowList(getRowList(row_list), 2);

    for (int i = 0; i < getRowList(row_list)->row_count; i++) {
        char *curr_value = values[i%2];
        char *prev_value = values[(i+1)%2];

        evaluateNode(
            query->tables,
            getRowList(row_list),
            i,
            col,
            curr_value,
            MAX_VALUE_LENGTH
        );

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

    destroyRowList(row_list);

    return 0;
}

/**
 * @brief Group items by bucket index lookup
 *
 * @param query
 * @param step
 * @param result_set
 * @return int 0 for success
 */
int executeGroupBucket (
    struct Query *query,
    struct PlanStep *step,
    struct ResultSet *result_set
) {
    // Prepare group nodes

    struct Node *group_nodes
        = malloc(sizeof(*group_nodes) * step->predicate_count);

    for (int i = 0; i < step->predicate_count; i++) {
        memcpy(
            &group_nodes[i],
            &step->predicates[i].left,
            sizeof(*group_nodes)
        );
    }

    // Get RowList
    RowListIndex row_list = popRowList(result_set);

    int bucket_count = 0;
    char (*bucket_keys)[MAX_VALUE_LENGTH] = NULL;
    RowListIndex *buckets = NULL;

    int join_count = getRowList(row_list)->join_count;
    int row_count = getRowList(row_list)->row_count;

    // Iterate rows

    for (int i = 0; i < getRowList(row_list)->row_count; i++) {
        char value[MAX_VALUE_LENGTH];

        // Evaluate group key
        evaluateNodeList(
            query->tables,
            getRowList(row_list),
            i,
            group_nodes,
            step->predicate_count,
            value,
            MAX_VALUE_LENGTH
        );

        // Find bucket index;
        int bucket_index = -1;
        for (int i = 0; i < bucket_count; i++) {
            if (strcmp(value, bucket_keys[i]) == 0) {
                bucket_index = i;
                break;
            }
        }

        if (bucket_index == -1) {
            // Bucket not found. We need to make a new bucket

            if (bucket_keys == NULL) {
                bucket_keys = malloc(sizeof(*bucket_keys));
                buckets = malloc(sizeof(*buckets));
            }
            else {
                bucket_keys = realloc(
                    bucket_keys,
                    sizeof(*bucket_keys) * (bucket_count + 1)
                );
                buckets = realloc(
                    buckets,
                    sizeof(*buckets) * (bucket_count + 1)
                );

                if (bucket_keys == NULL || buckets == NULL) {
                    fprintf(
                        stderr,
                        "Unable to allocate space for %d buckets.\n",
                        bucket_count + 1
                    );
                    exit(-1);
                }
            }

            strcpy(bucket_keys[bucket_count], value);
            buckets[bucket_count] = createRowList(join_count, row_count - i);

            pushRowList(result_set, buckets[bucket_count]);

            bucket_index = bucket_count++;
        }

        copyResultRow(
            getRowList(buckets[bucket_index]),
            getRowList(row_list),
            i
        );
    }

    // Discard any buckets more than the limit
    if (step->limit > -1) {
        while (bucket_count > step->limit) {
            RowListIndex list = popRowList(result_set);
            destroyRowList(list);
            bucket_count--;
        }
    }

    destroyRowList(row_list);

    return 0;
}