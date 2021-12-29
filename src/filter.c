#include <stdlib.h>
#include <string.h>

#include "db.h"
#include "limits.h"
#include "query.h"

int filterRows (struct Query *query, struct RowList *source_list, struct Predicate *p, struct RowList *target_list) {
    int source_count = source_list->row_count;

    populateColumnNode(query, &p->left);
    populateColumnNode(query, &p->right);

    if (p->left.field == FIELD_UNKNOWN || p->left.table_id < 0) {
        fprintf(stderr, "Predicate column not found: %s\n", p->left.text);
        exit(-1);
    }

    if (p->right.field == FIELD_UNKNOWN || p->right.table_id < 0) {
        fprintf(stderr, "Predicate column not found: %s\n", p->right.text);
        exit(-1);
    }

    target_list->row_count = 0;

    for (int i = 0; i < source_count; i++) {
        char value_left[VALUE_MAX_LENGTH] = {0};
        char value_right[VALUE_MAX_LENGTH] = {0};

        evaluateNode(query, source_list, i, &p->left, value_left, VALUE_MAX_LENGTH);
        evaluateNode(query, source_list, i, &p->right, value_right, VALUE_MAX_LENGTH);

        if (evaluateExpression(p->op, value_left, value_right)) {
            // Add to result set
            copyResultRow(target_list, source_list, i);
        }
    }

    return target_list->row_count;
}
