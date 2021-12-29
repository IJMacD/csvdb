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

        if (p->left.field == FIELD_CONSTANT) {
            strcpy(value_left, p->left.text);
        } else if (p->left.field >= 0) {
            int row_id_left = getRowID(source_list, p->left.table_id, i);
            getRecordValue(query->tables[p->left.table_id].db, row_id_left, p->left.field, value_left, VALUE_MAX_LENGTH);
        } else {
            fprintf(stderr, "Cannot evaluate predicate column: %s\n", p->left.text);
            exit(-1);
        }

        if (p->right.field == FIELD_CONSTANT) {
            strcpy(value_right, p->right.text);
        } else if (p->right.field >= 0) {
            int row_id_right = getRowID(source_list, p->right.table_id, i);
            getRecordValue(query->tables[p->right.table_id].db, row_id_right, p->right.field, value_right, VALUE_MAX_LENGTH);
        } else {
            fprintf(stderr, "Cannot evaluate predicate column: %s\n", p->right.text);
            exit(-1);
        }

        if (evaluateExpression(p->op, value_left, value_right)) {
            // Add to result set
            copyResultRow(target_list, source_list, i);
        }
    }

    return target_list->row_count;
}
