#include "db.h"
#include "limits.h"

int filterRows (struct DB *db, int *source_rowids, int source_count, struct Predicate *p, int *result_rowids) {
    int result_count = 0;
    int predicate_field_index = getFieldIndex(db, p->field);

    for (int i = 0; i < source_count; i++) {
        char value[VALUE_MAX_LENGTH] = {0};
        getRecordValue(db, source_rowids[i], predicate_field_index, value, VALUE_MAX_LENGTH);

        if (evaluateExpression(p->op, value, p->value)) {
            // Add to result set
            result_rowids[result_count++] = source_rowids[i];
        }
    }

    return result_count;
}
