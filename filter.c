#include "db.h"
#include "limits.h"

int filterRows (struct DB *db, struct RowList *source_list, struct Predicate *p, struct RowList *target_list) {
    int source_count = source_list->row_count;
    int predicate_field_index = getFieldIndex(db, p->field);

    target_list->row_count = 0;

    for (int i = 0; i < source_count; i++) {
        char value[VALUE_MAX_LENGTH] = {0};
        int row_id = getRowID(source_list, 0, i);
        getRecordValue(db, row_id, predicate_field_index, value, VALUE_MAX_LENGTH);

        if (evaluateExpression(p->op, value, p->value)) {
            // Add to result set
            appendRowID(target_list, row_id);
        }
    }

    return target_list->row_count;
}
