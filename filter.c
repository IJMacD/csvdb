#include "db.h"
#include "limits.h"
#include "query.h"

int filterRows (struct Query *query, struct RowList *source_list, struct Predicate *p, struct RowList *target_list) {
    int source_count = source_list->row_count;

    int table_id;
    int column_id;

    findColumn(query, p->field, &table_id, &column_id);

    target_list->row_count = 0;

    for (int i = 0; i < source_count; i++) {
        char value[VALUE_MAX_LENGTH] = {0};
        int row_id = getRowID(source_list, table_id, i);
        getRecordValue(query->tables[table_id].db, row_id, column_id, value, VALUE_MAX_LENGTH);

        if (evaluateExpression(p->op, value, p->value)) {
            // Add to result set
            copyResultRow(target_list, source_list, i);
        }
    }

    return target_list->row_count;
}
