#include <stdlib.h>
#include <string.h>

#include "evaluate.h"
#include "date.h"
#include "function.h"

int evaluateNode (struct Query * query, struct RowList *rowlist, int index, struct ColumnNode * column, char * value, int max_length) {
    if (column->field == FIELD_ROW_INDEX) {
        sprintf(value, "%d", index);
        return 0;
    }

    if (column->field == FIELD_CONSTANT) {
        evaluateConstantNode(column, value, max_length);
        return evaluateFunction(value, NULL, column, -1);
    }

    if (column->field >= 0) {
        int row_id = getRowID(rowlist, column->table_id, index);
        struct DB * db = query->tables[column->table_id].db;
        return evaluateFunction(value, db, column, row_id);
    }

    fprintf(stderr, "Cannot evaluate column: %s\n", column->text);
    exit(-1);
}

int evaluateConstantNode (struct ColumnNode * column, char * value, __attribute__((unused)) int max_length) {
    if (column->field != FIELD_CONSTANT) {
        fprintf(stderr, "Tried to evaluate non-contant value as constant: %s\n", column->text);
        exit(-1);
    }

    if (strcmp(column->text, "CURRENT_DATE") == 0
        || strcmp(column->text, "TODAY()") == 0)
    {
        struct DateTime dt;
        parseDateTime("CURRENT_DATE", &dt);
        sprintf(value, "%04d-%02d-%02d", dt.year, dt.month, dt.day);
    }

    return 0;
}