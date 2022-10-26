#include <stdlib.h>
#include <string.h>

#include "evaluate.h"
#include "date.h"
#include "function.h"

int evaluateNode (struct Query * q, struct RowList *rowlist, int index, struct ColumnNode * column, char * value, int max_length) {
    struct Field *field = column->fields;

    if (field->index == FIELD_ROW_INDEX) {
        sprintf(value, "%d", index);
        return 0;
    }

    if (field->index == FIELD_CONSTANT) {
        evaluateConstantNode(column, value, max_length);
        return evaluateFunction(value, NULL, column, -1);
    }

    if (field->index >= 0) {
        int row_id = getRowID(rowlist, field->table_id, index);
        return evaluateFunction(value, q->tables, column, row_id);
    }

    fprintf(stderr, "Cannot evaluate column: %s\n", field->text);
    exit(-1);
}

int evaluateConstantNode (struct ColumnNode * column, char * value, __attribute__((unused)) int max_length) {
    struct Field *field = column->fields;

    if (field->index != FIELD_CONSTANT) {
        fprintf(stderr, "Tried to evaluate non-contant value as constant: %s\n", field->text);
        exit(-1);
    }

    if (strcmp(field->text, "CURRENT_DATE") == 0
        || strcmp(field->text, "TODAY()") == 0)
    {
        struct DateTime dt;
        parseDateTime("CURRENT_DATE", &dt);
        sprintf(value, "%04d-%02d-%02d", dt.year, dt.month, dt.day);
    }

    return 0;
}