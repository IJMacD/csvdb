#include <stdlib.h>
#include <string.h>

#include "../structs.h"
#include "evaluate.h"
#include "function.h"
#include "../query/result.h"
#include "../db/db.h"
#include "../functions/date.h"
#include "../functions/util.h"

/**
 * @brief
 *
 * @param q
 * @param rowlist
 * @param index
 * @param column
 * @param value
 * @param max_length
 * @return int Number of bytes written
 */
int evaluateNode (
    struct Query * q,
    struct RowList *rowlist,
    int index,
    struct ColumnNode * column,
    char * output,
    __attribute__((unused)) int max_length
) {

    char value1[MAX_VALUE_LENGTH];
    char value2[MAX_VALUE_LENGTH];

    char *values[2];
    values[0] = value1;
    values[1] = value2;

    evaluateField(value1, q->tables, rowlist, &(column->fields[0]), index);

    if (column->function == FUNC_UNITY) {
        strcpy(output, value1);
        return strlen(output);
    }

    evaluateField(value2, q->tables, rowlist, &(column->fields[1]), index);

    return evaluateFunction(output, column->function, values, 2);
}

/**
 * @brief Evaluate a set of columns into a single sortable string using \x1f to
 * separate fields.
 *
 * @param query
 * @param rowlist
 * @param index
 * @param columns
 * @param column_count
 * @param output
 * @param max_length
 * @return int Number of bytes written
 */
int evaluateNodeList (
    struct Query * query,
    struct RowList *rowlist,
    int index,
    struct ColumnNode * columns,
    int column_count,
    char * output,
    __attribute__ ((unused)) int max_length
) {
    char value[MAX_VALUE_LENGTH];
    int bytes_written = 0;

    for (int j = 0; j < column_count; j++) {
        int count = evaluateNode(
            query,
            rowlist,
            index,
            &columns[j],
            value,
            sizeof(value)
        );

        // Numeric values need to be fixed width for comparison.
        // After testing it make no difference whether numeric values are
        // compared or strings are compared. (There are other slower steps).
        if (is_numeric(value)) {
            count = sprintf(output, "%020ld", atol(value));
        }
        else {
            strcpy(output, value);
        }

        output += count;
        *(output++) = '\x1f'; // Field separator

        bytes_written += count + 1;
    }

    return bytes_written;
}

/**
 * @brief
 *
 * @param output
 * @param tables
 * @param rowlist
 * @param field
 * @param result_index
 * @return int number of chars written
 */
int evaluateField (
    char * output,
    struct Table *tables,
    struct RowList *rowlist,
    struct Field *field,
    int result_index
) {

    if (field->index == FIELD_CONSTANT) {
        return evaluateConstantField(output, field);
    }

    if (field->table_id >= 0) {
        int row_id = getRowID(rowlist, field->table_id, result_index);

        if (field->index == FIELD_ROW_INDEX) {
            return sprintf(output, "%d", row_id);
        }

        struct DB *db = tables[field->table_id].db;

        return getRecordValue(
            db,
            row_id,
            field->index,
            output,
            MAX_VALUE_LENGTH
        ) > 0;
    }

    output[0] = '\0';

    return 0;
}

/**
 * @brief
 *
 * @param value
 * @param field
 * @return int Number of chars written
 */
int evaluateConstantField (char * value, struct Field * field) {

    if (field->index != FIELD_CONSTANT) {
        fprintf(
            stderr,
            "Tried to evaluate non-contant value as constant: %s\n",
            field->text
        );
        exit(-1);
    }

    if (
        field->text[0] == '0'
        && field->text[1] == 'x'
    ) {
        long val = strtol(field->text, NULL, 16);
        return sprintf(value, "%ld", val);
    }

    if (strcmp(field->text, "CURRENT_DATE") == 0
        || strcmp(field->text, "TODAY()") == 0)
    {
        struct DateTime dt;
        parseDateTime("CURRENT_DATE", &dt);
        return sprintf(value, "%04d-%02d-%02d", dt.year, dt.month, dt.day);
    }

    return sprintf(value, "%s", field->text);
}