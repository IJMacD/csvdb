#include <stdlib.h>
#include <string.h>

#include "../structs.h"
#include "evaluate.h"
#include "function.h"
#include "../query/result.h"
#include "../db/db.h"
#include "../functions/date.h"
#include "../functions/util.h"

static int evaluateField (
    struct Table *tables,
    struct RowList *rowlist,
    int index,
    struct Field *field,
    char * output,
    int max_length
);

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
    struct Table *tables,
    struct RowList *rowlist,
    int index,
    struct Node *node,
    char *output,
    int max_length
) {
    if (node->function == FUNC_UNITY) {
        // With FUNC_UNITY we'll just output directly to parent
        return evaluateField(
            tables,
            rowlist,
            index,
            (struct Field *)node,
            output,
            max_length
        );
    }

    if (node->child_count == -1) {
        // Optimistation where node is its own child

        char value[MAX_VALUE_LENGTH];
        evaluateField(
            tables,
            rowlist,
            index,
            (struct Field *)node,
            value,
            MAX_VALUE_LENGTH
        );

        char *values[] = { value };

        return evaluateFunction(output, node->function, (char **)values, 1);
    }

    char *values = malloc(node->child_count * MAX_VALUE_LENGTH);
    char **values_ptrs = malloc(node->child_count * sizeof(values));
    for (int i = 0; i < node->child_count; i++) {
        values_ptrs[i] = values + MAX_FIELD_LENGTH * i;
    }

    if (values == NULL) {
        fprintf(
            stderr,
            "Unable to allocate %d bytes for %d child node values\n",
            node->child_count * MAX_VALUE_LENGTH,
            node->child_count
        );
        exit(-1);
    }

    for (int i = 0; i < node->child_count; i++) {
        evaluateNode(
            tables,
            rowlist,
            index,
            &node->children[i],
            values_ptrs[i],
            MAX_VALUE_LENGTH
        );
    }

    int result = evaluateFunction(
        output,
        node->function,
        values_ptrs,
        node->child_count
    );

    free(values);
    free(values_ptrs);

    return result;
}

/**
 * @brief Evaluate a set of nodes into a single sortable string using \x1f to
 * separate fields.
 *
 * @param query
 * @param rowlist
 * @param index
 * @param nodes
 * @param node_count
 * @param output
 * @param max_length
 * @return int Number of bytes written
 */
int evaluateNodeList (
    struct Table * tables,
    struct RowList *rowlist,
    int index,
    struct Node *nodes,
    int node_count,
    char *output,
    __attribute__ ((unused)) int max_length
) {
    char value[MAX_VALUE_LENGTH];
    int bytes_written = 0;

    for (int j = 0; j < node_count; j++) {
        int count = evaluateNode(
            tables,
            rowlist,
            index,
            &nodes[j],
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

    *output = '\0';

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
static int evaluateField (
    struct Table *tables,
    struct RowList *rowlist,
    int index,
    struct Field *field,
    char * output,
    int max_length
) {

    if (field->index == FIELD_CONSTANT) {
        return evaluateConstantField(output, field);
    }

    if (field->table_id >= 0) {
        int row_id = getRowID(rowlist, field->table_id, index);

        if (field->index == FIELD_ROW_INDEX) {
            return sprintf(output, "%d", row_id);
        }

        struct DB *db = tables[field->table_id].db;

        return getRecordValue(
            db,
            row_id,
            field->index,
            output,
            max_length
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
int evaluateConstantField (char * value, struct Field *field) {

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

int isConstantNode (struct Node *node) {
    if (node->function == FUNC_UNITY || node->child_count == -1) {
        return node->field.index == FIELD_CONSTANT;
    }

    for (int i = 0; i < node->child_count; i++) {
        if (isConstantNode(&node->children[i]) == 0) {
            return 0;
        }
    }

    return 1;
}