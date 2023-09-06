#include <stdlib.h>
#include <string.h>

#include "../structs.h"
#include "evaluate.h"
#include "function.h"
#include "predicates.h"
#include "../query/node.h"
#include "../query/result.h"
#include "../db/db.h"
#include "../functions/date.h"
#include "../functions/util.h"

static int evaluateField (
    struct Table *tables,
    RowListIndex row_list,
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
    RowListIndex row_list,
    int index,
    struct Node *node,
    char *output,
    int max_length
) {
    if (node->function == FUNC_UNITY) {
        // With FUNC_UNITY we'll just output directly to parent
        return evaluateField(
            tables,
            row_list,
            index,
            (struct Field *)node,
            output,
            max_length
        );
    }

    if ((node->function & MASK_FUNC_FAMILY) == FUNC_FAM_OPERATOR) {
        int result = evaluateOperatorNode(tables, row_list, index, node);

        sprintf(output, "%s", result ? "1" : "0");

        return 1;
    }

    if (node->child_count == -1) {
        // Optimistation where node is its own child

        char value[MAX_VALUE_LENGTH];
        evaluateField(
            tables,
            row_list,
            index,
            (struct Field *)node,
            value,
            MAX_VALUE_LENGTH
        );

        char *values[] = { value };

        return evaluateFunction(output, node->function, (char **)values, 1);
    }

    int value_count = node->child_count;
    if (value_count == 0) {
        value_count = 1;
    }

    char *values = malloc(value_count * MAX_VALUE_LENGTH);
    char **values_ptrs = malloc(value_count * sizeof(values));
    for (int i = 0; i < value_count; i++) {
        values_ptrs[i] = values + MAX_FIELD_LENGTH * i;
    }

    if (values == NULL) {
        fprintf(
            stderr,
            __FILE__ ":%d Unable to allocate %d bytes for %d child node values\n", __LINE__,
            value_count * MAX_VALUE_LENGTH,
            value_count
        );
        exit(-1);
    }

    for (int i = 0; i < node->child_count; i++) {
        evaluateNode(
            tables,
            row_list,
            index,
            &node->children[i],
            values_ptrs[i],
            MAX_VALUE_LENGTH
        );
        // fprintf(stderr, "[EVALUATE] operand = '%s'\n", values_ptrs[i]);
    }

    int result = evaluateFunction(
        output,
        node->function,
        values_ptrs,
        node->child_count
    );
    // fprintf(stderr, "[EVALUATE] return value = '%s'\n", output);

    free(values);
    free(values_ptrs);

    return result;
}

// Evaluates functions on nodes with purely constant children
int evaluateConstantNode (
    struct Node *node,
    char *output
) {
    if (node->function == FUNC_UNITY) {
        // If this node is just unity, then just copy from the field
        strcpy(output, node->field.text);
        return 1;
    }

    int value_count = node->child_count;
    if (value_count <= 0) {
        value_count = 1;
    }

    char *values = malloc(value_count * MAX_VALUE_LENGTH);
    char **values_ptrs = malloc(value_count * sizeof(values));
    for (int i = 0; i < value_count; i++) {
        values_ptrs[i] = values + MAX_FIELD_LENGTH * i;
    }

    if (values == NULL) {
        fprintf(
            stderr,
            __FILE__ ":%d Unable to allocate %d bytes for %d child node values\n", __LINE__,
            value_count * MAX_VALUE_LENGTH,
            value_count
        );
        exit(-1);
    }

    if (node->child_count == -1) {
        // If this node is a self-child node, just copy from the field (since we
        // know the field must be constant)
        strcpy(values_ptrs[0], node->field.text);
    }
    else for (int i = 0; i < node->child_count; i++) {
        struct Node *child_node = &node->children[i];

        if (child_node->function == FUNC_UNITY) {
            evaluateConstantField(values_ptrs[i], &child_node->field);
        }
        else {
            evaluateConstantNode(child_node, values_ptrs[i]);
        }
        // fprintf(stderr, "[EVALUATE] operand = '%s'\n", values_ptrs[i]);
    }

    int result = evaluateFunction(
        output,
        node->function,
        values_ptrs,
        node->child_count
    );
    // fprintf(stderr, "[EVALUATE] return value = '%s'\n", output);

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
    RowListIndex row_list,
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
            row_list,
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
            count = strlen(strcpy(output, value));
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
    RowListIndex row_list,
    int index,
    struct Field *field,
    char * output,
    int max_length
) {
    if (field->index == FIELD_UNKNOWN) {
        fprintf(stderr, "Trying to evaluate unknown field\n");
        exit(-1);
    }

    if (field->index == FIELD_CONSTANT) {
        return evaluateConstantField(output, field);
    }

    if (field->table_id >= 0) {
        int row_id;

        if (row_list == ROWLIST_ROWID) {
            // Special value of row_list means: row_index === row_id
            row_id = index;
        }
        else {
            row_id = getRowID(getRowList(row_list), field->table_id, index);
        }

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
 * @param output
 * @param field
 * @return int Number of chars written
 */
int evaluateConstantField (char * output, struct Field *field) {

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
        return sprintf(output, "%ld", val);
    }

    if (strcmp(field->text, "CURRENT_DATE") == 0) {
        struct DateTime dt;
        parseDateTime("CURRENT_DATE", &dt);
        return sprintDate(output, &dt);
    }

    if (strcmp(field->text, "CURRENT_TIME") == 0) {
        struct DateTime dt;
        parseDateTime("CURRENT_TIME", &dt);
        return sprintf(output, "%02d:%02d:%02d", dt.hour, dt.minute, dt.second);
    }

    // Alpine doesn't like sprintf'ing a buffer into itself
    if (output == field->text) {
        // fprintf(stderr, "[DEBUG] src = dest\n");
        return strlen(output);
    }

    return sprintf(output, "%s", field->text);
}

/**
 * returns 1 if node is entirely constant; 0 if any part of it includes fields.
*/
int isConstantNode (struct Node *node) {
    if (node->function == FUNC_UNITY
        // Optimistation where node is its own child
        || node->child_count == -1
    ) {
        return node->field.index == FIELD_CONSTANT;
    }

    for (int i = 0; i < node->child_count; i++) {
        if (isConstantNode(&node->children[i]) == 0) {
            return 0;
        }
    }

    return 1;
}

/**
 * Evaluates a node partially.
 *
 * Partially evaluates a node up to a certain table_id and leaves other
 * sub-nodes unevaluated.
 *
 * Writes the computed result back into the node as appropriate
 */
void evaluateNodeTreePartial(
    struct Table *tables,
    int list_id,
    int result_index,
    struct Node *node,
    int max_table_id
) {
    int bit_map_limit = 1 << (max_table_id + 1);

    if (getTableBitMap(node) < bit_map_limit) {
        evaluateNode(
            tables,
            list_id,
            result_index,
            node,
            node->field.text,
            MAX_FIELD_LENGTH
        );

        node->function = FUNC_UNITY;
        node->field.index = FIELD_CONSTANT;

        return;
    }

    for (int i = 0; i < node->child_count; i++) {
        struct Node *child = &node->children[i];

        evaluateNodeTreePartial(
            tables,
            list_id,
            result_index,
            child,
            max_table_id
        );
    }
}