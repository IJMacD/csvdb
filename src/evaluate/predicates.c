#include <stdlib.h>
#include <string.h>

#include "./predicates.h"
#include "./evaluate.h"
#include "../structs.h"
#include "../query/result.h"
#include "../functions/date.h"
#include "../functions/util.h"

/**
 * Evaluate an OPERATOR node
 */
int evaluateOperatorNode (struct Table *tables, int row_list, int row_index, struct Node *node) {
    if ((node->function & MASK_FUNC_FAMILY) != FUNC_FAM_OPERATOR) {
        fprintf(
            stderr,
            "Node must be an operator. Found function 0x%X\n",
            node->function
        );
        exit(-1);
    }

    if (node->function == OPERATOR_NEVER) {
        return 0;
    }

    if (node->function == OPERATOR_ALWAYS) {
        return 1;
    }

    if (node->function == OPERATOR_OR) {
        return evaluateOperatorNodeListOR(tables, row_list, row_index, node->children, node->child_count);
    }

    if (node->function == OPERATOR_AND) {
        return evaluateOperatorNodeListAND(tables, row_list, row_index, node->children, node->child_count);
    }

    char value_left[MAX_VALUE_LENGTH] = {0};
    char value_right[MAX_VALUE_LENGTH] = {0};

    evaluateNode(
        tables,
        getRowList(row_list),
        row_index,
        &node->children[0],
        value_left,
        MAX_VALUE_LENGTH
    );
    evaluateNode(
        tables,
        getRowList(row_list),
        row_index,
        &node->children[1],
        value_right,
        MAX_VALUE_LENGTH
    );

    if (!evaluateExpression(node->function, value_left, value_right)) {
        return 0;
    }

    return 1;
}

/**
 * Evaluate a list of OPERATOR nodes
 * returns 1 if ALL of the nodes match
 */
int evaluateOperatorNodeListAND (struct Table *tables, int row_list, int row_index, struct Node *nodes, int node_count) {
    for (int j = 0; j < node_count; j++) {
        struct Node * p = nodes + j;

        if (!evaluateOperatorNode(tables, row_list, row_index, p)) {
            return 0;
        }
    }

    return 1;
}

/**
 * Evaluate a list of OPERATOR nodes
 * returns 1 if ANY of the nodes match
 */
int evaluateOperatorNodeListOR (struct Table *tables, int row_list, int row_index, struct Node *nodes, int node_count) {
    for (int j = 0; j < node_count; j++) {
        struct Node * p = nodes + j;

        if (evaluateOperatorNode(tables, row_list, row_index, p)) {
            return 1;
        }
    }

    return 0;
}

int evaluateExpression (enum Function op, const char *left, const char *right) {
    // printf("Evaluating %s OP %s\n", left, right);

    if (op == OPERATOR_NEVER) {
        return 0;
    }

    if (op == OPERATOR_ALWAYS) {
        return 1;
    }

    struct DateTime dt_left, dt_right;
    if (parseDateTime(left, &dt_left) && parseDateTime(right, &dt_right)) {
        // Date comparison
        int julian_left = datetimeGetJulian(&dt_left);
        int julian_right = datetimeGetJulian(&dt_right);

        if (op == OPERATOR_EQ) return julian_left == julian_right;
        if (op == OPERATOR_NE) return julian_left != julian_right;
        if (op == OPERATOR_LT) return julian_left < julian_right;
        if (op == OPERATOR_LE) return julian_left <= julian_right;
        if (op == OPERATOR_GT) return julian_left > julian_right;
        if (op == OPERATOR_GE) return julian_left >= julian_right;

        return 0;
    }

    if (op == OPERATOR_LIKE) {
        size_t len = strlen(right);
        if (right[len-1] == '%') {
            return strncmp(left, right, len -1) == 0;
        }

        return strcmp(left, right) == 0;
    }

    size_t l_len = strlen(left);
    size_t r_len = strlen(right);

    if (strcmp(right, "NULL") == 0) {

        if (op == OPERATOR_EQ) return l_len == 0;
        if (op == OPERATOR_NE) return l_len != 0;

        return 0;
    }

    if (strcmp(left, "NULL") == 0) {

        if (op == OPERATOR_EQ) return r_len == 0;
        if (op == OPERATOR_NE) return r_len != 0;

        return 0;
    }

    // NULL values do not evaluate true with any other operator
    if (l_len == 0 || r_len == 0) {
        return 0;
    }

    if (is_numeric(left)) {
        long left_num = strtol(left, NULL, 10);
        long right_num = strtol(right, NULL, 10);

        if (op == OPERATOR_EQ) return left_num == right_num;
        if (op == OPERATOR_NE) return left_num != right_num;
        if (op == OPERATOR_LT) return left_num < right_num;
        if (op == OPERATOR_LE) return left_num <= right_num;
        if (op == OPERATOR_GT) return left_num > right_num;
        if (op == OPERATOR_GE) return left_num >= right_num;
    }

    if (op == OPERATOR_EQ) return strcmp(left, right) == 0;
    if (op == OPERATOR_NE) return strcmp(left, right) != 0;
    if (op == OPERATOR_LT) return strcmp(left, right) < 0;
    if (op == OPERATOR_LE) return strcmp(left, right) <= 0;
    if (op == OPERATOR_GT) return strcmp(left, right) > 0;
    if (op == OPERATOR_GE) return strcmp(left, right) >= 0;

    fprintf(stderr, "Unrecognised operator: %d\n", op);
    exit(-1);
}

/**
 * @brief Will ensure field is on left and constant is on right
 *
 * @param p
 */
void normalisePredicate (struct Node *predicate) {
    struct Node *left = &predicate->children[0];
    struct Node *right = &predicate->children[1];

    if (left->field.index == FIELD_CONSTANT && right->field.index >= 0) {
        flipPredicate(predicate);
    } else if (left->function != FUNC_PK && right->function == FUNC_PK) {
        flipPredicate(predicate);
    }
}

int flipPredicate (struct Node *predicate) {
    struct Node *left = &predicate->children[0];
    struct Node *right = &predicate->children[1];

    // copy struct automatically
    struct Node tmp = *left;

    // swap
    memcpy(left, right, sizeof(tmp));
    memcpy(right, &tmp, sizeof(tmp));

    enum Function op = predicate->function;

    // flip operator as necessary
    if (op == OPERATOR_LT) {
        op = OPERATOR_GT;
    } else if (op == OPERATOR_LE) {
        op = OPERATOR_GE;
    } else if (op == OPERATOR_GT) {
        op = OPERATOR_LT;
    } else if (op == OPERATOR_GE) {
        op = OPERATOR_LE;
    } else if (op == OPERATOR_EQ) {
        // no op
    } else {
        // Unable to swap
        return -1;
    }

    predicate->function = op;

    return 0;
}