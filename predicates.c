#include <stdlib.h>
#include <string.h>
#include "predicates.h"
#include "query.h"
#include "limits.h"
#include "util.h"

char parseOperator (const char *input) {
    if (strcmp(input, "=") == 0)
        return OPERATOR_EQ;
    if (strcmp(input, "!=") == 0)
        return OPERATOR_NE;
    if (strcmp(input, "IS") == 0)
        return OPERATOR_EQ;
    if (strcmp(input, "<") == 0)
        return OPERATOR_LT;
    if (strcmp(input, "<=") == 0)
        return OPERATOR_LE;
    if (strcmp(input, ">") == 0)
        return OPERATOR_GT;
    if (strcmp(input, ">=") == 0)
        return OPERATOR_GE;
    return OPERATOR_UN;
}

int evaluateExpression (char op, const char *left, const char *right) {
    // printf("Evaluating %s OP %s\n", left, right);

    if (strcmp(right, "NULL") == 0) {
        size_t len = strlen(left);

        if (op == OPERATOR_EQ) return len == 0;
        if (op == OPERATOR_NE) return len != 0;

        return 0;
    }

    if (strcmp(left, "NULL") == 0) {
        size_t len = strlen(right);

        if (op == OPERATOR_EQ) return len == 0;
        if (op == OPERATOR_NE) return len != 0;

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

    return 0;
}


int compare (int numeric_mode, const char * valueA, long valueA_numeric, const char *valueB) {
    if (numeric_mode) {
        long valueB_numeric = atol(valueB);
        return valueA_numeric - valueB_numeric;
    } else {
        return strcmp(valueA, valueB);
    }
}