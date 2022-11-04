#include <stdlib.h>
#include <string.h>

#include "structs.h"
#include "predicates.h"
#include "date.h"
#include "util.h"

int parseOperator (const char *input) {
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
    if (strcmp(input, "LIKE") == 0)
        return OPERATOR_LIKE;
    return OPERATOR_UN;
}

int evaluateExpression (enum Operator op, const char *left, const char *right) {
    // printf("Evaluating %s OP %s\n", left, right);

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

    return 0;
}

/**
 * @brief Will ensure field is on left and constant is on right
 *
 * @param p
 */
void normalisePredicate (struct Predicate *p) {
    if (p->left.fields[0].index == FIELD_CONSTANT && p->right.fields[0].index >= 0) {
        flipPredicate(p);
    } else if (p->left.function != FUNC_PK && p->right.function == FUNC_PK) {
        flipPredicate(p);
    }
}

int flipPredicate (struct Predicate *p) {
    // copy struct automatically
    struct ColumnNode tmp = p->left;

    // swap
    memcpy(&p->left, &p->right, sizeof(tmp));
    memcpy(&p->right, &tmp, sizeof(tmp));

    // flip operator as necessary
    if (p->op == OPERATOR_LT) {
        p->op = OPERATOR_GT;
    } else if (p->op == OPERATOR_LE) {
        p->op = OPERATOR_GE;
    } else if (p->op == OPERATOR_GT) {
        p->op = OPERATOR_LT;
    } else if (p->op == OPERATOR_GE) {
        p->op = OPERATOR_LE;
    } else if (p->op == OPERATOR_EQ) {
        // no op
    } else {
        // Unable to swap
        return -1;
    }

    return 0;
}