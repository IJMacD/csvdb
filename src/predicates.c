#include <stdlib.h>
#include <string.h>
#include "predicates.h"
#include "query.h"
#include "limits.h"
#include "util.h"
#include "date.h"

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

int evaluateExpression (int op, const char *left, const char *right) {
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
 * Returns -ve if left is less than right
 * Returns   0 if left are right are equal
 * Returns +ve if left is greater than right
 */
int compareValues (const char *left, const char *right) {
    // printf("Comparing %s to %s\n", left, right);

    // TODO: Handle null

    if (is_numeric(left)) {
        long left_num = strtol(left, NULL, 10);
        long right_num = strtol(right, NULL, 10);

        return left_num - right_num;
    }

    return strcmp(left, right);
}


int compare (int numeric_mode, const char * valueA, long valueA_numeric, const char *valueB) {
    if (numeric_mode) {
        long valueB_numeric = atol(valueB);
        return valueA_numeric - valueB_numeric;
    } else {
        return strcmp(valueA, valueB);
    }
}
