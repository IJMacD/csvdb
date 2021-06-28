#include <stdlib.h>
#include <string.h>
#include "predicates.h"
#include "query.h"
#include "limits.h"
#include "util.h"

int compare (int numeric_mode, const char * valueA, long valueA_numeric, const char *valueB);

char parseOperator (const char *input) {
    if (strcmp(input, "=") == 0)  return OPERATOR_EQ;
    if (strcmp(input, "!=") == 0) return OPERATOR_NE;
    if (strcmp(input, "IS") == 0) return OPERATOR_EQ;
    if (strcmp(input, "<") == 0)  return OPERATOR_LT;
    if (strcmp(input, "<=") == 0) return OPERATOR_LE;
    if (strcmp(input, ">") == 0)  return OPERATOR_GT;
    if (strcmp(input, ">=") == 0) return OPERATOR_GE;
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

    if (op == OPERATOR_EQ) return strcmp(left, right) == 0;
    if (op == OPERATOR_NE) return strcmp(left, right) != 0;

    long left_num = strtol(left, NULL, 10);
    long right_num = strtol(right, NULL, 10);

    if (op == OPERATOR_LT) return left_num < right_num;
    if (op == OPERATOR_LE) return left_num <= right_num;
    if (op == OPERATOR_GT) return left_num > right_num;
    if (op == OPERATOR_GE) return left_num >= right_num;

    return 0;
}

int pk_search(struct DB *db, int pk_index, const char *value, int result_index) {
    int index_a = 0;
    int index_b = db->record_count - 1;
    int index_match = -1;
    int numeric_mode = is_numeric(value);

    long search_value;

    if (numeric_mode) {
        search_value = atol(value);
    }

    char val[VALUE_MAX_LENGTH];

    // Boundary cases
    getRecordValue(db, index_a, pk_index, val, VALUE_MAX_LENGTH);
    int res = compare(numeric_mode, value, search_value, val);
    if (res < 0) {
        return -1;
    }

    if (res == 0) {
        index_match = index_a;
    } else {
        getRecordValue(db, index_b, pk_index, val, VALUE_MAX_LENGTH);
        res = compare(numeric_mode, value, search_value, val);

        if (res > 0) {
            return -1;
        }

        if (res == 0) {
            index_match = index_b;
        } else while (index_a < index_b - 1) {
            int index_curr = (index_a + index_b) / 2;

            getRecordValue(db, index_curr, pk_index, val, VALUE_MAX_LENGTH);
            res = compare(numeric_mode, value, search_value, val);

            if (res == 0) {
                // printf("pk_search [%d   <%d>   %d]: %s\n", index_a, index_curr, index_b, val);
                index_match = index_curr;
                break;
            }

            if (res > 0) {
                // printf("pk_search [%d   (%d) x %d]: %s\n", index_a, index_curr, index_b, val);
                index_a = index_curr;

            } else {
                // printf("pk_search [%d x (%d)   %d]: %s\n", index_a, index_curr, index_b, val);
                index_b = index_curr;
            }
        }
    }

    if (index_match < 0) {
        return -1;
    }

    if (result_index == FIELD_ROW_INDEX) {
        return index_match;
    }

    if (getRecordValue(db, index_match, result_index, val, VALUE_MAX_LENGTH) > 0) {
        return atoi(val);
    }

    return -1;
}

int compare (int numeric_mode, const char * valueA, long valueA_numeric, const char *valueB) {
    if (numeric_mode) {
        long valueB_numeric = atol(valueB);
        return valueA_numeric - valueB_numeric;
    } else {
        return strcmp(valueA, valueB);
    }
}