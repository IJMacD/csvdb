#include <stdlib.h>
#include <string.h>
#include "predicates.h"
#include "limits.h"

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

int pk_search(struct DB *db, int pk_index, char *value) {
    int index_a = 0;
    int index_b = db->record_count - 1;

    long search_value = atol(value);

    char val[VALUE_MAX_LENGTH];

    long curr_value;

    // Boundary cases
    getRecordValue(db, index_a, pk_index, val, VALUE_MAX_LENGTH);
    curr_value = atol(val);
    if (curr_value == search_value) {
        return index_a;
    } else if (search_value < curr_value) {
        return -1;
    }
    getRecordValue(db, index_b, pk_index, val, VALUE_MAX_LENGTH);
    curr_value = atol(val);
    if (curr_value == search_value) {
        return index_b;
    } else if (search_value > curr_value) {
        return -1;
    }

    while (index_a < index_b - 1) {
        int index_curr = (index_a + index_b) / 2;

        getRecordValue(db, index_curr, pk_index, val, VALUE_MAX_LENGTH);

        curr_value = atol(val);

        if (curr_value == search_value) {
            // printf("pk_search [%d   <%d>   %d]: %s\n", index_a, index_curr, index_b, val);
            return index_curr;
        }

        if (curr_value < search_value) {
            // printf("pk_search [%d   (%d) x %d]: %s\n", index_a, index_curr, index_b, val);
            index_a = index_curr;

        } else {
            // printf("pk_search [%d x (%d)   %d]: %s\n", index_a, index_curr, index_b, val);
            index_b = index_curr;
        }
    }

    return -1;
}
