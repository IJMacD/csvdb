#pragma once

#include "limits.h"

struct Predicate {
    int op;
    char field[FIELD_MAX_LENGTH];
    char value[VALUE_MAX_LENGTH];
};

int parseOperator (const char *input);

int compare (int numeric_mode, const char * valueA, long valueA_numeric, const char *valueB);

int evaluateExpression (int op, const char *left, const char *right);

int compareValues (const char *left, const char *right);