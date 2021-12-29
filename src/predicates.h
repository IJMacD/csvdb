#pragma once

#include "limits.h"
#include "result.h"

// Operator bitmap
//
//          GT      LT      EQ
// NV                           |   0   Never ??
// EQ                       1   |   1
// LT               1       0   |   2
// LE               1       1   |   3
// GT       1       0       0   |   4
// GE       1       0       1   |   5
// NE       1       1       0   |   6
// AL       1       1       1   |   7   Always ??

#define OPERATOR_UN         0
#define OPERATOR_EQ         1
#define OPERATOR_LT         2
#define OPERATOR_LE         3
#define OPERATOR_GT         4
#define OPERATOR_GE         5
#define OPERATOR_NE         6
#define OPERATOR_LIKE       128

#define OPERATOR_NEVER      0
#define OPERATOR_ALWAYS     7

struct Predicate {
    int op;
    struct ColumnNode left;
    struct ColumnNode right;
};

int parseOperator (const char *input);

int compare (int numeric_mode, const char * valueA, long valueA_numeric, const char *valueB);

int evaluateExpression (int op, const char *left, const char *right);

int compareValues (const char *left, const char *right);

void normalisePredicate (struct Predicate *p);
