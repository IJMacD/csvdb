#include "db.h"

// Operator bitmap
//
//          GT      LT      EQ
// EQ                       1   |   1
// LT               1       0   |   2
// LE               1       1   |   3
// GT       1       0       0   |   4
// GE       1       0       1   |   5
// NE       1       1       0   |   6

#define OPERATOR_UN         0
#define OPERATOR_EQ         1
#define OPERATOR_LT         2
#define OPERATOR_LE         3
#define OPERATOR_GT         4
#define OPERATOR_GE         5
#define OPERATOR_NE         6
#define OPERATOR_LIKE       128

int parseOperator (const char *input);

int compare (int numeric_mode, const char * valueA, long valueA_numeric, const char *valueB);

int evaluateExpression (int op, const char *left, const char *right);
