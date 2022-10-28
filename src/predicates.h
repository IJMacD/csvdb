#include "structs.h"

int parseOperator (const char *input);

int compare (int numeric_mode, const char * valueA, long valueA_numeric, const char *valueB);

int evaluateExpression (enum Operator op, const char *left, const char *right);

int compareValues (const char *left, const char *right);

void normalisePredicate (struct Predicate *p);
