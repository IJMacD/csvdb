#include "structs.h"

int parseOperator (const char *input);

int evaluateExpression (enum Operator op, const char *left, const char *right);

void normalisePredicate (struct Predicate *p);

int flipPredicate (struct Predicate *p);
