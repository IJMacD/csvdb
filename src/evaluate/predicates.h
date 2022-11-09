#include "../structs.h"

int parseOperator (const char *input);

int evaluateExpression (enum Function op, const char *left, const char *right);

void normalisePredicate (struct Node *p);

int flipPredicate (struct Node *p);
