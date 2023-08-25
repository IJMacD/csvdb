#include "../structs.h"

int parseOperator (const char *input);

int evaluateExpressionNode (struct Table *tables, int row_list, int row_index, struct Node *nodes, int node_count);

int evaluateExpression (enum Function op, const char *left, const char *right);

void normalisePredicate (struct Node *p);

int flipPredicate (struct Node *p);
