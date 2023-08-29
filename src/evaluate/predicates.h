#include "../structs.h"

int evaluateOperatorNode (struct Table *tables, int row_list, int row_index, struct Node *node);

int evaluateOperatorNodeListAND (struct Table *tables, int row_list, int row_index, struct Node *nodes, int node_count);

int evaluateOperatorNodeListOR (struct Table *tables, int row_list, int row_index, struct Node *nodes, int node_count);

int evaluateExpression (enum Function op, const char *left, const char *right);

void normalisePredicate (struct Node *p);

int flipPredicate (struct Node *p);
