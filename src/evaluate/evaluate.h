#include "../structs.h"

int evaluateNode (
    struct Table *tables,
    struct RowList *rowlist,
    int index,
    struct Node *node,
    char * value,
    int max_length
);

int evaluateNodeList (
    struct Table *tables,
    struct RowList *rowlist,
    int index,
    struct Node *node,
    int node_count,
    char * output,
    int max_length
);

int evaluateConstantField (char * value, struct Field *field);

int isConstantNode (struct Node *node);
