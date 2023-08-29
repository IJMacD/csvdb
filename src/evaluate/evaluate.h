#include "../structs.h"

int evaluateNode (
    struct Table *tables,
    RowListIndex row_list,
    int index,
    struct Node *node,
    char * value,
    int max_length
);

int evaluateConstantNode (
    struct Node *node,
    char *output
);

int evaluateNodeList (
    struct Table *tables,
    RowListIndex row_list,
    int index,
    struct Node *node,
    int node_count,
    char * output,
    int max_length
);

int evaluateConstantField (char * output, struct Field *field);

int isConstantNode (struct Node *node);
