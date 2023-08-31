#include "../structs.h"

int parseNode (
    const char * query,
    size_t * index,
    struct Node *node
);

int parseComplexNode (
    const char *query,
    size_t *index,
    struct Node * node
);

void parseNodeList (const char *query, size_t *index, struct Node *node);
