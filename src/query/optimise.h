#include "../structs.h"

void optimiseCollapseConstantNode (struct Node *node);

void optimiseRowidAlgebra (struct Node *node);

void optimiseFlattenANDPredicates (struct Query * query);

void optimiseWhereToOn (struct Query *query);

void optimiseOnToWhere (
    int table_id,
    struct Node *joinNode,
    struct Query *query
);
