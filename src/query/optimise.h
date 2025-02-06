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

void optimiseUniqueOr(struct Node *node);

int areChildrenUnique(struct Node *node);