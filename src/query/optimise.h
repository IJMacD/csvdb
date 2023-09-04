#include "../structs.h"

void optimiseCollapseConstantNode (struct Node *node);

void optimiseRowidAlgebra (struct Node *node);

void optimiseFlattenANDPredicates (struct Query * query);

void optimiseWhereToOn (struct Query *query);
