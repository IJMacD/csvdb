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

void optimiseRemoveTrivialPredicates(struct Query *query);

void optimiseFoldRedundantPredicates(struct Query *query);

void optimiseMergeOrRanges(struct Node *node);

void optimiseTransitivePredicates(struct Query *query);

int areChildrenUnique(struct Node *node);
