#include "../structs.h"

int parseNode(
    const char *query,
    size_t *index,
    struct Node *node,
    struct Query *q);

int parseComplexNode(
    const char *query,
    size_t *index,
    struct Node *node,
    struct Query *q);

int parseNodeList(
    const char *query,
    size_t *index,
    struct Node *node,
    struct Query *q);
