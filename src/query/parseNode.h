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

int parseFunctionParams (
    const char * query,
    size_t * index,
    struct Node *node
);

int parseOperator (const char *input);

int checkConstantField (struct Field *field);

enum Function parseSimpleOperators (const char *query, size_t *index);
