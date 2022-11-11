#include "../structs.h"

int runQueries (
    const char *query,
    enum OutputOption output_flags,
    FILE * output
);

int query (
    const char *query,
    enum OutputOption output_flags,
    FILE * output,
    const char **end_ptr
);

int select_query (
    const char *query,
    enum OutputOption output_flags,
    FILE * output,
    const char **end_ptr
);

int select_subquery(const char *query, char *filename, const char **end_ptr);

void copyNodeTree (struct Node *dest, struct Node *src);

void freeNode (struct Node *node);
