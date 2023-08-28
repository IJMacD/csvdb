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

int select_subquery_file (
    const char *query,
    char *filename,
    const char **end_ptr
);

int select_subquery_mem (
    const char *query,
    struct DB *db,
    const char **end_ptr
);

int process_query (
    struct Query *q,
    enum OutputOption output_flags,
    FILE * output
);

struct Table *allocateTable (struct Query *q);

struct Node *allocatePredicateNode (struct Query *q);
