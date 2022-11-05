#include "structs.h"

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

int populateColumnNode (struct Query * query, struct ColumnNode * column);

int select_subquery(const char *query, char *filename);
