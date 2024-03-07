#include "../structs.h"

int runQueries(
    const char *query,
    enum OutputOption output_flags,
    FILE *output);

int query(
    const char *query,
    enum OutputOption output_flags,
    FILE *output,
    const char **end_ptr);
