#include "structs.h"

int query (const char *query, enum OutputOption output_flags, FILE * output);

int select_query (const char *query, enum OutputOption output_flags, FILE * output);

int populateColumnNode (struct Query * query, struct ColumnNode * column);

void destroyQuery (struct Query *q);

int select_subquery(const char *query, char *filename);
