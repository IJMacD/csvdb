#include "../structs.h"

int parseQuery (struct Query *q, const char *query, const char **end_ptr);

/**
 * Parse SQL into a newly allocated Query on the heap.
 * On success the caller owns the result and must destroyQuery() then free().
 * Returns NULL on parse or allocation failure (errors already reported).
 */
struct Query *parseOwnedQuery(const char *sql);
