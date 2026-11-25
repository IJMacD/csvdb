#include "../structs.h"
#include "../query/result.h"

int select_query(
    const char *query,
    enum OutputOption output_flags,
    FILE *output,
    const char **end_ptr);

int select_subquery_file(
    const char *query,
    char *filename,
    const char **end_ptr);

int select_subquery_mem(
    const char *query,
    struct DB *db,
    const char **end_ptr);

/**
 * Run a scalar subquery from a pre-parsed template Query, cloning it and
 * optionally binding unresolved columns from the current outer row as
 * constants (AST-bind correlation).
 * Writes the first column of the first result row into output.
 * Returns bytes written, 0 for empty/NULL, or -1 on error.
 */
int evaluate_scalar_subquery(
    struct Query *template_query,
    struct Table *outer_tables,
    int outer_table_count,
    RowListIndex outer_row_list,
    int outer_row_index,
    int has_outer_row,
    char *output,
    int max_length);

/**
 * Like evaluate_scalar_subquery but keeps the full result set in db.
 * On success the caller owns db and must closeDB(db).
 * Returns 0 on success, -1 on error (db is closed on failure).
 */
int evaluate_subquery_to_db(
    struct Query *template_query,
    struct Table *outer_tables,
    int outer_table_count,
    RowListIndex outer_row_list,
    int outer_row_index,
    int has_outer_row,
    struct DB *db);

/** Deep-copy a parsed Query (tables unopened; safe to clone repeatedly). */
struct Query *cloneQuery(const struct Query *src);

/** Free a Query allocated with makeQuery/cloneQuery/calloc + parseQuery. */
void destroyQuery(struct Query *query);

int process_query(
    struct Query *q,
    enum OutputOption output_flags,
    FILE *output);

struct Table *allocateTable(struct Query *q);

struct Node *allocateColumnNode(struct Query *q);

struct Node *allocatePredicateNode(struct Query *q);

struct Node *allocateHavingNode(struct Query *q);
