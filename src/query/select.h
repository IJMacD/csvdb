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
 * Run a scalar subquery, optionally binding unresolved columns from the
 * current outer row as constants (AST-bind correlation).
 * Writes the first column of the first result row into output.
 * Returns bytes written, 0 for empty/NULL, or -1 on error.
 */
int evaluate_scalar_subquery(
    const char *sql,
    struct Table *outer_tables,
    int outer_table_count,
    RowListIndex outer_row_list,
    int outer_row_index,
    int has_outer_row,
    char *output,
    int max_length);

int process_query(
    struct Query *q,
    enum OutputOption output_flags,
    FILE *output);

struct Table *allocateTable(struct Query *q);

struct Node *allocateColumnNode(struct Query *q);

struct Node *allocatePredicateNode(struct Query *q);

struct Node *allocateHavingNode(struct Query *q);
