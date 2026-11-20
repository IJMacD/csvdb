#include "../structs.h"

/**
 * Index files are sorted key column(s) followed by a trailing base-table
 * rowid column. The header name is irrelevant; the last column is always
 * the pointer (CREATE INDEX writes it as "rowid").
 */
int indexRowidColumn (struct DB *index_db);

/**
 * How many leading nodes on an index plan step are seek bounds: 1, or 2 when
 * nodes[0]/nodes[1] form a complementary GE/GT+LE/LT pair on the same column.
 * Remaining nodes (if any) are residual filters evaluated during the walk.
 */
int indexStepSeekNodeCount (struct Node *nodes, int node_count);

/**
 * Non-zero if predicate is a simple column OP constant comparison whose
 * column exists in the index file (so it can be applied during an index walk).
 */
int predicateSatisfiableFromIndex (struct DB *index_db, struct Node *predicate);

enum IndexSearchResult indexPrimarySeek (
    struct DB *db,
    enum Function predicate_op,
    const char *predicate_value,
    struct RowList * row_list,
    int limit
);
enum IndexSearchResult indexUniqueSeek (
    struct DB *index_db,
    int rowid_column,
    enum Function predicate_op,
    const char *predicate_value,
    struct RowList * row_list,
    int limit,
    struct Node *filters,
    int filter_count
);
enum IndexSearchResult indexScan (
    struct DB *index_db,
    int rowid_column,
    struct RowList * row_list,
    int limit,
    struct Node *filters,
    int filter_count
);
enum IndexSearchResult indexSeek (
    struct DB *index_db,
    int rowid_column,
    enum Function predicate_op,
    const char *predicate_value,
    struct RowList * row_list,
    int limit,
    struct Node *filters,
    int filter_count
);
enum IndexSearchResult indexCoveringSeek (
    struct DB *db,
    enum Function predicate_op,
    const char *predicate_value,
    struct RowList * row_list,
    int limit,
    struct Node *filters,
    int filter_count
);
enum IndexSearchResult indexBoundedSeek (
    struct DB *index_db,
    int rowid_column,
    enum Function lower_op,
    const char *lower_value,
    enum Function upper_op,
    const char *upper_value,
    struct RowList * row_list,
    int limit,
    struct Node *filters,
    int filter_count
);
