#include "../structs.h"

/**
 * Index files are sorted key column(s) followed by a trailing base-table
 * rowid column. The header name is irrelevant; the last column is always
 * the pointer (CREATE INDEX writes it as "rowid").
 */
int indexRowidColumn (struct DB *index_db);

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
    int limit
);
enum IndexSearchResult indexScan (
    struct DB *index_db,
    int rowid_column,
    struct RowList * row_list,
    int limit
);
enum IndexSearchResult indexSeek (
    struct DB *index_db,
    int rowid_column,
    enum Function predicate_op,
    const char *predicate_value,
    struct RowList * row_list,
    int limit
);
enum IndexSearchResult indexCoveringSeek (
    struct DB *db,
    enum Function predicate_op,
    const char *predicate_value,
    struct RowList * row_list,
    int limit
);
enum IndexSearchResult indexBoundedSeek (
    struct DB *index_db,
    int rowid_column,
    enum Function lower_op,
    const char *lower_value,
    enum Function upper_op,
    const char *upper_value,
    struct RowList * row_list,
    int limit
);
