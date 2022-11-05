#include "structs.h"

enum IndexSearchResult indexPrimarySeek (
    struct DB *db,
    enum Operator predicate_op,
    const char *predicate_value,
    struct RowList * row_list,
    int limit
);
enum IndexSearchResult indexUniqueSeek (
    struct DB *index_db,
    int rowid_column,
    enum Operator predicate_op,
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
    enum Operator predicate_op,
    const char *predicate_value,
    struct RowList * row_list,
    int limit
);
// int indexRangeScan (
//     struct DB *index_db,
//     int rowid_column,
//     int predicate_op1,
//     const char *predicate_value1,
//     int predicate_op2,
//     const char *predicate_value2,
//     struct RowList * row_list,
//     int limit
// );
