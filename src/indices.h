#include "db.h"
#include "result.h"

#define RESULT_FOUND        0
#define RESULT_NO_ROWS      -1
#define RESULT_BETWEEN      -2
#define RESULT_BELOW_MIN    -3
#define RESULT_ABOVE_MAX    -4

#define RESULT_NO_INDEX     -10

#define INDEX_NONE          0
#define INDEX_ANY           0
#define INDEX_REGULAR       1
#define INDEX_UNIQUE        2
#define INDEX_PRIMARY       3

#define MODE_UNIQUE         0
#define MODE_LOWER_BOUND    1
#define MODE_UPPER_BOUND    2

int indexPrimaryScan (struct DB *db, int predicate_op, const char *predicate_value, struct RowList * row_list, int limit);
int indexUniqueScan (struct DB *index_db, int rowid_column, int predicate_op, const char *predicate_value, struct RowList * row_list, int limit);
int indexScan (struct DB *index_db, int rowid_column, int predicate_op, const char *predicate_value, struct RowList * row_list, int limit);
// int indexRangeScan (struct DB *index_db, int rowid_column, int predicate_op1, const char *predicate_value1, int predicate_op2, const char *predicate_value2, struct RowList * row_list, int limit);
