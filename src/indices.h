#include "db.h"
#include "result.h"

#define RESULT_NO_ROWS      -1
#define RESULT_NO_INDEX     -2
#define RESULT_BELOW_MIN    -3
#define RESULT_ABOVE_MAX    -4

#define INDEX_ANY           0
#define INDEX_REGULAR       1
#define INDEX_UNIQUE        2
#define INDEX_PK            3

int primaryKeyScan (struct DB *db, const char *predicate_field, int predicate_op, const char *predicate_value, struct RowList * row_list);
int indexUniqueScan (struct DB *index_db, const char *predicate_field, int predicate_op, const char *predicate_value, struct RowList * row_list);
int indexRangeScan (struct DB *index_db, const char *predicate_field, int predicate_op, const char *predicate_value, struct RowList * row_list);

int indexWalk(struct DB *db, int rowid_column, int lower_index, int upper_index, struct RowList * row_list);

int rangeScan (struct DB *db, int predicate_op, int lower_index, int upper_index, int rowid_column, struct RowList * row_list);
