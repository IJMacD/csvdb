#include "db.h"

#define RESULT_NO_ROWS      -1
#define RESULT_NO_INDEX     -2
#define RESULT_BELOW_MIN    -3
#define RESULT_ABOVE_MAX    -4

int primaryKeyScan (struct DB *db, const char *predicate_field, char predicate_op, const char *predicate_value, int *result_rowids);
int indexUniqueScan (const char *predicate_field, char predicate_op, const char *predicate_value, int *result_rowids);
int indexRangeScan (int *result_rowids, const char *predicate_field, char predicate_op, const char *predicate_value, int flags);
int fullTableScan (struct DB *db, int *result_rowids, const char *predicate_field, char predicate_op, const char *predicate_value, int limit_value, int offset_value, int flags);


int pk_search(struct DB *db, int pk_index, const char *value, int result_index);
int rangeScan (struct DB *db, char predicate_op, int lower_index, int upper_index, int rowid_column, int *result_rowids);
