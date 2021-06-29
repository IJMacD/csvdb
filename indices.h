#include "db.h"

#define RESULT_NO_ROWS      -1
#define RESULT_NO_INDEX     -2
#define RESULT_BELOW_MIN    -3
#define RESULT_ABOVE_MAX    -4

#define INDEX_ANY           0
#define INDEX_PK            1
#define INDEX_UNIQUE        2

int primaryKeyScan (struct DB *db, const char *predicate_field, int predicate_op, const char *predicate_value, int *result_rowids);
int indexUniqueScan (const char *predicate_field, int predicate_op, const char *predicate_value, int *result_rowids);
int indexRangeScan (const char *predicate_field, int predicate_op, const char *predicate_value, int *result_rowids);
int fullTableScan (struct DB *db, int *result_rowids, const char *predicate_field, int predicate_op, const char *predicate_value, int limit_value, int offset_value, int flags);

int findIndex(struct DB *db, const char *index_name, int index_type_flags);

int indexWalk(struct DB *db, int rowid_column, int lower_index, int upper_index, int direction, int *result_rowids);

int pk_search(struct DB *db, int pk_index, const char *value, int result_index);
int rangeScan (struct DB *db, int predicate_op, int lower_index, int upper_index, int rowid_column, int *result_rowids);
