#include "db.h"

#define ORDER_ASC   0
#define ORDER_DESC  1

void sortResultRows (struct DB *db, int field_index, int direction, const int *rowids, int row_count, int *out_rowids);