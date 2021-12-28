#include "db.h"
#include "result.h"

#define ORDER_ASC   0
#define ORDER_DESC  1

void sortResultRows (struct DB *db, int table_id, int field_index, int direction, struct RowList * source_list, struct RowList * target_list);
