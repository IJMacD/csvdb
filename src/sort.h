#include "db.h"
#include "result.h"

#define ORDER_ASC   0
#define ORDER_DESC  1

struct SortField {
    int table_id;
    int field;
    int order_direction;
};

void sortResultRows (struct DB *db, int table_id, int field_index, int direction, struct RowList * source_list, struct RowList * target_list);

// To implement better sort later
// void sortResultRows (struct DB *db, struct SortField *sort_field, int sort_count, struct RowList * source_list, struct RowList * target_list);
