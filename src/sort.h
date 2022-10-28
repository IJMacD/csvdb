#include "structs.h"

void sortResultRows (struct DB *db, int table_id, int field_index, int direction, struct RowList * source_list, struct RowList * target_list);

// To implement better sort later
// void sortResultRows (struct DB *db, struct SortField *sort_field, int sort_count, struct RowList * source_list, struct RowList * target_list);
