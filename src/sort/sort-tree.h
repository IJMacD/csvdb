#include "../structs.h"

void sortResultRows (
    struct Table *tables,
    struct Node *column,
    int direction,
    struct RowList * source_list,
    struct RowList * target_list
);

void sortResultRowsMultiple (
    struct Table *tables,
    struct Node *columns,
    int column_count,
    int *sort_directions,
    RowListIndex source_list,
    RowListIndex target_list
);
