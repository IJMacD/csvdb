#include "structs.h"

void sortQuick (
    struct Query *q,
    struct ColumnNode *columns,
    int column_count,
    enum Order *sort_directions,
    struct RowList *row_list
);
