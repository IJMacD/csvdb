#include "structs.h"

void sortResultRows (struct Query *q, struct ColumnNode *column, int direction, struct RowList * source_list, struct RowList * target_list);

void sortResultRowsMultiple (struct Query *q, struct ColumnNode *columns, int column_count, int *sort_directions, RowListIndex source_list, RowListIndex target_list);
