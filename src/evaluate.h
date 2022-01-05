#include "query.h"
#include "result.h"

int evaluateNode (struct Query * query, struct RowList *rowlist, int index, struct ColumnNode * column, char * value, int max_length);

int evaluateConstantNode (struct ColumnNode * column, char * value, int max_length);
