#include "query.h"
#include "result.h"

int evaluateNode (struct Query * query, struct RowList *rowlist, int index, struct ColumnNode * column, char * value, int max_length);

int evaluateField (char * output, struct Table *tables, struct RowList *rowlist, struct Field *field, int result_index);

int evaluateConstantField (char * value, struct Field * field);
