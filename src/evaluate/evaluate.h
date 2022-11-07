#include "../structs.h"

int evaluateNode (
    struct Query * query,
    struct RowList *rowlist,
    int index,
    struct ColumnNode * column,
    char * value,
    int max_length
);

int evaluateNodeList (
    struct Query * query,
    struct RowList *rowlist,
    int index,
    struct ColumnNode * columns,
    int column_count,
    char * output,
    int max_length
);

int evaluateField (
    char * output,
    struct Table *tables,
    struct RowList *rowlist,
    struct Field *field,
    int result_index
);

int evaluateConstantField (char * value, struct Field * field);

int evaluateAggregateFunction (
    char * output,
    struct Table *db,
    int table_count,
    struct ColumnNode *column,
    struct RowList * row_list
);
