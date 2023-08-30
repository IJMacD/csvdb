int evaluateFunction(
    char * output,
    int function,
    char **values,
    int value_count
);

int evaluateAggregateFunction (
    char * output,
    struct Table *tables,
    struct Node *node,
    RowListIndex row_list
);
