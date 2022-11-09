#include <stdio.h>

#include "../structs.h"

int executeQueryPlan (
    struct Table *tables,
    int table_count,
    struct Plan *plan,
    enum OutputOption output_flags,
    FILE * output
);