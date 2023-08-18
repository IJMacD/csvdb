#include <stdio.h>

#include "../structs.h"

int executeQueryPlan (
    struct Query *query,
    struct Plan *plan,
    enum OutputOption output_flags,
    FILE * output
);