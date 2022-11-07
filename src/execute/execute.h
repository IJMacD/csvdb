#include <stdio.h>

#include "../structs.h"

int executeQueryPlan (
    struct Query *q,
    struct Plan *plan,
    enum OutputOption output_flags,
    FILE * output
);