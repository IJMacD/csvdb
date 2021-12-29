#include "query.h"
#include "plan.h"

int explain_select_query (
    struct Query *q,
    struct Plan *plan,
    int output_flags,
    FILE * output
);