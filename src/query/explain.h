#include "../structs.h"

int explain_select_query (
    struct Table *tables,
    struct Plan *plan,
    int output_flags,
    FILE * output
);