#include "../structs.h"

int executeSelect (
    FILE *output,
    enum OutputOption options,
    struct Table *tables,
    int table_count,
    struct PlanStep *step,
    struct ResultSet *result_set
);