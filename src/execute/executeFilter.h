#include "../structs.h"

int executeTableAccessRowid (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
);

int executeSlice (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
);

int executeOffset (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
);
