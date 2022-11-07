#include "../structs.h"

int executeTableAccessRowid (
    struct Query *query,
    struct PlanStep *step,
    struct ResultSet *result_set
);

int executeSlice (
    struct Query *query,
    struct PlanStep *step,
    struct ResultSet *result_set
);
