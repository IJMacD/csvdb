#include "../structs.h"

int executeCrossJoin (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
);

int executeConstantJoin (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
);

int executeLoopJoin (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
);

int executeUniqueJoin (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
);

int executeIndexJoin (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
);
