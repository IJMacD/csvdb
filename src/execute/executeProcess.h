#include "../structs.h"

int executeSort (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
);

int executeReverse (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
);

int executeGroupSorted (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
);

int executeGroupBucket (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
);