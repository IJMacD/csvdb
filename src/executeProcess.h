#include "structs.h"

int executeSort (
    struct Query *query,
    struct PlanStep *step,
    struct ResultSet *result_set
);

int executeReverse (
    struct Query *query,
    struct PlanStep *step,
    struct ResultSet *result_set
);

int executeGroupSorted (
    struct Query *query,
    struct PlanStep *step,
    struct ResultSet *result_set
);

int executeGroupBucket (
    struct Query *query,
    struct PlanStep *step,
    struct ResultSet *result_set
);