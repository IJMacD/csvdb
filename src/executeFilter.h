#include "structs.h"

int executeTableAccessFull (struct Query *query, struct PlanStep *step, struct ResultSet *result_set);

int executeSlice (struct Query *query, struct PlanStep *step, struct ResultSet *result_set);
