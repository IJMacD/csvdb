#include "structs.h"

int executeCrossJoin (struct Query *query, struct PlanStep *step, struct ResultSet *result_set);

int executeConstantJoin (struct Query *query, struct PlanStep *step, struct ResultSet *result_set);

int executeLoopJoin (struct Query *query, struct PlanStep *step, struct ResultSet *result_set);

int executeUniqueJoin (struct Query *query, struct PlanStep *step, struct ResultSet *result_set);

int executeIndexJoin (struct Query *query, struct PlanStep *step, struct ResultSet *result_set);
