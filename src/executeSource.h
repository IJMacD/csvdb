#include "structs.h"

int executeSourceDummyRow (struct Query *query, struct PlanStep *step, struct ResultSet *result_set);

int executeSourcePK (struct Query *query, struct PlanStep *step, struct ResultSet *result_set);

int executeSourceUnique (struct Query *query, struct PlanStep *step, struct ResultSet *result_set);

int executeSourceIndexSeek (struct Query *query, struct PlanStep *step, struct ResultSet *result_set);

int executeSourceIndexScan (struct Query *query, struct PlanStep *step, struct ResultSet *result_set);

int executeSourceTableFull (struct Query *query, struct PlanStep *step, struct ResultSet *result_set);

int executeSourceTableScan (struct Query *query, struct PlanStep *step, struct ResultSet *result_set);