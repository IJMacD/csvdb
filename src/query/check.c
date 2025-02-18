#include <stdlib.h>
#include "../structs.h"

/**
 * Runs sanity checks on a plan to make sure we don't produce unbounded output
 */
void checkPlan (struct Query *query, struct Plan *plan) {
    int table_id = 0;

    for (int i = 0; i < plan->step_count; i++) {
        struct PlanStep *step = &plan->steps[i];
        struct Table *table = &query->tables[table_id];

        if (step->limit == -1 && (
            // These plan types don't use predicates, so with no limit will
            // definitely produce unbounded output.
            step->type == PLAN_TABLE_SCAN ||
            step->type == PLAN_CROSS_JOIN
        )) {
            if (table->db->vfs == VFS_SEQUENCE) {
                fprintf(stderr, "Error: Unbounded SEQUENCE. Add predicates to limit output\n");
                exit(-1);
            }
            if (table->db->vfs == VFS_CALENDAR) {
                fprintf(stderr, "Error: Unbounded CALENDAR. Add predicates to limit output\n");
                exit(-1);
            }
        }

        switch (step->type)
        {
            // These steps all access a new table
            case PLAN_TABLE_ACCESS_FULL:
            case PLAN_TABLE_SCAN:
            case PLAN_PK:
            case PLAN_PK_RANGE:
            case PLAN_UNIQUE:
            case PLAN_UNIQUE_RANGE:
            case PLAN_INDEX_RANGE:
            case PLAN_INDEX_SCAN:
            case PLAN_COVERING_INDEX_SEEK:
            case PLAN_CROSS_JOIN:
            case PLAN_CONSTANT_JOIN:
            case PLAN_LOOP_JOIN:
            case PLAN_UNIQUE_JOIN:
            case PLAN_INDEX_JOIN:
                table_id++;
                break;

            default:
                break;
        }
    }
}