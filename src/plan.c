#include <stdlib.h>
#include <string.h>

#include "plan.h"
#include "indices.h"
#include "function.h"
#include "sort.h"
#include "util.h"

void addStep (struct Plan *plan, int type);

void addOrderStepIfRequired (struct Plan *plan, struct Query *q);

void addStepWithPredicate (struct Plan *plan, int type, struct Predicate *p);

void addStepWithPredicates (struct Plan *plan, int type, struct Predicate *p, int predicate_count);

void addStepWithLimit (struct Plan *plan, int type, int limit);

void addJoinStepsIfRequired (struct Plan *plan, struct Query *q);

static int optimisePredicates (struct Query *q, struct Predicate * predicates, int count);

void applyLimitOptimisation (struct Plan *plan, struct Query *query);

int makePlan (struct Query *q, struct Plan *plan) {
    plan->step_count = 0;

    // If there's no table specified then it must be a
    // single-row-all-constant query
    if (q->table_count == 0) {
        plan->step_count = 1;

        struct PlanStep * step = plan->steps;
        step->type = PLAN_SELECT;
        step->predicate_count = 0;

        return plan->step_count;
    }

    if (q->flags & FLAG_PRIMARY_KEY_SEARCH) {
        /******************
         * PRIMARY KEY
         *****************/

        int type;
        // WARNING: Assumes PK is first predicate
        if (q->predicates[0].op == OPERATOR_EQ) {
            type = PLAN_PK;
        } else {
            type = PLAN_PK_RANGE;
        }

        addStepWithPredicate(plan, type, q->predicates + 0);

        // Sort is never required if the index is PK_UNIQUE
        if (type == PLAN_PK_RANGE) {
            addOrderStepIfRequired(plan, q);
        }
    }
    else if (q->flags & FLAG_HAVE_PREDICATE) {

        // Try to find a predicate on the first table
        int predicatesOnFirstTable = optimisePredicates(q, q->predicates, q->predicate_count);

        struct Table table = q->tables[0];

        struct Predicate *p = &q->predicates[0];

        if (predicatesOnFirstTable > 0) {

            // Remove qualified name so indexes can be searched etc.
            int dot_index = str_find_index(p->left.text, '.');
            if (dot_index >= 0) {
                char value[FIELD_MAX_LENGTH];
                strcpy(value, p->left.text);
                strcpy(p->left.text, value + dot_index + 1);
            }

            /*******************
             * INDEX SCAN
             *******************/

            // Try to find any index
            int find_result = findIndex(NULL, table.name, p->left.text, INDEX_ANY);

            if (p->op != OPERATOR_LIKE && p->left.function == FUNC_UNITY && find_result > 0) {

                int type;

                if (find_result == INDEX_PRIMARY) {
                    if (p->op == OPERATOR_EQ) {
                        type = PLAN_PK;
                    } else {
                        type = PLAN_PK_RANGE;
                    }
                } else if (find_result == INDEX_UNIQUE) {
                    if (p->op == OPERATOR_EQ) {
                        type = PLAN_UNIQUE;
                    } else {
                        type = PLAN_UNIQUE_RANGE;
                    }
                } else {
                    type = PLAN_INDEX_RANGE;
                }

                addStepWithPredicate(plan, type, p);

                addJoinStepsIfRequired(plan, q);

                if (q->predicate_count > 1) {
                    addStepWithPredicates(plan, PLAN_TABLE_ACCESS_ROWID, q->predicates + 1, q->predicate_count - 1);
                }

                // Follow our own logic to add an order step
                // We can avoid it if we're just going to sort on the same column we've just scanned
                if ((q->flags & FLAG_ORDER) && strcmp(p->left.text, q->order_field) == 0) {
                    // So a sort is not necessary but we might need to reverse

                    if (type == PLAN_PK || type == PLAN_UNIQUE) {
                        // Reverse is never required if the plan is PLAN_PK or PLAN_UNIQUE
                    } else if (q->flags & FLAG_GROUP){
                        // Reverse is not required if we're grouping
                    } else if (q->order_direction == ORDER_DESC) {
                        addStep(plan, PLAN_REVERSE);
                    }
                } else {
                    addOrderStepIfRequired(plan, q);
                }

                applyLimitOptimisation(plan, q);
            }
            // Before we do a full table scan... we have one more opportunity to use an index
            // To save a sort later, see if we can use an index for ordering now
            else if (
                (q->flags & FLAG_ORDER) &&
                // If we're selecting a lot of rows this optimisation is probably worth it.
                // If we have an EQ operator then it's probably cheaper to filter first
                (p->op != OPERATOR_EQ) &&
                findIndex(NULL, table.name, q->order_field, INDEX_ANY)
            ) {

                struct Predicate *order_p = malloc(sizeof(*order_p));
                strcpy(order_p->left.text, q->order_field);
                // OPERATOR_ALWAYS on index range means the entire range;
                order_p->op = OPERATOR_ALWAYS;
                order_p->right.text[0] = '\0';

                // Add step for Sorted index access
                addStepWithPredicate(plan, PLAN_INDEX_RANGE, order_p);

                // Optimisation: filter before join
                int skip_predicates = 0;
                for (int i = 0; i < q->predicate_count; i++) {
                    // If left and right are either constant or table 0 then we can filter
                    if (q->predicates[i].left.table_id <= 0 &&
                        q->predicates[i].right.table_id <= 0)
                    {
                        skip_predicates++;
                    } else {
                        break;
                    }
                }

                if (skip_predicates > 0) {
                    addStepWithPredicates(plan, PLAN_TABLE_ACCESS_ROWID, q->predicates, skip_predicates);
                }

                addJoinStepsIfRequired(plan, q);

                if (q->order_direction == ORDER_DESC && !(q->flags & FLAG_GROUP)) {
                    addStep(plan, PLAN_REVERSE);
                }

                // Add step for remaining predicates filter
                if (q->predicate_count > skip_predicates) {
                    addStepWithPredicates(plan, PLAN_TABLE_ACCESS_ROWID, q->predicates + skip_predicates, q->predicate_count - skip_predicates);
                }

                applyLimitOptimisation(plan, q);
            }
            /********************
             * TABLE ACCESS FULL
             ********************/
            else {
                if (q->table_count > 1) {
                    // First predicates are from first table
                    addStepWithPredicates(plan, PLAN_TABLE_ACCESS_FULL, q->predicates, predicatesOnFirstTable);

                    // The join
                    addJoinStepsIfRequired(plan, q);

                    if (q->predicate_count > predicatesOnFirstTable) {
                        // Add the rest of the predicates after the join
                        addStepWithPredicates(plan, PLAN_TABLE_ACCESS_ROWID, q->predicates + predicatesOnFirstTable, q->predicate_count - predicatesOnFirstTable);
                    }
                } else {
                    // Only one table so add all predicates together
                    addStepWithPredicates(plan, PLAN_TABLE_ACCESS_FULL, q->predicates, q->predicate_count);

                }

                addOrderStepIfRequired(plan, q);

                applyLimitOptimisation(plan, q);
            }
        } else {

            // Predicate isn't on the first table need to do full access

            addStep(plan, PLAN_TABLE_ACCESS_FULL);

            addJoinStepsIfRequired(plan, q);

            addStepWithPredicates(plan, PLAN_TABLE_ACCESS_ROWID, q->predicates, q->predicate_count);

            addOrderStepIfRequired(plan, q);

            applyLimitOptimisation(plan, q);
        }
    }
    else if ((q->flags & FLAG_ORDER) && !(q->flags & FLAG_GROUP)) {
        // Before we do a full table scan... we have one more opportunity to use an index
        // To save a sort later, see if we can use an index for ordering now
        struct Table table = q->tables[0];
        if (findIndex(NULL, table.name, q->order_field, INDEX_ANY)) {

            struct Predicate *order_p = malloc(sizeof(*order_p));
            strcpy(order_p->left.text, q->order_field);
            // OPERATOR_ALWAYS means scan full index
            order_p->op = OPERATOR_ALWAYS;
            order_p->right.text[0] = '\0';

            addStepWithPredicate(plan, PLAN_INDEX_RANGE, order_p);

            if (q->order_direction == ORDER_DESC) {
                addStep(plan, PLAN_REVERSE);
            }
            else if (q->limit_value >= 0 && q->table_count <= 1) {
                // Usually this can't be done with ORDER BY but in this case we
                // can since there are no predicates
                plan->steps[plan->step_count - 1].limit = q->offset_value + q->limit_value;
            }

            addJoinStepsIfRequired(plan, q);

        } else {
            addStep(plan, PLAN_TABLE_ACCESS_FULL);

            addJoinStepsIfRequired(plan, q);

            addOrderStepIfRequired(plan, q);

            applyLimitOptimisation(plan, q);
        }
    } else {
        addStep(plan, PLAN_TABLE_ACCESS_FULL);

        addJoinStepsIfRequired(plan, q);
    }

    /*******************
     * Grouping
     *******************/
    if (q->flags & FLAG_GROUP) {
        addStep(plan, PLAN_GROUP);
    }

    /********************
     * OFFSET/FETCH FIRST
     ********************/

    if (q->limit_value >= 0) {
        addStepWithLimit(plan, PLAN_SLICE, q->offset_value + q->limit_value);
    }

    addStep(plan, PLAN_SELECT);

    return plan->step_count;
}

void destroyPlan (struct Plan *plan) {
    for (int i = 0; i < plan->step_count; i++) {
        // don't double free
        // Most predicates will be free'd in destroyQuery
        if (plan->steps[i].predicate_count > 0 && plan->steps[i].predicates[0].right.text[0] == '\0') free(plan->steps[i].predicates);
    }
}

void addStep (struct Plan *plan, int type) {
    int i = plan->step_count;

    plan->steps[i].predicate_count = 0;
    plan->steps[i].type = type;
    plan->steps[i].limit = -1;

    plan->step_count++;
}

/**
 * Will not necessarily add order step.
 * Will check if it's necessary
 */
void addOrderStepIfRequired (struct Plan *plan, struct Query *q) {
    int i = plan->step_count;

    // Check if there's an order by clause
    if (!(q->flags & FLAG_ORDER)) {
        return;
    }

    // At the moment grouping always results in a single row so sorting
    // is never neccessary.
    if (q->flags & FLAG_GROUP) {
        return;
    }

    // Don't need to sort if the order field has an equality predicate on it
    // Comment: This assumes we folled a specific path above
    //
    // Comment: should properly check both sides for functions/constants etc.
    for (int j = 0; j < q->predicate_count; j++) {
        if (strcmp(q->predicates[j].left.text, q->order_field) == 0 && q->predicates[j].op == OPERATOR_EQ && q->predicates[j].left.function == FUNC_UNITY) {
            return;
        }
    }

    struct Predicate *order_p = malloc(sizeof(*order_p));
    strcpy(order_p->left.text, q->order_field);
    order_p->op = q->order_direction;
    order_p->right.text[0] = '\0';

    plan->steps[i].predicate_count = 1;
    plan->steps[i].predicates = order_p;
    plan->steps[i].type = PLAN_SORT;
    plan->steps[i].limit = -1;

    plan->step_count++;
}

void addStepWithPredicate (struct Plan *plan, int type, struct Predicate *p) {
    addStepWithPredicates(plan, type, p, 1);
}

void addStepWithPredicates (struct Plan *plan, int type, struct Predicate *p, int predicate_count) {
    int i = plan->step_count;

    plan->steps[i].predicate_count = predicate_count;
    plan->steps[i].predicates = p;
    plan->steps[i].type = type;
    plan->steps[i].limit = -1;

    plan->step_count++;
}

void addStepWithLimit (struct Plan *plan, int type, int limit) {
    int i = plan->step_count;

    plan->steps[i].predicate_count = 0;
    plan->steps[i].type = type;
    plan->steps[i].limit = limit;

    plan->step_count++;
}

void addJoinStepsIfRequired (struct Plan *plan, struct Query *q) {
    /*******************
     * JOIN
     *******************/
    for (int i = 1; i < q->table_count; i++) {
        struct Table * table = q->tables + i;
        struct Predicate * join = &table->join;

        if (join->op == OPERATOR_ALWAYS) {
            addStep(plan, PLAN_CROSS_JOIN);
        } else {
            if (join->left.field == FIELD_CONSTANT ||
                join->right.field == FIELD_CONSTANT)
            {
                addStepWithPredicate(plan, PLAN_CONSTANT_JOIN, join);
            }
            else {

                if (join->op == OPERATOR_EQ) {
                    int join_result_left = findIndex(NULL, table->name, join->left.text, INDEX_UNIQUE);
                    int join_result_right = findIndex(NULL, table->name, join->right.text, INDEX_UNIQUE);

                    if (
                        join_result_left == INDEX_UNIQUE || join_result_left == INDEX_PRIMARY ||
                        join_result_right == INDEX_UNIQUE || join_result_right == INDEX_PRIMARY
                    ) {
                        addStepWithPredicate(plan, PLAN_UNIQUE_JOIN, join);
                    }
                    else {
                        addStepWithPredicate(plan, PLAN_INNER_JOIN, join);
                    }
                }
                else {
                    addStepWithPredicate(plan, PLAN_INNER_JOIN, join);
                }
            }
        }
    }
}

/**
 * @brief Re-order predicates so first N predicates only include first table and constants
 *
 * NOTE: Current implementation can actually only cope with N = 1
 *
 * @param q
 * @param predicates
 * @param count
 * @return int N, number of predicates on first table
 */
static int optimisePredicates (__attribute__((unused)) struct Query *q, struct Predicate * predicates, int count) {
    int chosen_predicate_index = -1;

    // Comment: Only checking left?

    for (int i = 0; i < count; i++) {
        // First check if we've been given an explicit index
        // If so, we'll have to assume that's on the first table
        //
        // Comment: Coult/should this have been filled in earlier?
        if (strncmp(predicates[i].left.text, "UNIQUE(", 7) == 0 ||
            strncmp(predicates[i].left.text, "INDEX(", 6) == 0)
        {
            predicates[i].left.table_id = 0;
        }

        if (predicates[i].left.table_id == 0) {
            chosen_predicate_index = i;
            break;
        }
    }

    // Swap predicates so first one is on first table
    if (chosen_predicate_index > 0) {
        struct Predicate tmp;
        memcpy(&tmp, &predicates[0], sizeof(tmp));
        memcpy(&predicates[0], &predicates[chosen_predicate_index], sizeof(tmp));
        memcpy(&predicates[chosen_predicate_index], &tmp, sizeof(tmp));
    }

    if (chosen_predicate_index >= 0)
        return 1;

    return 0;
}

// Optimisation: Apply limit early to latest step added
void applyLimitOptimisation (struct Plan *plan, struct Query *query) {
    if (query->limit_value >= 0 && query->table_count <= 1 && !(query->flags & FLAG_ORDER)) {
        plan->steps[plan->step_count - 1].limit = query->offset_value + query->limit_value;
    }
}