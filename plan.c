#include <stdlib.h>
#include <string.h>

#include "plan.h"
#include "indices.h"
#include "sort.h"

void addStep (struct Plan *plan, int type);

void addOrderStepIfRequired (struct Plan *plan, struct Query *q);

void addStepWithPredicate (struct Plan *plan, int type, struct Predicate *p);

void addStepWithParams (struct Plan *plan, int type, int param1, int param2);

int makePlan (struct Query *q, struct Plan *plan) {
    plan->step_count = 0;

    if (q->flags & FLAG_PRIMARY_KEY_SEARCH) {
        /******************
         * PRIMARY KEY
         *****************/
        struct Predicate *p = malloc(sizeof(*p));
        p->field = q->predicate_field;
        p->op = q->predicate_op;
        p->value = q->predicate_value;

        int type;
        if (q->predicate_op == OPERATOR_EQ) {
            type = PLAN_PK_UNIQUE;
        } else {
            type = PLAN_PK_RANGE;
        }

        addStepWithPredicate(plan, type, p);

        // Sort is never required if the index is PK_UNIQUE
        if (type == PLAN_PK_RANGE) {
            addOrderStepIfRequired(plan, q);
        }
    }
    else if (q->flags & FLAG_HAVE_PREDICATE) {

        struct Predicate *p = malloc(sizeof(*p));
        p->field = q->predicate_field;
        p->op = q->predicate_op;
        p->value = q->predicate_value;

        struct DB index_db;

        /*******************
         * UNIQUE INDEX SCAN
         *******************/
        // Try to find a unique index
        if (p->op != OPERATOR_LIKE && findIndex(&index_db, q->table, q->predicate_field, INDEX_UNIQUE) == 0) {
            closeDB(&index_db);

            int type;
            if (q->predicate_op == OPERATOR_EQ) {
                type = PLAN_INDEX_UNIQUE;
            } else {
                type = PLAN_INDEX_RANGE;
            }

            addStepWithPredicate(plan, type, p);

            // Sort is never required if the index is INDEX_UNIQUE
            if (type == PLAN_INDEX_RANGE && (q->flags & FLAG_ORDER) && strcmp(q->predicate_field, q->order_field) == 0) {
                // We're sorting on the same column as we've just traversed in the index
                // so a sort is not necessary but we might need to reverse
                if (q->order_direction == ORDER_DESC && !(q->flags & FLAG_GROUP)) {
                    addStep(plan, PLAN_REVERSE);
                }
            } else {
                addOrderStepIfRequired(plan, q);
            }
        }
        /*******************
         * INDEX RANGE SCAN
         *******************/
        else if (p->op != OPERATOR_LIKE && findIndex(&index_db, q->table, q->predicate_field, INDEX_ANY) == 0) {
            closeDB(&index_db);

            addStepWithPredicate(plan, PLAN_INDEX_RANGE, p);

            if ((q->flags & FLAG_ORDER) && strcmp(q->predicate_field, q->order_field) == 0) {
                // We're sorting on the same column as we've just traversed in the index
                // so a sort is not necessary but we might need to reverse
                if (q->order_direction == ORDER_DESC && !(q->flags & FLAG_GROUP)) {
                    addStep(plan, PLAN_REVERSE);
                }
            } else {
                addOrderStepIfRequired(plan, q);
            }
        }
        // Before we do a full table scan... we have one more opportunity to use an index
        // To save a sort later, see if we can use an index for ordering now
        else if (
            (q->flags & FLAG_ORDER) &&
            // If we're selecting a lot of rows this optimisation is probably worth it.
            // If we have an EQ operator then it's probably cheaper to filter first
            (p->op != OPERATOR_EQ) &&
            findIndex(&index_db, q->table, q->order_field, INDEX_ANY) == 0
        ) {
            closeDB(&index_db);

            struct Predicate *order_p = malloc(sizeof(*order_p));
            order_p->field = q->order_field;
            order_p->op = 0;
            order_p->value = NULL;

            // Add step for Sorted index access
            addStepWithPredicate(plan, PLAN_INDEX_RANGE, order_p);

            if (q->order_direction == ORDER_DESC && !(q->flags & FLAG_GROUP)) {
                addStep(plan, PLAN_REVERSE);
            }

            // Add step for predicate filter
            addStepWithPredicate(plan, PLAN_TABLE_ACCESS_ROWID, p);
        }
        /********************
         * TABLE ACCESS FULL
         ********************/
        else {
            addStepWithPredicate(plan, PLAN_TABLE_ACCESS_FULL, p);

            // Do we need an explicit sort?
            addOrderStepIfRequired(plan, q);
        }
    }
    else if ((q->flags & FLAG_ORDER) && !(q->flags & FLAG_GROUP)) {
        // Before we do a full table scan... we have one more opportunity to use an index
        // To save a sort later, see if we can use an index for ordering now
        struct DB index_db;
        if (findIndex(&index_db, q->table, q->order_field, INDEX_ANY) == 0) {
            closeDB(&index_db);

            struct Predicate *order_p = malloc(sizeof(*order_p));
            order_p->field = q->order_field;
            order_p->op = 0;
            order_p->value = NULL;

            addStepWithPredicate(plan, PLAN_INDEX_RANGE, order_p);

            if (q->order_direction == ORDER_DESC) {
                addStep(plan, PLAN_REVERSE);
            }

        } else {
            addStep(plan, PLAN_TABLE_ACCESS_FULL);

            addOrderStepIfRequired(plan, q);
        }
    } else {
        addStep(plan, PLAN_TABLE_ACCESS_FULL);
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

    if (q->offset_value > 0 || q->limit_value >= 0) {
        addStepWithParams(plan, PLAN_SLICE, q->offset_value, q->limit_value);
    }

    addStep(plan, PLAN_SELECT);

    return plan->step_count;
}

void destroyPlan (struct Plan *plan) {
    for (int i = 0; i < plan->step_count; i++) {
        if (plan->steps[i].predicate_count > 0) free(plan->steps[i].predicates);
    }
}

void addStep (struct Plan *plan, int type) {
    int i = plan->step_count;

    plan->steps[i].predicate_count = 0;
    plan->steps[i].type = type;

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

    struct Predicate *order_p = malloc(sizeof(*order_p));
    order_p->field = q->order_field;
    order_p->op = q->order_direction;
    order_p->value = NULL;

    plan->steps[i].predicate_count = 1;
    plan->steps[i].predicates = order_p;
    plan->steps[i].type = PLAN_SORT;

    plan->step_count++;
}

void addStepWithPredicate (struct Plan *plan, int type, struct Predicate *p) {
    int i = plan->step_count;

    plan->steps[i].predicate_count = 1;
    plan->steps[i].predicates = p;
    plan->steps[i].type = type;

    plan->step_count++;
}

void addStepWithParams (struct Plan *plan, int type, int param1, int param2) {
    int i = plan->step_count;

    plan->steps[i].predicate_count = 0;
    plan->steps[i].type = type;
    plan->steps[i].param1 = param1;
    plan->steps[i].param2 = param2;

    plan->step_count++;
}