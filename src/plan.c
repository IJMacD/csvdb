#include <stdlib.h>
#include <string.h>

#include "plan.h"
#include "indices.h"
#include "sort.h"
#include "util.h"

void addStep (struct Plan *plan, int type);

void addOrderStepIfRequired (struct Plan *plan, struct Query *q);

void addStepWithPredicate (struct Plan *plan, int type, struct Predicate *p);

void addStepWithPredicates (struct Plan *plan, int type, struct Predicate *p, int predicate_count);

void addStepWithParams (struct Plan *plan, int type, int param1, int param2);

void addJoinStepsIfRequired (struct Plan *plan, struct Query *q);

int makePlan (struct Query *q, struct Plan *plan) {
    plan->step_count = 0;

    if (q->flags & FLAG_PRIMARY_KEY_SEARCH) {
        /******************
         * PRIMARY KEY
         *****************/

        int type;
        // WARNING: Assumes PK is first predicate
        if (q->predicates[0].op == OPERATOR_EQ) {
            type = PLAN_PK_UNIQUE;
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
        int chosen_predicate_index = -1;
        int table_id = -1;
        int field_id;

        struct Table table = q->tables[0];

        for (int i = 0; i < q->predicate_count; i++) {
            findColumn(q, q->predicates[i].left.text, &table_id, &field_id);

            if (table_id == 0) {
                chosen_predicate_index = i;
                break;
            }
        }

        // Swap predicates so first one is on first table
        if (chosen_predicate_index > 0) {
            struct Predicate tmp;
            memcpy(&tmp, &q->predicates[0], sizeof(tmp));
            memcpy(&q->predicates[0], &q->predicates[chosen_predicate_index], sizeof(tmp));
            memcpy(&q->predicates[chosen_predicate_index], &tmp, sizeof(tmp));
        }

        struct Predicate *p = &q->predicates[0];

        struct DB index_db;

        if (table_id == 0) {

            // Remove qualified name so indexes can be searched etc.
            int dot_index = str_find_index(p->left.text, '.');
            if (dot_index >= 0) {
                char value[FIELD_MAX_LENGTH];
                strcpy(value, p->left.text);
                strcpy(p->left.text, value + dot_index + 1);
            }

            /*******************
             * UNIQUE INDEX SCAN
             *******************/
            // Try to find a unique index
            if (p->op != OPERATOR_LIKE && findIndex(&index_db, table.name, p->left.text, INDEX_UNIQUE) == 0) {
                closeDB(&index_db);

                int type;
                if (p->op == OPERATOR_EQ) {
                    type = PLAN_INDEX_UNIQUE;
                } else {
                    type = PLAN_INDEX_RANGE;
                }

                addStepWithPredicate(plan, type, p);

                addJoinStepsIfRequired(plan, q);

                if (q->predicate_count > 1) {
                    addStepWithPredicates(plan, PLAN_TABLE_ACCESS_ROWID, q->predicates + 1, q->predicate_count - 1);
                }

                // Sort is never required if the index is INDEX_UNIQUE
                if (type == PLAN_INDEX_RANGE && (q->flags & FLAG_ORDER) && strcmp(p->left.text, q->order_field) == 0) {
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
            else if (p->op != OPERATOR_LIKE && findIndex(&index_db, table.name, p->left.text, INDEX_ANY) == 0) {
                closeDB(&index_db);

                addStepWithPredicate(plan, PLAN_INDEX_RANGE, p);

                addJoinStepsIfRequired(plan, q);

                if (q->predicate_count > 1) {
                    addStepWithPredicates(plan, PLAN_TABLE_ACCESS_ROWID, q->predicates + 1, q->predicate_count - 1);
                }

                if ((q->flags & FLAG_ORDER) && strcmp(p->left.text, q->order_field) == 0) {
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
                findIndex(&index_db, table.name, q->order_field, INDEX_ANY) == 0
            ) {
                closeDB(&index_db);

                struct Predicate *order_p = malloc(sizeof(*order_p));
                strcpy(order_p->left.text, q->order_field);
                order_p->op = 0;
                order_p->right.text[0] = '\0';

                // Add step for Sorted index access
                addStepWithPredicate(plan, PLAN_INDEX_RANGE, order_p);

                addJoinStepsIfRequired(plan, q);

                if (q->order_direction == ORDER_DESC && !(q->flags & FLAG_GROUP)) {
                    addStep(plan, PLAN_REVERSE);
                }

                // Add step for predicates filter
                addStepWithPredicates(plan, PLAN_TABLE_ACCESS_ROWID, p, q->predicate_count);
            }
            /********************
             * TABLE ACCESS FULL
             ********************/
            else {
                if (q->table_count > 1) {
                    // First predicate is from first table
                    addStepWithPredicate(plan, PLAN_TABLE_ACCESS_FULL, p);

                    // The join
                    addJoinStepsIfRequired(plan, q);

                    if (q->predicate_count > 1) {
                        // Add the rest of the predicates after the join
                        addStepWithPredicates(plan, PLAN_TABLE_ACCESS_ROWID, q->predicates + 1, q->predicate_count - 1);
                    }
                } else {
                    // Only one table so add all predicates together
                    addStepWithPredicates(plan, PLAN_TABLE_ACCESS_FULL, q->predicates, q->predicate_count);
                }

                // Do we need an explicit sort?
                addOrderStepIfRequired(plan, q);
            }
        } else {

            // Predicate isn't on the first table need to do full access

            addStep(plan, PLAN_TABLE_ACCESS_FULL);

            addJoinStepsIfRequired(plan, q);

            addStepWithPredicates(plan, PLAN_TABLE_ACCESS_ROWID, q->predicates, q->predicate_count);

            addOrderStepIfRequired(plan, q);
        }
    }
    else if ((q->flags & FLAG_ORDER) && !(q->flags & FLAG_GROUP)) {
        // Before we do a full table scan... we have one more opportunity to use an index
        // To save a sort later, see if we can use an index for ordering now
        struct DB index_db;
        struct Table table = q->tables[0];
        if (findIndex(&index_db, table.name, q->order_field, INDEX_ANY) == 0) {
            closeDB(&index_db);

            struct Predicate *order_p = malloc(sizeof(*order_p));
            strcpy(order_p->left.text, q->order_field);
            order_p->op = 0;
            order_p->right.text[0] = '\0';

            addStepWithPredicate(plan, PLAN_INDEX_RANGE, order_p);

            if (q->order_direction == ORDER_DESC) {
                addStep(plan, PLAN_REVERSE);
            }

        } else {
            addStep(plan, PLAN_TABLE_ACCESS_FULL);

            addJoinStepsIfRequired(plan, q);

            addOrderStepIfRequired(plan, q);
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

    if (q->offset_value > 0 || q->limit_value >= 0) {
        addStepWithParams(plan, PLAN_SLICE, q->offset_value, q->limit_value);
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
    for (int j = 0; j < q->predicate_count; j++) {
        if (strcmp(q->predicates[j].left.text, q->order_field) == 0 && q->predicates[j].op == OPERATOR_EQ) {
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

void addJoinStepsIfRequired (struct Plan *plan, struct Query *q) {
    /*******************
     * JOIN
     *******************/
    for (int i = 1; i < q->table_count; i++) {
        addStep(plan, PLAN_CROSS_JOIN);
    }
}