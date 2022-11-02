#include <stdlib.h>
#include <string.h>

#include "structs.h"
#include "util.h"
#include "db.h"
#include "predicates.h"

static void addStep (struct Plan *plan, int type);

static void addOrderStepsIfRequired (struct Plan *plan, struct Query *q);

static void addStepWithPredicate (struct Plan *plan, int type, struct Predicate *p);

static void addStepWithPredicates (struct Plan *plan, int type, struct Predicate *p, int predicate_count);

static void addStepWithLimit (struct Plan *plan, int type, int limit);

static void addJoinStepsIfRequired (struct Plan *plan, struct Query *q);

static void addGroupStepIfRequired (struct Plan *plan, struct Query *query);

static void addLimitStepIfRequired (struct Plan *plan, struct Query *query);

static int optimisePredicates (struct Query *q, struct Predicate * predicates, int count);

static struct Predicate *makePredicate (struct ColumnNode *column, enum Operator op);

int makePlan (struct Query *q, struct Plan *plan) {
    plan->step_count = 0;

    // If there's no table specified then it must be a
    // single-row-all-constant query
    if (q->table_count == 0) {
        plan->step_count = 2;

        struct PlanStep * step = plan->steps;
        step->type = PLAN_DUMMY_ROW;
        step->predicate_count = 0;
        step->predicates = NULL;

        step = plan->steps + 1;
        step->type = PLAN_SELECT;
        step->predicate_count = 0;
        step->predicates = NULL;

        return plan->step_count;
    }

    if (q->predicate_count > 0) {

        // Try to find a predicate on the first table
        int predicatesOnFirstTable = optimisePredicates(q, q->predicates, q->predicate_count);

        // First table
        struct Table *table = &q->tables[0];

        // First predicate
        struct Predicate *p = &q->predicates[0];

        struct Field * field_left = p->left.fields;
        struct Field * field_right = p->right.fields;

        if (predicatesOnFirstTable > 0) {

            enum PlanStepType step_type = 0;
            size_t len = strlen(field_right->text);

            int skip_index = 0;

            // We know that CALENDAR can perform super efficient full table
            // scans with predicates.
            // Currently Index access can only use a single predicate at a
            // time which makes CALENDAR access much slower than it needs to
            // be.
            // In the future with real index ranges this special case could
            // probably be removed.
            if (predicatesOnFirstTable > 1 && table->db->vfs == VFS_CALENDAR) {
                struct Predicate *p2 = &q->predicates[1];

                if (strcmp(field_left->text, p2->left.fields[0].text) == 0) {
                    skip_index = 1;
                }
            }

            if (skip_index == 0) {
                // LIKE can only use index if '%' is at the end
                if (p->op == OPERATOR_LIKE && field_right->text[len-1] != '%') {
                    // NOP
                    step_type = 0;
                }
                else if (p->left.function == FUNC_PK) {

                    if (p->op == OPERATOR_EQ) {
                        step_type = PLAN_PK;
                    }
                    // Can't use PK index for LIKE yet
                    else if (p->op != OPERATOR_LIKE) {
                        step_type = PLAN_PK_RANGE;
                    }

                }
                // Can only do indexes on bare columns for now
                else if (p->left.function == FUNC_UNITY) {

                    // Remove qualified name so indexes can be searched etc.
                    int dot_index = str_find_index(field_left->text, '.');
                    if (dot_index >= 0) {
                        char value[MAX_FIELD_LENGTH];
                        strcpy(value, field_left->text);
                        strcpy(field_left->text, value + dot_index + 1);
                    }

                    /*******************
                     * INDEX SCAN
                     *******************/

                    // Try to find any index
                    enum IndexSearchType find_result = findIndex(NULL, table->name, field_left->text, INDEX_ANY);

                    if (find_result) {

                        if (find_result == INDEX_PRIMARY) {
                            if (p->op == OPERATOR_EQ) {
                                step_type = PLAN_PK;
                            }
                            // Can't use PK index for LIKE yet
                            else if (p->op != OPERATOR_LIKE) {
                                step_type = PLAN_PK_RANGE;
                            }
                        }
                        // LIKE makes any INDEX automatically non-unique
                        else if (find_result == INDEX_UNIQUE && p->op != OPERATOR_LIKE) {
                            if (p->op == OPERATOR_EQ) {
                                step_type = PLAN_UNIQUE;
                            } else {
                                step_type = PLAN_UNIQUE_RANGE;
                            }
                        }
                        // INDEX RANGE can handle LIKE
                        else {
                            step_type = PLAN_INDEX_RANGE;
                        }
                    }
                }
            }

            // If plan_type is set, that means we have an index
            if (step_type) {

                addStepWithPredicate(plan, step_type, p);

                addJoinStepsIfRequired(plan, q);

                if (q->predicate_count > 1) {
                    addStepWithPredicates(plan, PLAN_TABLE_ACCESS_ROWID, q->predicates + 1, q->predicate_count - 1);
                }

                if (step_type == PLAN_PK || step_type == PLAN_UNIQUE) {
                    // Reverse is never required if the plan is PLAN_PK or PLAN_UNIQUE
                } else if (q->flags & FLAG_GROUP){
                    // Reverse is not required if we're grouping
                } else if (q->flags & FLAG_ORDER) {
                    // Follow our own logic to add an order step
                    // We can avoid it if we're just going to sort on the same column we've just scanned
                    if (
                        (q->order_count == 1)
                        && q->order_node[0].function == FUNC_UNITY
                        && strcmp(field_left->text, q->order_node[0].fields[0].text) == 0
                    ) {

                        // So a sort is not necessary but we might still need to reverse
                        if (q->order_direction[0] == ORDER_DESC) {
                            addStep(plan, PLAN_REVERSE);
                        }
                    } else {
                        addOrderStepsIfRequired(plan, q);
                    }
                }
            }
            // Before we do a full table scan... we have one more opportunity to use an index
            // To save a sort later, see if we can use an index for ordering now
            else if (
                skip_index == 0
                && (q->flags & FLAG_ORDER)
                && q->order_count == 1
                // If we're selecting a lot of rows this optimisation is probably worth it.
                // If we have an EQ operator then it's probably cheaper to filter first.
                // Tested with `WHERE score = 42 ORDER BY name`
                // example times:
                //  Index, then filter:     real    0m3.012s
                //  Filter, then sort:      real    0m1.637s
                && (p->op != OPERATOR_EQ)
                && q->order_node[0].function == FUNC_UNITY
                && findIndex(NULL, table->name, q->order_node[0].fields[0].text, INDEX_ANY)
            ) {
                struct Predicate *order_p = makePredicate(&q->order_node[0], OPERATOR_ALWAYS);

                // Add step for Sorted index access
                addStepWithPredicate(plan, PLAN_INDEX_SCAN, order_p);

                // Optimisation: filter before join
                int skip_predicates = 0;
                for (int i = 0; i < q->predicate_count; i++) {
                    // If left and right are either constant or table 0 then we can filter
                    if (q->predicates[i].left.fields[0].table_id <= 0 &&
                        q->predicates[i].right.fields[0].table_id <= 0)
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

                if (q->order_direction[0] == ORDER_DESC && !(q->flags & FLAG_GROUP)) {
                    addStep(plan, PLAN_REVERSE);
                }

                // Add step for remaining predicates filter
                if (q->predicate_count > skip_predicates) {
                    addStepWithPredicates(plan, PLAN_TABLE_ACCESS_ROWID, q->predicates + skip_predicates, q->predicate_count - skip_predicates);
                }
            }
            else if ((q->flags & FLAG_GROUP) && q->group_node[0].function == FUNC_UNITY) {
                // Before we do a full table scan... we have one more opportunity to use an index
                // To save a sort later, see if we can use an index for ordering now
                struct Table *table = &q->tables[0];
                if (findIndex(NULL, table->name, q->group_node[0].fields[0].text, INDEX_ANY)) {

                    struct Predicate *group_p = makePredicate(&q->group_node[0], OPERATOR_ALWAYS);

                    addStepWithPredicate(plan, PLAN_INDEX_SCAN, group_p);

                    addStepWithPredicates(plan, PLAN_TABLE_ACCESS_ROWID, q->predicates, predicatesOnFirstTable);

                    addJoinStepsIfRequired(plan, q);

                    if (q->predicate_count > predicatesOnFirstTable) {
                        // Add the rest of the predicates after the join
                        addStepWithPredicates(plan, PLAN_TABLE_ACCESS_ROWID, q->predicates + predicatesOnFirstTable, q->predicate_count - predicatesOnFirstTable);
                    }

                    // In this case it's OK to apply limit optimisation to Join step
                    // even though we have an ORDER BY clause.
                    struct PlanStep *prev = &plan->steps[plan->step_count - 1];
                    if (q->limit_value >= 0) {
                        // Usually this can't be done with ORDER BY but in this case we
                        // can since there are no predicates
                        prev->limit = q->offset_value + q->limit_value;
                    }

                } else {
                    addStepWithPredicates(plan, PLAN_TABLE_SCAN, q->predicates, predicatesOnFirstTable);

                    addJoinStepsIfRequired(plan, q);

                    if (q->predicate_count > predicatesOnFirstTable) {
                        // Add the rest of the predicates after the join
                        addStepWithPredicates(plan, PLAN_TABLE_ACCESS_ROWID, q->predicates + predicatesOnFirstTable, q->predicate_count - predicatesOnFirstTable);
                    }
                }
            }
            /********************
             * TABLE ACCESS FULL
             ********************/
            else {
                if (q->table_count > 1) {
                    // First predicates are from first table
                    addStepWithPredicates(plan, PLAN_TABLE_SCAN, q->predicates, predicatesOnFirstTable);

                    // The join
                    addJoinStepsIfRequired(plan, q);

                    if (q->predicate_count > predicatesOnFirstTable) {
                        // Add the rest of the predicates after the join
                        addStepWithPredicates(plan, PLAN_TABLE_ACCESS_ROWID, q->predicates + predicatesOnFirstTable, q->predicate_count - predicatesOnFirstTable);
                    }
                } else {
                    // Only one table so add all predicates together
                    addStepWithPredicates(plan, PLAN_TABLE_SCAN, q->predicates, q->predicate_count);

                }

                addOrderStepsIfRequired(plan, q);
            }
        } else {

            // Predicate isn't on the first table need to do full access

            addStep(plan, PLAN_TABLE_SCAN);

            addJoinStepsIfRequired(plan, q);

            addStepWithPredicates(plan, PLAN_TABLE_ACCESS_ROWID, q->predicates, q->predicate_count);

            addOrderStepsIfRequired(plan, q);
        }
    }
    else if (
            (q->flags & FLAG_ORDER)
            && q->order_count == 1
            && q->order_node[0].function == FUNC_UNITY
            && !(q->flags & FLAG_GROUP)
        ) {
        // Before we do a full table scan... we have one more opportunity to use an index
        // To save a sort later, see if we can use an index for ordering now
        struct Table *table = &q->tables[0];
        if (findIndex(NULL, table->name, q->order_node[0].fields[0].text, INDEX_ANY)) {

            struct Predicate *order_p = makePredicate(&q->order_node[0], OPERATOR_ALWAYS);

            addStepWithPredicate(plan, PLAN_INDEX_SCAN, order_p);

            if (q->order_direction[0] == ORDER_DESC) {
                addStep(plan, PLAN_REVERSE);
            }

            addJoinStepsIfRequired(plan, q);

            // In this case it's OK to apply limit optimisation to Join step
            // even though we have an ORDER BY clause.
            struct PlanStep *prev = &plan->steps[plan->step_count - 1];
            if (q->limit_value >= 0) {
                // Usually this can't be done with ORDER BY but in this case we
                // can since there are no predicates
                prev->limit = q->offset_value + q->limit_value;
            }

        } else {
            addStep(plan, PLAN_TABLE_SCAN);

            addJoinStepsIfRequired(plan, q);

            addOrderStepsIfRequired(plan, q);
        }
    }
    else if (
        (q->flags & FLAG_GROUP)
        && q->group_node[0].function == FUNC_UNITY
    ) {
        // Before we do a full table scan... we have one more opportunity to use an index
        // To save a sort later, see if we can use an index for grouping now

        struct Table *table = &q->tables[0];
        if (findIndex(NULL, table->name, q->group_node[0].fields[0].text, INDEX_ANY)) {

            struct Predicate *group_p = makePredicate(&q->group_node[0], OPERATOR_ALWAYS);

            addStepWithPredicate(plan, PLAN_INDEX_SCAN, group_p);

            addJoinStepsIfRequired(plan, q);

            // In this case it's OK to apply limit optimisation to Join step
            // even though we have an ORDER BY clause.
            struct PlanStep *prev = &plan->steps[plan->step_count - 1];
            if (q->limit_value >= 0) {
                // Usually this can't be done with ORDER BY but in this case we
                // can since there are no predicates
                prev->limit = q->offset_value + q->limit_value;
            }

        } else {
            addStep(plan, PLAN_TABLE_SCAN);

            addJoinStepsIfRequired(plan, q);
        }
    }
    else {
        addStep(plan, PLAN_TABLE_SCAN);

        addJoinStepsIfRequired(plan, q);

        addOrderStepsIfRequired(plan, q);
    }

    addGroupStepIfRequired(plan, q);

    addLimitStepIfRequired(plan, q);

    addStep(plan, PLAN_SELECT);

    return plan->step_count;
}

void destroyPlan (struct Plan *plan) {
    for (int i = 0; i < plan->step_count; i++) {
        // don't double free
        // Most predicates will be free'd in destroyQuery
        if (plan->steps[i].predicates == NULL) {
            free(plan->steps[i].predicates);
            plan->steps[i].predicates = NULL;
        }
    }
}

static void addStep (struct Plan *plan, int type) {
    int i = plan->step_count;

    plan->steps[i].predicate_count = 0;
    plan->steps[i].predicates = NULL;
    plan->steps[i].type = type;
    plan->steps[i].limit = -1;

    plan->step_count++;
}

/**
 * Will not necessarily add order step.
 * Will check if it's necessary
 */
static void addOrderStepsIfRequired (struct Plan *plan, struct Query *q) {
    // Check if there's an order by clause
    if (!(q->flags & FLAG_ORDER)) {
        return;
    }

    // At the moment grouping queries cannot be sorted in the same process
    if (q->flags & FLAG_GROUP) {
        return;
    }

    // If we're sorting on rowid or PK on single table then we can optimse
    if (q->order_count > 0 && q->table_count == 1) {
        struct PlanStep *first = &plan->steps[0];

        // Only works if results were retrieved in table order
        if (first->type == PLAN_TABLE_SCAN || first->type == PLAN_TABLE_SCAN) {

            if (
                q->order_node[0].function == FUNC_UNITY
                && (strcmp(q->order_node[0].fields[0].text, "rowid") == 0 || strcmp(q->order_node[0].fields[0].text, "PK") == 0)
            ) {
                // No need to sort

                if (q->order_direction[0] == ORDER_DESC) {
                    // Just need to reverse
                    addStep(plan, PLAN_REVERSE);
                }

                return;
            }
        }
    }

    // Iterate the ORDER BY steps in reverse order.
    // Columns with equality predicates don't need sorting.
    //
    // e.g. a query like this:
    //      ORDER BY A, B, C, D WHERE B = 0
    //
    // will add the following steps:
    //      * SORT D
    //      * SORT C
    //      * SORT A
    //
    // This results in A being the primary sort key, then C and finally D.
    // Yes, it is inefficient to sort the whole list three times, but it is
    // simple.

    int sorts_added = 0;

    for (int i = q->order_count - 1; i >= 0 ; i--) {
        int sort_needed = 1;

        // If there is an equality predicate on this column then there is no
        // need to ever sort by it.
        for (int j = 0; j < q->predicate_count; j++) {
            if (
                q->predicates[j].op == OPERATOR_EQ
                && strcmp(q->predicates[j].left.fields[0].text, q->order_node[i].fields[0].text) == 0
                && q->predicates[j].left.function == q->order_node[i].function
            ) {
                // OK we don't need to sort by this field

                sort_needed = 0;
                break;
            }
        }

        // If this is the first sort to add (last in list), we might not need
        // to sort if the results are already in the right order.
        if (sort_needed && sorts_added == 0) {
            struct PlanStep *first = plan->steps;

            if (first->type == PLAN_INDEX_RANGE || first->type == PLAN_UNIQUE_RANGE) {
                if (
                    first->predicates[0].left.function == q->order_node[i].function
                    && strcmp(first->predicates[0].left.fields[0].text, q->order_node[i].fields[0].text) == 0
                ) {
                    sort_needed = 0;

                    // We don't need to sort, but we might need to reverse
                    if (q->order_direction[i] == ORDER_DESC) {
                        addStep(plan, PLAN_REVERSE);
                    }

                    // This still counts as sorting as far as multi-step sorting goes
                    sorts_added++;
                }
            }
        }

        if (sort_needed) {
            if (sorts_added > 0 && q->order_direction[i] == ORDER_DESC) {
                // If there are sort steps lower in precedence, and this one is
                // DESC then the results need to be flipped first.

                struct PlanStep *prev = &plan->steps[plan->step_count - 1];
                if (prev->type == PLAN_REVERSE) {
                    // No need to reverse twice in a row
                    plan->step_count--;
                }
                else {
                    addStep(plan, PLAN_REVERSE);
                }
            }

            struct Predicate *order_predicate = makePredicate(&q->order_node[i], (enum Operator)q->order_direction[i]);

            addStepWithPredicate(plan, PLAN_SORT, order_predicate);

            sorts_added++;
        }
    }

    // To implement better sort later
    // addStepWithPredicates(plan, PLAN_SORT, order_predicates, q->order_count);
}

static void addStepWithPredicate (struct Plan *plan, int type, struct Predicate *p) {
    addStepWithPredicates(plan, type, p, 1);
}

static void addStepWithPredicates (struct Plan *plan, int type, struct Predicate *p, int predicate_count) {
    int i = plan->step_count;

    plan->steps[i].predicate_count = predicate_count;
    plan->steps[i].predicates = p;
    plan->steps[i].type = type;
    plan->steps[i].limit = -1;

    plan->step_count++;
}

static void addStepWithLimit (struct Plan *plan, int type, int limit) {
    int i = plan->step_count;

    plan->steps[i].predicate_count = 0;
    plan->steps[i].predicates = NULL;
    plan->steps[i].type = type;
    plan->steps[i].limit = limit;

    plan->step_count++;
}

static void addJoinStepsIfRequired (struct Plan *plan, struct Query *q) {
    /*******************
     * JOIN
     *******************/
    for (int i = 1; i < q->table_count; i++) {
        struct Table * table = q->tables + i;
        struct Predicate * join = &table->join;

        if (join->op == OPERATOR_ALWAYS) {
            addStep(plan, PLAN_CROSS_JOIN);
        }
        else {
            if (join->left.fields[0].index == FIELD_CONSTANT ||
                join->right.fields[0].index == FIELD_CONSTANT)
            {
                addStepWithPredicate(plan, PLAN_CONSTANT_JOIN, join);
            }
            else {

                if (join->op == OPERATOR_EQ) {
                    int join_result_left = findIndex(NULL, table->name, join->left.fields[0].text, INDEX_UNIQUE);
                    int join_result_right = findIndex(NULL, table->name, join->right.fields[0].text, INDEX_UNIQUE);

                    if (
                        join_result_left == INDEX_UNIQUE || join_result_left == INDEX_PRIMARY ||
                        join_result_right == INDEX_UNIQUE || join_result_right == INDEX_PRIMARY
                    ) {
                        addStepWithPredicate(plan, PLAN_UNIQUE_JOIN, join);
                    }
                    else {
                        addStepWithPredicate(plan, PLAN_LOOP_JOIN, join);
                    }
                }
                else {
                    addStepWithPredicate(plan, PLAN_LOOP_JOIN, join);
                }
            }
        }
    }
}

static void addGroupStepIfRequired (struct Plan *plan, struct Query *q) {

    /*******************
     * Grouping
     *******************/
    if (q->flags & FLAG_GROUP && q->group_count > 0) {
        struct Predicate *group_predicate = makePredicate(&q->group_node[0], OPERATOR_UN);

        // Grouping *requires* sorting
        // We'll check if we can get away without sorting explicitly.
        // This check is less than perfect.
        int sort_required = 1;

        if (q->group_node[0].function == FUNC_UNITY) {
            struct PlanStep *first = &plan->steps[0];
            char *first_predicate_value = first->predicates[0].left.fields[0].text;

            if (
                (first->type == PLAN_INDEX_RANGE || first->type == PLAN_UNIQUE_RANGE)
                && strcmp(first_predicate_value, q->group_node[0].fields[0].text) == 0
            ) {
                // Already sorted on our field
                sort_required = 0;
            }
        }

        if (sort_required) {
            addStepWithPredicate(plan, PLAN_SORT, group_predicate);
        }

        addStepWithPredicate(plan, PLAN_GROUP, group_predicate);

        if (q->limit_value >= 0) {
            struct PlanStep *prev = &plan->steps[plan->step_count - 1];

            prev->limit = q->limit_value;
        }
    }
}

static void addLimitStepIfRequired (struct Plan *plan, struct Query *query) {
    /********************
     * OFFSET/FETCH FIRST
     ********************/

    if (query->limit_value >= 0) {
        int limit = query->offset_value + query->limit_value;

        struct PlanStep *prev = &plan->steps[plan->step_count - 1];

        // Optimisation: Apply limit to previous step if possible instead of
        // adding explicit SLICE step.

        if (prev->type == PLAN_PK || prev->type == PLAN_UNIQUE) {
            // These steps never need a limit applied
            return;
        }

        // PLAN_SORT is incapable of self limiting
        if (prev->type != PLAN_SORT)
        {
            // If there's no previous limit or the limit was higher than
            // necessary we'll set our limit.
            if (prev->limit == -1 || prev->limit > limit) {
                prev->limit = limit;
            }
            return;
        }

        // We need to add explicit step
        addStepWithLimit(plan, PLAN_SLICE, limit);
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

    // First Step: We really want a PRIMARY KEY
    for (int i = 0; i < count; i++) {
        // Swap left and right if necessary
        normalisePredicate(predicates + i);

        if (predicates[i].left.function == FUNC_PK && predicates[i].left.fields[0].table_id == 0) {
            chosen_predicate_index = i;
            break;
        }
    }

    // If we didn't find a primary key, then move on to the next optimisation step
    if (chosen_predicate_index < 0) {

        for (int i = 0; i < count; i++) {
            // Any predicate on the first table is fine
            // Comment: Only checking left?
            if (predicates[i].left.fields[0].table_id == 0) {
                chosen_predicate_index = i;
                break;
            }
        }
    }

    // Swap predicates so first predicate is on first table
    if (chosen_predicate_index > 0) {
        struct Predicate tmp;
        memcpy(&tmp, &predicates[0], sizeof(tmp));
        memcpy(&predicates[0], &predicates[chosen_predicate_index], sizeof(tmp));
        memcpy(&predicates[chosen_predicate_index], &tmp, sizeof(tmp));
    }

    // We found at least one predicate on the first table now just count how
    // many are already in place
    if (chosen_predicate_index >= 0) {
        int i = 1;
        while (predicates[i].left.fields[0].table_id == 0 && i < count) {
            i++;
        }
        return i;
    }

    return 0;
}

static struct Predicate *makePredicate (struct ColumnNode *column, enum Operator op) {
    struct Predicate *predicate = malloc(sizeof(*predicate));

    memcpy(&predicate->left, column, sizeof(*column));

    predicate->op = op;

    predicate->right.fields[0].table_id = -1;
    predicate->right.fields[0].index = FIELD_UNKNOWN;
    predicate->right.fields[0].text[0] = '\0';

    return predicate;
}