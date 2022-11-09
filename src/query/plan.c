#include <stdlib.h>
#include <string.h>

#include "../structs.h"
#include "../functions/util.h"
#include "../db/db.h"
#include "../evaluate/predicates.h"
#include "../evaluate/evaluate.h"

static struct PlanStep *addStep (struct Plan *plan, int type);

static struct PlanStep *addStepWithNode (
    struct Plan *plan,
    int type,
    struct Node *node
);

static struct PlanStep *addStepWithNodes (
    struct Plan *plan,
    int type,
    struct Node *nodes,
    int node_count
);

static struct PlanStep *addStepWithLimit (
    struct Plan *plan,
    int type,
    int limit
);

static void addOrderStepsIfRequired (struct Plan *plan, struct Query *q);

static void addJoinStepsIfRequired (struct Plan *plan, struct Query *q);

static void addGroupStepIfRequired (struct Plan *plan, struct Query *query);

static void addLimitStepIfRequired (struct Plan *plan, struct Query *query);

static int optimiseNodes (
    struct Query *q,
    struct Node * predicates,
    int count
);

static int applySortLogic (
    struct Query *q,
    struct Plan *plan,
    int *sorts_needed
);

static int areNodesEqual (struct Node *nodeA, struct Node *nodeB);

int makePlan (struct Query *q, struct Plan *plan) {
    plan->step_count = 0;

    // If there's no table specified then it must be a
    // single-row-all-constant query
    if (q->table_count == 0) {
        addStep(plan, PLAN_DUMMY_ROW);

        addStepWithNodes(plan, PLAN_SELECT, q->columns, q->column_count);

        return plan->step_count;
    }

    if (q->predicate_count > 0) {

        // Try to find a predicate on the first table
        int predicatesOnFirstTable = optimiseNodes(
            q,
            q->predicate_nodes,
            q->predicate_count
        );

        // First table
        struct Table *table = &q->tables[0];

        // First predicate
        struct Node *p = &q->predicate_nodes[0];
        enum Function op = p->function;

        struct Node *left = &p->children[0];
        struct Field *field_left = (struct Field *)left;
        struct Field *field_right = (struct Field *)&p->children[1];
            // struct Node *predicate = &predicates[j];
            // struct Node *left = &predicate->children[0];
            // struct Node *right = &predicate->children[1];

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
                struct Node *p2 = &q->predicate_nodes[1];
                struct Field *p2_field_left = (struct Field *)&p2->children[0];

                if (strcmp(field_left->text, p2_field_left->text) == 0) {
                    skip_index = 1;
                }
            }

            if (skip_index == 0) {
                if (field_left->index == FIELD_ROW_INDEX) {
                    step_type = PLAN_TABLE_SCAN;
                }
                // LIKE can only use index if '%' is at the end
                else if (
                    op == OPERATOR_LIKE
                    && field_right->text[len-1] != '%'
                ) {
                    // NOP
                    step_type = 0;
                }
                else if (left->function == FUNC_PK) {

                    if (op == OPERATOR_EQ) {
                        step_type = PLAN_PK;
                    }
                    // Can't use PK index for LIKE yet
                    else if (op != OPERATOR_LIKE) {
                        step_type = PLAN_PK_RANGE;
                    }

                }
                // Can only do indexes on bare columns for now
                else if (left->function == FUNC_UNITY) {

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
                    enum IndexSearchType find_result = findIndex(
                        NULL,
                        table->name,
                        field_left->text,
                        INDEX_ANY
                    );

                    if (find_result) {

                        if (find_result == INDEX_PRIMARY) {
                            if (op == OPERATOR_EQ) {
                                step_type = PLAN_PK;
                            }
                            // Can't use PK index for LIKE yet
                            else if (op != OPERATOR_LIKE) {
                                step_type = PLAN_PK_RANGE;
                            }
                        }
                        // LIKE makes any INDEX automatically non-unique
                        else if (
                            find_result == INDEX_UNIQUE
                            && op != OPERATOR_LIKE
                        ) {
                            if (op == OPERATOR_EQ) {
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
                addStepWithNode(plan, step_type, &q->predicate_nodes[0]);

                addJoinStepsIfRequired(plan, q);

                if (q->predicate_count > 1) {
                    addStepWithNodes(
                        plan,
                        PLAN_TABLE_ACCESS_ROWID,
                        q->predicate_nodes + 1,
                        q->predicate_count - 1
                    );
                }

                if (step_type == PLAN_PK || step_type == PLAN_UNIQUE) {
                    // Reverse is never required if the plan is PLAN_PK or
                    // PLAN_UNIQUE.
                } else if (q->flags & FLAG_GROUP){
                    // Reverse is not required if we're grouping.
                } else if (q->order_count > 0) {
                    // Follow our own logic to add an order step
                    // We can avoid it if we're just going to sort on the same
                    // column we've just scanned.
                    if (
                        (q->order_count == 1)
                        && q->order_nodes[0].function == FUNC_UNITY
                        && strcmp(
                            field_left->text,
                            q->order_nodes[0].field.text
                        ) == 0
                    ) {

                        // So a sort is not necessary but we might still need to
                        // reverse
                        if (q->order_nodes[0].alias[0] == ORDER_DESC) {
                            addStep(plan, PLAN_REVERSE);
                        }
                    } else {
                        addOrderStepsIfRequired(plan, q);
                    }
                }
            }
            // Before we do a full table scan... we have one more opportunity to
            // use an index to save a sort later, see if we can use an index for
            // ordering now.
            else if (
                skip_index == 0
                && q->order_count == 1
                // If we're selecting a lot of rows this optimisation is
                // probably worth it. If we have an EQ operator then it's
                // probably cheaper to filter first.
                //
                // Tested with `WHERE score = 42 ORDER BY name`
                // example times:
                //  Index, then filter:     real    0m3.012s
                //  Filter, then sort:      real    0m1.637s
                && (op != OPERATOR_EQ)
                && q->order_nodes[0].function == FUNC_UNITY
                && findIndex(
                    NULL,
                    table->name,
                    q->order_nodes[0].field.text,
                    INDEX_ANY
                )
            ) {
                // Add step for Sorted index access
                addStepWithNode(plan, PLAN_INDEX_SCAN, &q->order_nodes[0]);

                // Optimisation: filter before join
                int skip_predicates = 0;
                for (int i = 0; i < q->predicate_count; i++) {
                    // If left and right are either constant or table 0 then we
                    // can filter
                    if (
                        q->predicate_nodes[i].children[0].field.table_id <= 0
                        && q->predicate_nodes[i].children[1].field.table_id <= 0
                    ) {
                        skip_predicates++;
                    } else {
                        break;
                    }
                }

                if (skip_predicates > 0) {
                    addStepWithNodes(
                        plan,
                        PLAN_TABLE_ACCESS_ROWID,
                        q->predicate_nodes,
                        skip_predicates
                    );
                }

                addJoinStepsIfRequired(plan, q);

                if (
                    q->order_nodes[0].alias[0] == ORDER_DESC
                    && !(q->flags & FLAG_GROUP)
                ) {
                    addStep(plan, PLAN_REVERSE);
                }

                // Add step for remaining predicates filter
                if (q->predicate_count > skip_predicates) {
                    addStepWithNodes(
                        plan,
                        PLAN_TABLE_ACCESS_ROWID,
                        q->predicate_nodes + skip_predicates,
                        q->predicate_count - skip_predicates
                    );
                }
            }
            else if (
                (q->flags & FLAG_GROUP)
                && q->group_nodes[0].function == FUNC_UNITY
            ) {
                // Before we do a full table scan... we have one more
                // opportunity to use an index to save a sort later, see if we
                // can use an index for ordering now
                struct Table *table = &q->tables[0];
                if (findIndex(
                    NULL,
                    table->name,
                    q->group_nodes[0].field.text,
                    INDEX_ANY
                )) {

                    addStepWithNode(plan, PLAN_INDEX_SCAN, &q->group_nodes[0]);

                    addStepWithNodes(
                        plan,
                        PLAN_TABLE_ACCESS_ROWID,
                        q->predicate_nodes,
                        predicatesOnFirstTable
                    );

                    addJoinStepsIfRequired(plan, q);

                    if (q->predicate_count > predicatesOnFirstTable) {
                        // Add the rest of the predicates after the join
                        addStepWithNodes(
                            plan,
                            PLAN_TABLE_ACCESS_ROWID,
                            q->predicate_nodes + predicatesOnFirstTable,
                            q->predicate_count - predicatesOnFirstTable
                        );
                    }

                    // In this case it's OK to apply limit optimisation to Join
                    // step even though we have an ORDER BY clause.
                    struct PlanStep *prev = &plan->steps[plan->step_count - 1];
                    if (q->limit_value >= 0) {
                        // Usually this can't be done with ORDER BY but in this
                        // case we can since there are no predicates.
                        prev->limit = q->offset_value + q->limit_value;
                    }

                } else {
                    addStepWithNodes(
                        plan,
                        PLAN_TABLE_ACCESS_FULL,
                        q->predicate_nodes,
                        predicatesOnFirstTable
                    );

                    addJoinStepsIfRequired(plan, q);

                    if (q->predicate_count > predicatesOnFirstTable) {
                        // Add the rest of the predicates after the join
                        addStepWithNodes(
                            plan,
                            PLAN_TABLE_ACCESS_ROWID,
                            q->predicate_nodes + predicatesOnFirstTable,
                            q->predicate_count - predicatesOnFirstTable
                        );
                    }
                }
            }
            /********************
             * TABLE ACCESS FULL
             ********************/
            else {
                if (q->table_count > 1) {

                    // First predicates are from first table
                    addStepWithNodes(
                        plan,
                        PLAN_TABLE_ACCESS_FULL,
                        q->predicate_nodes,
                        predicatesOnFirstTable
                    );

                    // The join
                    addJoinStepsIfRequired(plan, q);

                    if (q->predicate_count > predicatesOnFirstTable) {
                        // Add the rest of the predicates after the join
                        addStepWithNodes(
                            plan,
                            PLAN_TABLE_ACCESS_ROWID,
                            q->predicate_nodes + predicatesOnFirstTable,
                            q->predicate_count - predicatesOnFirstTable
                        );
                    }
                } else {
                    // Only one table so add all predicates together
                    addStepWithNodes(
                        plan,
                        PLAN_TABLE_ACCESS_FULL,
                        q->predicate_nodes,
                        q->predicate_count
                    );
                }

                addOrderStepsIfRequired(plan, q);
            }
        } else {

            // Node isn't on the first table need to do full access

            addStep(plan, PLAN_TABLE_SCAN);

            addJoinStepsIfRequired(plan, q);

            addStepWithNodes(
                plan,
                PLAN_TABLE_ACCESS_ROWID,
                q->predicate_nodes,
                q->predicate_count
            );

            addOrderStepsIfRequired(plan, q);
        }
    }
    else if (
        q->order_count >= 1
        && q->order_nodes[0].function == FUNC_UNITY
        && !(q->flags & FLAG_GROUP)
    ) {
        // Before we do a full table scan... we have one more opportunity to use
        // an index to save a sort later, see if we can use an index for
        // ordering now.

        struct Table *table = &q->tables[0];

        enum IndexSearchType index_type = findIndex(
            NULL,
            table->name,
            q->order_nodes[0].field.text,
            INDEX_ANY
        );

        if (index_type != INDEX_NONE) {
            if (index_type == INDEX_UNIQUE) {
                addStepWithNode(plan, PLAN_UNIQUE_RANGE, &q->order_nodes[0]);
            }
            else {
                addStepWithNode(plan, PLAN_INDEX_SCAN, &q->order_nodes[0]);
            }

            addJoinStepsIfRequired(plan, q);

            addOrderStepsIfRequired(plan, q);

        } else {
            addStep(plan, PLAN_TABLE_SCAN);

            addJoinStepsIfRequired(plan, q);

            addOrderStepsIfRequired(plan, q);
        }
    }
    else if (
        (q->flags & FLAG_GROUP)
        && q->group_nodes[0].function == FUNC_UNITY
    ) {
        // Before we do a full table scan... we have one more opportunity to use
        // an index to save a sort later, see if we can use an index for
        // grouping now.

        struct Table *table = &q->tables[0];
        if (findIndex(
            NULL,
            table->name,
            q->group_nodes[0].field.text,
            INDEX_ANY
        )) {

            addStepWithNode(plan, PLAN_INDEX_SCAN, &q->group_nodes[0]);

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

    if (q->offset_value > 0) {
        addStepWithLimit(plan, PLAN_OFFSET, q->offset_value);
    }

    addStepWithNodes(plan, PLAN_SELECT, q->columns, q->column_count);

    return plan->step_count;
}

void destroyPlan (struct Plan *plan) {
    for (int i = 0; i < plan->step_count; i++) {
        if (plan->steps[i].nodes != NULL) {
            free(plan->steps[i].nodes);
            plan->steps[i].nodes = NULL;
        }
    }
}

static struct PlanStep *addStep (struct Plan *plan, int type) {
    int i = plan->step_count;

    plan->steps[i].node_count = 0;
    plan->steps[i].nodes = NULL;
    plan->steps[i].type = type;
    plan->steps[i].limit = -1;

    plan->step_count++;

    return &plan->steps[i];
}

/**
 * @brief
 *
 * @param plan
 * @param type
 * @param node addStep() will malloc a copy of this node
 * @return struct PlanStep*
 */
static struct PlanStep *addStepWithNode (
    struct Plan *plan,
    int type,
    struct Node *node
) {
    return addStepWithNodes(plan, type, node, 1);
}

/**
 * @brief
 *
 * @param plan
 * @param type
 * @param nodes addStep() will malloc a copy of these nodes
 * @param node_count
 * @return struct PlanStep*
 */
static struct PlanStep *addStepWithNodes (
    struct Plan *plan,
    int type,
    struct Node *nodes,
    int node_count
) {
    struct Node *ps = malloc(sizeof(*ps) * node_count);
    memcpy(ps, nodes, sizeof(*ps) * node_count);

    int i = plan->step_count;

    plan->steps[i].node_count = node_count;
    plan->steps[i].nodes = ps;
    plan->steps[i].type = type;
    plan->steps[i].limit = -1;

    plan->step_count++;

    return &plan->steps[i];
}

static struct PlanStep *addStepWithLimit (struct Plan *plan, int type, int limit) {
    int i = plan->step_count;

    plan->steps[i].node_count = 0;
    plan->steps[i].nodes = NULL;
    plan->steps[i].type = type;
    plan->steps[i].limit = limit;

    plan->step_count++;

    return &plan->steps[i];
}

/**
 * Will not necessarily add order step.
 * Will check if it's necessary
 */
static void addOrderStepsIfRequired (struct Plan *plan, struct Query *q) {
    // Check if there's an order by clause
    if (q->order_count == 0) {
        return;
    }

    if (q->flags & FLAG_GROUP) {
        return;
    }

    // indices of sort columns required
    int sorts_needed[10] = {0};

    int sorts_added = applySortLogic(q, plan, sorts_needed);

    if (sorts_added == 0) {
        return;
    }

    // We might just need a REVERSE
    if (sorts_added == -1) {
        addStep(plan, PLAN_REVERSE);
    }
    else {
        struct Node *tmp_node_list = malloc(
            sizeof(*tmp_node_list) * sorts_added
        );

        for (int i = 0; i < sorts_added; i++) {
            int idx = sorts_needed[i];

            memcpy(
                &tmp_node_list[i],
                &q->order_nodes[idx],
                sizeof(*tmp_node_list)
            );
        }

        addStepWithNodes(plan, PLAN_SORT, tmp_node_list, sorts_added);

        free(tmp_node_list);
    }
}

static void addJoinStepsIfRequired (struct Plan *plan, struct Query *q) {
    /*******************
     * JOIN
     *******************/
    for (int i = 1; i < q->table_count; i++) {
        struct Table *table = q->tables + i;
        struct Node *join = &table->join;

        enum Function op = join->function;

        if (op == OPERATOR_ALWAYS) {
            // Still need to include predicate to indicate to the executor
            // which table to join.
            join->field.table_id = i;
            addStepWithNode(plan, PLAN_CROSS_JOIN, join);
        }
        else {

            struct Field *left_field = (struct Field *)&join->children[0];
            struct Field *right_field = (struct Field *)&join->children[1];

            // We'll define that table-to-be-joined MUST be on left
            if (left_field->table_id != i) {
                if (right_field->table_id != i) {
                    fprintf(stderr, "At least one of the predicates must be on "
                        "the joined table.\n");
                    exit(-1);
                }

                // Swap left and right so that preciate looks like this:
                //  A JOIN B ON B.field = A.field
                flipPredicate(join);
            }

            // Constant must be on right if there is one
            if (right_field->index == FIELD_CONSTANT)
            {
                addStepWithNode(plan, PLAN_CONSTANT_JOIN, join);
            }
            else if (right_field->table_id == i) {
                // Both sides of predicate are on same table
                // We can do contsant join
                addStepWithNode(plan, PLAN_CONSTANT_JOIN, join);
            }
            else {
                int index_result = findIndex(
                    NULL,
                    table->name,
                    left_field->text,
                    INDEX_ANY
                );

                if (op == OPERATOR_EQ) {
                    if (
                        index_result == INDEX_UNIQUE
                        || index_result == INDEX_PRIMARY
                    ) {
                        addStepWithNode(plan, PLAN_UNIQUE_JOIN, join);
                    }
                    else if (index_result == INDEX_REGULAR) {
                        addStepWithNode(plan, PLAN_INDEX_JOIN, join);
                    }
                    else {
                        addStepWithNode(plan, PLAN_LOOP_JOIN, join);
                    }
                }
                else {
                    if (index_result != INDEX_NONE) {
                        addStepWithNode(plan, PLAN_INDEX_JOIN, join);
                    }
                    else {
                        addStepWithNode(plan, PLAN_LOOP_JOIN, join);
                    }
                }
            }
        }
    }
}

static void addGroupStepIfRequired (struct Plan *plan, struct Query *q) {

    /*******************
     * Grouping
     *******************/

    // Aggregate function on all rows, single row output
    if (q->flags & FLAG_GROUP && q->group_count == 0) {
        struct PlanStep *prev = addStep(plan, PLAN_GROUP);

        if (q->limit_value >= 0) {
            prev->limit = q->offset_value + q->limit_value;
        }

        return;
    }

    if (q->flags & FLAG_GROUP && q->group_count > 0) {
        struct Node *group_node = &q->group_nodes[0];

        // Check if we can use sorted grouping first
        struct PlanStep *first_step = &plan->steps[0];
        if (
            first_step->type == PLAN_INDEX_RANGE
            || first_step->type == PLAN_INDEX_SCAN
        ) {
            struct Node *first_node = &first_step->nodes[0];

            if (areNodesEqual(first_node, group_node)) {
                struct PlanStep *prev = addStepWithNode(
                    plan,
                    PLAN_GROUP_SORTED,
                    group_node
                );

                if (q->limit_value > -1) {
                    prev->limit = q->offset_value + q->limit_value;
                }

                return;
            }
        }

        struct PlanStep *prev = addStepWithNode(plan, PLAN_GROUP, group_node);

        if (q->limit_value >= 0) {
            prev->limit = q->offset_value + q->limit_value;
        }
    }
}

static void addLimitStepIfRequired (struct Plan *plan, struct Query *query) {
    /********************
     * OFFSET/FETCH FIRST
     ********************/

    if (query->limit_value >= 0) {
        int limit = query->offset_value + query->limit_value;

        if (
            query->predicate_count == 0
            && query->order_count == 0
            && !(query->flags & FLAG_GROUP)
        ) {
            // Check if all LEFT UNIQUE joins
            int all_left_unique = 1;

            for (int i = 1; i < plan->step_count; i++) {
                if (
                    plan->steps[i].type == PLAN_CONSTANT_JOIN
                    || plan->steps[i].type == PLAN_LOOP_JOIN
                    || plan->steps[i].type == PLAN_CROSS_JOIN
                    || plan->steps[i].type == PLAN_INDEX_JOIN
                ) {
                    all_left_unique = 0;
                    break;
                }

                if (plan->steps[i].type == PLAN_UNIQUE_JOIN) {
                    // Defined to be this table join ID on left
                    struct Field *join_field
                        = (struct Field *)&plan->steps[i].nodes[0];

                    int table_id = join_field->table_id;

                    if (query->tables[table_id].join_type != JOIN_LEFT) {
                        all_left_unique = 0;
                        break;
                    }
                }
            }

            // If we have no joins, or only unique left joins; then we can
            // apply the limit to the first "{table|index} {access|scan}""
            if (all_left_unique) {
                plan->steps[0].limit = limit;
                return;
            }
        }

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
 * @brief Re-order predicates so first N predicates only include first table and
 * constants.
 *
 * @param q
 * @param predicates
 * @param count
 * @return int N, number of predicates on first table
 */
static int optimiseNodes (
    __attribute__((unused)) struct Query *q,
    struct Node * predicates,
    int count
) {
    int chosen_predicate_index = -1;

    // First Step: We really want a PRIMARY KEY
    for (int i = 0; i < count; i++) {
        // Swap left and right if necessary
        normalisePredicate(predicates + i);

        struct Node *left = &predicates[i].children[0];

        if (left->function == FUNC_PK && left->field.table_id == 0) {
            chosen_predicate_index = i;
            break;
        }
    }

    // If we didn't find a primary key, then move on to the next optimisation
    // step.
    if (chosen_predicate_index < 0) {
        for (int i = 0; i < count; i++) {
            struct Node *left = &predicates[i].children[0];

            // Any predicate on the first table is fine
            // Comment: Only checking left?
            if (left->field.table_id == 0) {
                chosen_predicate_index = i;
                break;
            }
        }
    }

    // Swap predicates so first predicate is on first table
    if (chosen_predicate_index > 0) {
        struct Node tmp;
        memcpy(&tmp, &predicates[0], sizeof(tmp));
        memcpy(
            &predicates[0],
            &predicates[chosen_predicate_index],
            sizeof(tmp)
        );
        memcpy(&predicates[chosen_predicate_index], &tmp, sizeof(tmp));
    }

    // We found at least one predicate on the first table now just count how
    // many are already in place
    if (chosen_predicate_index >= 0) {
        int i = 1;
        while (i < count && predicates[i].children[0].field.table_id == 0) {
            i++;
        }
        return i;
    }

    return 0;
}

/**
 * @brief Check which of the fields in the ORDER BY clause are *actually*
 * required. For example sorts might not be needed if retrieving rows from an
 * index, or if there is an equality constraint on the column.
 * Writes the map of required sorts into the array provided by sorts_needed
 *
 * @param q
 * @param plan
 * @param sorts_needed OUT array listing query order node indices required
 * @return int count of sort steps added, or -1 for a single reverse
 */
static int applySortLogic (
    struct Query *q,
    struct Plan *plan,
    int *sorts_needed
) {
    int non_unique_joins = 0;
    for (int i = 0; i < plan->step_count; i++) {
        enum PlanStepType type = plan->steps[i].type;
        if (
            type == PLAN_LOOP_JOIN
            || type == PLAN_CONSTANT_JOIN
            || type == PLAN_CROSS_JOIN
        ) {
            non_unique_joins++;
        }
    }

    // Now start to iterate the sort columns to
    // decide which sorts are actually needed
    int sorts_added = 0;

    for (int i = 0; i < q->order_count; i++) {
        int sort_needed = 1;

        // If there is an equality predicate on this column then there is no
        // need to ever sort by it.
        for (int j = 0; j < q->predicate_count; j++) {
            struct Node *predicate = &q->predicate_nodes[j];
            struct Node *left = &predicate->children[0];

            if (
                predicate->function == OPERATOR_EQ
                && left->function == q->order_nodes[i].function
                && left->field.table_id == q->order_nodes[i].field.table_id
                && left->field.index == q->order_nodes[i].field.index
            ) {
                // OK we don't need to sort by this field

                sort_needed = 0;
                break;
            }
        }

        if (sort_needed) {
            sorts_needed[sorts_added++] = i;
        }
    }

    // These assumptions work if the rows are from exactly one table, they can
    // however be from additional tables if they are uniquely joined to the
    // first table.
    if (sorts_added > 0 && non_unique_joins == 0) {
        int primary_sort_idx = sorts_needed[0];
        struct Node *primary_sort_col = &q->order_nodes[primary_sort_idx];
        enum Order primary_sort_dir = primary_sort_col->alias[0];

        struct PlanStep *first_step = &plan->steps[0];

        // If we're sorting on rowid or PK on single table then we can optimse
        // Only works if results were retrieved in table order
        if (
            first_step->type == PLAN_TABLE_SCAN
            || first_step->type == PLAN_TABLE_SCAN
        ) {

            if (
                primary_sort_col->function == FUNC_UNITY
                && primary_sort_col->field.index == FIELD_ROW_INDEX
            ) {
                // No need to sort

                if (primary_sort_dir == ORDER_DESC) {
                    // Just need to reverse
                    return -1;
                }

                return 0;
            }
        }

        if (first_step->node_count > 0) {
            struct Node *predicate = &first_step->nodes[0];
            struct Node *left;

            if (predicate->function == FUNC_UNITY) {
                // INDEX SCAN just takes a single node, not a predicate
                left = predicate;
            }
            else if (predicate->child_count == 2) {
                // Looks like a real predicate
                left = &predicate->children[0];
            }
            else {
                // Not a predicate, just bail
                return sorts_added;
            }

            if (
                (
                    first_step->type == PLAN_UNIQUE
                    || first_step->type == PLAN_UNIQUE_RANGE
                )
                && left->field.table_id
                    == primary_sort_col->field.table_id
                && left->field.index
                    == primary_sort_col->field.index
            ) {
                // Rows were returned in INDEX order.
                // The first order column is the index column,
                // and since it is a UNIQUE index there can never be more
                // than one row returned for the first sort column so,
                // we never need to sort.

                if (primary_sort_dir == ORDER_DESC) {
                    // Just need to reverse
                    return -1;
                }

                return 0;
            }

            if (
                (
                    first_step->type == PLAN_INDEX_RANGE
                    || first_step->type == PLAN_INDEX_SCAN
                )
                && sorts_added == 1
                && left->function == primary_sort_col->function
                && left->field.table_id
                    == primary_sort_col->field.table_id
                && left->field.index
                    == primary_sort_col->field.index
            ) {
                // Rows were returned in INDEX order.
                // The first order column is the index column,
                // and since it is only one sort column,
                // we never need to sort.

                if (primary_sort_dir == ORDER_DESC) {
                    // Just need to reverse
                    return -1;
                }

                return 0;
            }
        }
    }

    return sorts_added;
}

static int areNodesEqual (struct Node *nodeA, struct Node *nodeB) {
    return nodeA->function == nodeB->function
        && nodeA->field.table_id
            == nodeB->field.table_id
        && nodeA->field.index
            == nodeB->field.index;
}