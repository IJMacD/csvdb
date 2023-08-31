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

static void checkCoveringIndex (
    struct Query *q,
    struct Plan *plan
);

static int areNodesEqual (struct Node *nodeA, struct Node *nodeB);

static int haveNonUniqueJoins (struct Plan *plan);

static void addPredicateSource (struct Plan *plan, struct Query *query);

static enum PlanStepType findIndexSource (struct Query *query);

static const char *getNodeFieldName (struct Node *node);

int makePlan (struct Query *q, struct Plan *plan) {
    plan->step_count = 0;

    // Do we have any never predicates?
    int predicates_any_never = 0;
    for (int i = 0; i < q->predicate_count; i++) {
        if (q->predicate_nodes[i].function == OPERATOR_NEVER) {
            predicates_any_never = 1;
            break;
        }
    }

    // If there's a single NEVER predicate then we have 0 rows
    if (predicates_any_never) {
        addStepWithNodes(plan, PLAN_SELECT, q->columns, q->column_count);

        return plan->step_count;
    }

    // If there's no table specified then it must be a
    // single-row-all-constant query
    if (q->table_count == 0) {
        addStep(plan, PLAN_DUMMY_ROW);

        addStepWithNodes(plan, PLAN_SELECT, q->columns, q->column_count);

        return plan->step_count;
    }

    // Do we have any relevant predicates?
    int predicates_all_always = 1;
    for (int i = 0; i < q->predicate_count; i++) {
        if (q->predicate_nodes[i].function != OPERATOR_ALWAYS) {
            predicates_all_always = 0;
            break;
        }
    }

    int have_predicates = predicates_all_always == 0;

    int have_group_by = q->group_count > 0;
    int have_grouping = have_group_by || (q->flags & FLAG_GROUP);

    if (have_predicates) {
        addPredicateSource(plan, q);
    }
    else if (
        // We need at least one ORDER BY node,
        q->order_count >= 1
        // it needs to be on a UNITY function,
        && q->order_nodes[0].function == FUNC_UNITY
        // without grouping
        && !have_grouping
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

        // If the first ORDER BY node is a unique index then we can make use of
        // it here if we're sure that there each row on the first table can only
        // result in exactly one row in the output.
        if (index_type == INDEX_UNIQUE && haveNonUniqueJoins(plan) == 0) {
            addStepWithNode(plan, PLAN_UNIQUE_RANGE, &q->order_nodes[0]);
        }
        // Nearly sorted sorts are *really* expensive, if there's more than one
        // ORDER BY node then it's probably not worth it
        else if (index_type != INDEX_NONE && q->order_count == 1) {
            addStepWithNode(plan, PLAN_INDEX_SCAN, &q->order_nodes[0]);
        }
        else {
            addStep(plan, PLAN_TABLE_SCAN);
        }

        addJoinStepsIfRequired(plan, q);

        addOrderStepsIfRequired(plan, q);
    }
    else if (
        have_group_by
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

    checkCoveringIndex(q, plan);

    addStepWithNodes(plan, PLAN_SELECT, q->columns, q->column_count);

    return plan->step_count;
}

static void addPredicateSource (struct Plan *plan, struct Query *query) {
    int have_order_by = query->order_count > 0;
    int have_group_by = query->group_count > 0;
    int have_grouping = have_group_by || (query->flags & FLAG_GROUP);

    // Try to find a predicate on the first table
    int predicatesOnFirstTable = optimiseNodes(
        query,
        query->predicate_nodes,
        query->predicate_count
    );

    // First table
    struct Table *table = &query->tables[0];

    // First predicate
    struct Node *p = &query->predicate_nodes[0];
    enum Function op = p->function;

    struct Node *left = &p->children[0];
    struct Field *field_left = (struct Field *)left;

    if (predicatesOnFirstTable == 0) {
        // None of the predicates are on the first table, we'll need to do full
        // table scan

        addStep(plan, PLAN_TABLE_SCAN);

        addJoinStepsIfRequired(plan, query);

        addStepWithNodes(
            plan,
            PLAN_TABLE_ACCESS_ROWID,
            query->predicate_nodes,
            query->predicate_count
        );

        addOrderStepsIfRequired(plan, query);

        return;
    }

    enum PlanStepType step_type = 0;

    int skip_index = 0;

    // We know that CALENDAR can perform super efficient full table
    // scans with predicates.
    // Currently Index access can only use a single predicate at a
    // time which makes CALENDAR access much slower than it needs to
    // be.
    // In the future with real index ranges this special case could
    // probably be removed.
    if (predicatesOnFirstTable > 1 && table->db->vfs == VFS_CALENDAR) {
        struct Node *p2 = &query->predicate_nodes[1];
        struct Field *p2_field_left = (struct Field *)&p2->children[0];

        if (strcmp(field_left->text, p2_field_left->text) == 0) {
            skip_index = 1;
        }
    }

    if (skip_index == 0) {
        step_type = findIndexSource(query);
    }

    // If plan_type is set, that means we have an index
    if (step_type) {
        addStepWithNode(plan, step_type, &query->predicate_nodes[0]);

        addJoinStepsIfRequired(plan, query);

        if (query->predicate_count > 1) {
            addStepWithNodes(
                plan,
                PLAN_TABLE_ACCESS_ROWID,
                query->predicate_nodes + 1,
                query->predicate_count - 1
            );
        }

        if (step_type == PLAN_PK || step_type == PLAN_UNIQUE) {
            // Reverse is never required if the plan is PLAN_PK or
            // PLAN_UNIQUE.
        } else if (have_grouping){
            // Reverse is not required if we're grouping.
        } else if (have_order_by) {
            // Follow our own logic to add an order step
            // We can avoid it if we're just going to sort on the same
            // column we've just scanned.
            if (
                (query->order_count == 1)
                && query->order_nodes[0].function == FUNC_UNITY
                && strcmp(
                    field_left->text,
                    query->order_nodes[0].field.text
                ) == 0
            ) {

                // So a sort is not necessary but we might still need to
                // reverse
                if (query->order_nodes[0].alias[0] == ORDER_DESC) {
                    addStep(plan, PLAN_REVERSE);
                }
            } else {
                addOrderStepsIfRequired(plan, query);
            }
        }

        return;
    }

    // Before we do a full table scan... we have one more opportunity to
    // use an index to save a sort later, see if we can use an index for
    // ordering now.
    if (
        skip_index == 0
        && query->order_count == 1
        // If we're selecting a lot of rows this optimisation is
        // probably worth it. If we have an EQ operator then it's
        // probably cheaper to filter first.
        //
        // Tested with `WHERE score = 42 ORDER BY name`
        // example times:
        //  Index, then filter:     real    0m3.012s
        //  Filter, then sort:      real    0m1.637s
        && (op != OPERATOR_EQ)
        && query->order_nodes[0].function == FUNC_UNITY
        && findIndex(
            NULL,
            table->name,
            query->order_nodes[0].field.text,
            INDEX_ANY
        )
    ) {
        // Add step for Sorted index access
        addStepWithNode(plan, PLAN_INDEX_SCAN, &query->order_nodes[0]);

        if (predicatesOnFirstTable > 0) {
            addStepWithNodes(
                plan,
                PLAN_TABLE_ACCESS_ROWID,
                query->predicate_nodes,
                predicatesOnFirstTable
            );
        }

        addJoinStepsIfRequired(plan, query);

        // Add step for remaining predicates filter
        if (query->predicate_count > predicatesOnFirstTable) {
            addStepWithNodes(
                plan,
                PLAN_TABLE_ACCESS_ROWID,
                query->predicate_nodes + predicatesOnFirstTable,
                query->predicate_count - predicatesOnFirstTable
            );
        }

        if (
            query->order_nodes[0].alias[0] == ORDER_DESC
            && !have_grouping
        ) {
            addStep(plan, PLAN_REVERSE);
        }

        return;
    }

    if (
        skip_index == 0
        && have_group_by
        && query->group_nodes[0].function == FUNC_UNITY
    ) {
        // Before we do a full table scan... we have one more
        // opportunity to use an index to save a sort later, see if we
        // can use an index for ordering now
        struct Table *table = &query->tables[0];
        if (findIndex(
            NULL,
            table->name,
            query->group_nodes[0].field.text,
            INDEX_ANY
        )) {

            addStepWithNode(plan, PLAN_INDEX_SCAN, &query->group_nodes[0]);

            addStepWithNodes(
                plan,
                PLAN_TABLE_ACCESS_ROWID,
                query->predicate_nodes,
                predicatesOnFirstTable
            );

            addJoinStepsIfRequired(plan, query);

            if (query->predicate_count > predicatesOnFirstTable) {
                // Add the rest of the predicates after the join
                addStepWithNodes(
                    plan,
                    PLAN_TABLE_ACCESS_ROWID,
                    query->predicate_nodes + predicatesOnFirstTable,
                    query->predicate_count - predicatesOnFirstTable
                );
            }

            // In this case it's OK to apply limit optimisation to Join
            // step even though we have an ORDER BY clause.
            struct PlanStep *prev = &plan->steps[plan->step_count - 1];
            if (query->limit_value >= 0) {
                // Usually this can't be done with ORDER BY but in this
                // case we can since there are no predicates.
                prev->limit = query->offset_value + query->limit_value;
            }

        } else {
            addStepWithNodes(
                plan,
                PLAN_TABLE_ACCESS_FULL,
                query->predicate_nodes,
                predicatesOnFirstTable
            );

            addJoinStepsIfRequired(plan, query);

            if (query->predicate_count > predicatesOnFirstTable) {
                // Add the rest of the predicates after the join
                addStepWithNodes(
                    plan,
                    PLAN_TABLE_ACCESS_ROWID,
                    query->predicate_nodes + predicatesOnFirstTable,
                    query->predicate_count - predicatesOnFirstTable
                );
            }
        }

        return;
    }

    /********************
     * TABLE ACCESS FULL
     ********************/
    if (query->table_count > 1) {

        // First predicates are from first table
        addStepWithNodes(
            plan,
            PLAN_TABLE_ACCESS_FULL,
            query->predicate_nodes,
            predicatesOnFirstTable
        );

        // The join
        addJoinStepsIfRequired(plan, query);

        if (query->predicate_count > predicatesOnFirstTable) {
            // Add the rest of the predicates after the join
            addStepWithNodes(
                plan,
                PLAN_TABLE_ACCESS_ROWID,
                query->predicate_nodes + predicatesOnFirstTable,
                query->predicate_count - predicatesOnFirstTable
            );
        }

        addOrderStepsIfRequired(plan, query);
    }
    /* table_count == 1 */
    // Special case if the only predicate is an OR operator node
    else if (
        query->predicate_count == 1 &&
        query->predicate_nodes[0].function == OPERATOR_OR
    )
    {
        struct Node *or_children = query->predicate_nodes[0].children;

        for (int i = 0; i < query->predicate_nodes[0].child_count; i++) {
            // Add one *source* yes SOURCE step per OR child
            addStepWithNode(
                plan,
                PLAN_TABLE_ACCESS_FULL,
                &or_children[i]
            );
        }

        addOrderStepsIfRequired(plan, query);

    }
    /* table_count == 1 */
    else {

        // Only one table so add all predicates together
        addStepWithNodes(
            plan,
            PLAN_TABLE_ACCESS_FULL,
            query->predicate_nodes,
            query->predicate_count
        );

        addOrderStepsIfRequired(plan, query);
    }
}

static enum PlanStepType findIndexSource (struct Query *query) {
    // First predicate
    struct Node *p = &query->predicate_nodes[0];
    enum Function op = p->function;

    struct Node *left = &p->children[0];
    struct Field *field_left = (struct Field *)left;
    struct Field *field_right = (struct Field *)&p->children[1];

    size_t len = strlen(field_right->text);

    if (field_left->index == FIELD_ROW_INDEX) {
        return PLAN_TABLE_SCAN;
    }

    // LIKE can only use index if '%' is at the end
    if (
        op == OPERATOR_LIKE
        && field_right->text[len-1] != '%'
    ) {
        // Unable to use index
        return 0;
    }

    if (left->function == FUNC_PK) {
        if (op == OPERATOR_EQ) {
            return PLAN_PK;
        }

        // Can't use PK index for LIKE yet
        if (op != OPERATOR_LIKE) {
            return PLAN_PK_RANGE;
        }

        return 0;
    }

    // Can only do indexes on bare columns for now
    if (left->function != FUNC_UNITY) {
        return 0;
    }

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
    // First table
    struct Table *table = &query->tables[0];

    // Try to find any index
    enum IndexSearchType find_result = findIndex(
        NULL,
        table->name,
        field_left->text,
        INDEX_ANY
    );

    if (find_result == INDEX_NONE) {
        return 0;
    }

    if (find_result == INDEX_PRIMARY) {
        if (op == OPERATOR_EQ) {
            return PLAN_PK;
        }

        // Can't use PK index for LIKE yet
        if (op != OPERATOR_LIKE) {
            return PLAN_PK_RANGE;
        }

        return 0;
    }

    // LIKE makes any INDEX automatically non-unique
    if (
        find_result == INDEX_UNIQUE
        && op != OPERATOR_LIKE
    ) {
        if (op == OPERATOR_EQ) {
            return PLAN_UNIQUE;
        }

        return PLAN_UNIQUE_RANGE;
    }

    // INDEX RANGE can handle LIKE
    return PLAN_INDEX_RANGE;
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

            struct Node *left_node = &join->children[0];
            struct Node *right_node = &join->children[1];

            struct Field *left_field =
                (
                    left_node->function == FUNC_UNITY
                    || left_node->child_count == -1
                ) ? &left_node->field : &left_node->children[0].field;

            struct Field *right_field =
                (
                    right_node->function == FUNC_UNITY
                    || right_node->child_count == -1
                ) ? &right_node->field : &right_node->children[0].field;

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

                // Reestablish our cached references if needed
                left_field =
                    (
                        left_node->function == FUNC_UNITY
                        || left_node->child_count == -1
                    ) ? &left_node->field : &left_node->children[0].field;

                    right_node = &join->children[1];
                right_field =
                    (
                        right_node->function == FUNC_UNITY
                        || right_node->child_count == -1
                    ) ? &right_node->field : &right_node->children[0].field;
            }

            // Constant must be on right if there is one
            if (isConstantNode(right_node))
            {
                addStepWithNode(plan, PLAN_CONSTANT_JOIN, join);
            }
            // else if (right_field->table_id == i) {
            //     // Both sides of predicate are on same table
            //     // We can do constant join
            //     addStepWithNode(plan, PLAN_CONSTANT_JOIN, join);
            // }
            else {
                enum IndexSearchType index_result = findIndex(
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
    if (sorts_added > 0 && haveNonUniqueJoins(plan) == 0) {
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

/**
 * @brief Is is possible to have more rows than only those on the first table?
 *
 * @param plan
 * @return int
 */
static int haveNonUniqueJoins (struct Plan *plan) {
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
    return non_unique_joins;
}

static void checkCoveringIndex (
    struct Query *q,
    struct Plan *plan
) {
    // TODO
    // BUG: FROM CALENDAR WHERE date = CURRENT_DATE SELECT date
    return;
    // Don't bother with covering index until bug is fixed

    // Don't bother with joins yet
    if (q->table_count > 1) {
        return;
    }

    struct PlanStep *firstStep = &plan->steps[0];

    // Check if we have an index access
    if (
        firstStep->type != PLAN_INDEX_RANGE
        && firstStep->type != PLAN_INDEX_SCAN
        && firstStep->type != PLAN_UNIQUE
        && firstStep->type != PLAN_UNIQUE_RANGE
    ) {
        return;
    }

    // If we need to access the table anyway then there's no point in using a
    // covering index
    for (int i = 1; i < plan->step_count; i++) {
        if (plan->steps[i].type == PLAN_TABLE_ACCESS_ROWID) {
            return;
        }
    }

    const char *col_name = getNodeFieldName(&firstStep->nodes[0]);

    struct Table *firstTable = &q->tables[0];

    struct DB *db = malloc(sizeof(*db));

    if (
        findIndex(db, firstTable->name, col_name, INDEX_ANY)
            == INDEX_NONE
    ) {
        fprintf(
            stderr,
            "We were told there's an index but we can't find it now.\n"
        );
        free(db);
        return;
    }

    int *mapped_cols = malloc(sizeof(*mapped_cols) * q->column_count);

    // Every column in the SELECT clause must be present in the index
    for (int i = 0; i < q->column_count; i++) {
        const char *col_name = getNodeFieldName(&q->columns[i]);

        mapped_cols[i] = getFieldIndex(db, col_name);

        if (mapped_cols[i] < 0) {
            free(db);
            free(mapped_cols);
            return;
        }
    }

    // Every column in the ORDER BY clause must be present in the index
    for (int i = 0; i < q->order_count; i++) {
        const char *col_name = getNodeFieldName(&q->order_nodes[i]);

        int result = getFieldIndex(db, col_name);

        if (result < 0) {
            free(db);
            free(mapped_cols);
            return;
        }
    }

    // Every column in the GROUP BY clause must be present in the index
    for (int i = 0; i < q->group_count; i++) {
        const char *col_name = getNodeFieldName(&q->group_nodes[i]);

        int result = getFieldIndex(db, col_name);

        if (result < 0) {
            free(db);
            free(mapped_cols);
            return;
        }
    }

    // Success!

    // If we were going to do an INDEX SCAN then it can simply be swapped for a
    // TABLE SCAN
    if (firstStep->type == PLAN_INDEX_SCAN) {
        firstStep->type = PLAN_TABLE_SCAN;
        firstStep->node_count = 0;

        // This is mainly for EXPLAIN
        char table_name[MAX_TABLE_LENGTH];
        sprintf(table_name, "%s__%s", q->tables[0].name, col_name);
        strcpy(q->tables[0].name, table_name);
        strcpy(q->tables[0].alias, table_name);
    }
    else {
        firstStep->type = PLAN_COVERING_INDEX_SEEK;
    }

    q->tables[0].db = db;


    // Replace the SELECT nodes with references to the index
    for (int i = 0; i < q->column_count; i++) {
        q->columns[i].field.index = mapped_cols[i];
    }

    // Replace the ORDER BY nodes with references to the index
    for (int i = 0; i < plan->step_count; i++) {
        struct PlanStep *step = &plan->steps[i];
        if (step->type == PLAN_SORT) {
            for (int j = 0; j < step->node_count; j++) {
                const char *field_name = getNodeFieldName(&step->nodes[j]);
                int field_index = getFieldIndex(db, field_name);
                if (step->nodes[j].child_count <= 0) {
                    step->nodes[j].field.index = field_index;
                }
                else {
                    step->nodes[j].children[0].field.index = field_index;
                }
            }
        }
    }

    // Replace the GROUP BY nodes with references to the index
    for (int i = 0; i < plan->step_count; i++) {
        struct PlanStep *step = &plan->steps[i];
        if (step->type == PLAN_GROUP) {
            for (int j = 0; j < step->node_count; j++) {
                const char *field_name = getNodeFieldName(&step->nodes[j]);
                int field_index = getFieldIndex(db, field_name);
                if (step->nodes[j].child_count <= 0) {
                    step->nodes[j].field.index = field_index;
                }
                else {
                    step->nodes[j].children[0].field.index = field_index;
                }
            }
        }
    }

    free(mapped_cols);
}

static const char *getNodeFieldName (struct Node *node) {
    if ((node->function & MASK_FUNC_FAMILY) == FUNC_FAM_OPERATOR) {
        return node->children[0].field.text;
    }

    return node->field.text;
}