#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "./predicates.h"
#include "./evaluate.h"
#include "../structs.h"
#include "../query/result.h"
#include "../query/select.h"
#include "../db/db.h"
#include "../functions/date.h"
#include "../functions/util.h"

/**
 * Evaluate <left> <compare> ANY|ALL (subquery) by materialising the subquery
 * and testing the comparison against each first-column value.
 * ANY: true if any row matches; empty → false.
 * ALL: true if every row matches; empty → true.
 */
static int evaluateQuantifiedOperator(
    struct Table *tables,
    RowListIndex list_id,
    int row_index,
    struct Node *node)
{
    if (node->child_count != 2) {
        fprintf(stderr, "Quantified operator expects two children\n");
        exit(-1);
    }

    enum Function compare = quantifiedCompare(node);
    if ((compare & MASK_FUNC_FAMILY) != FUNC_FAM_OPERATOR ||
        compare == OPERATOR_OR ||
        compare == OPERATOR_AND ||
        compare == OPERATOR_ANY ||
        compare == OPERATOR_ALL ||
        compare == OPERATOR_ALWAYS ||
        compare == OPERATOR_NEVER)
    {
        fprintf(stderr, "Invalid comparison on quantified operator\n");
        exit(-1);
    }

    char value_left[MAX_VALUE_LENGTH] = {0};
    int result = evaluateNode(
        tables,
        list_id,
        row_index,
        &node->children[0],
        value_left,
        MAX_VALUE_LENGTH);
    if (result < 0) {
        fprintf(stderr, "Unable to evaluate quantified left side\n");
        exit(-1);
    }

    struct Node *right = &node->children[1];
    if (right->function != FUNC_SUBQUERY || right->subquery == NULL) {
        fprintf(stderr, "Quantified operator RHS must be a subquery\n");
        exit(-1);
    }

    int outer_table_count = 0;
    if (list_id >= 0) {
        struct RowList *rl = getRowList(list_id);
        if (rl != NULL) {
            outer_table_count = (int)rl->join_count;
        }
    }

    struct DB db = {0};
    result = evaluate_subquery_to_db(
        right->subquery,
        tables,
        outer_table_count,
        list_id,
        row_index,
        list_id >= 0,
        &db);
    if (result < 0) {
        fprintf(stderr, "Unable to evaluate quantified subquery\n");
        exit(-1);
    }

    int rows = getRecordCount(&db);
    int is_any = (node->function == OPERATOR_ANY);
    int matched = is_any ? 0 : 1;
    int saw_row = 0;

    if (rows == 0 || db.field_count <= 0) {
        closeDB(&db);
        return is_any ? 0 : 1;
    }

    for (int i = 0; i < rows; i++) {
        char value_right[MAX_VALUE_LENGTH] = {0};
        if (getRecordValue(&db, i, 0, value_right, MAX_VALUE_LENGTH) < 0) {
            // Skip unreadable rows (header-only phantom); treat as absent.
            continue;
        }
        saw_row = 1;

        int cmp = evaluateExpression(compare, value_left, value_right);
        if (is_any) {
            if (cmp) {
                matched = 1;
                break;
            }
        } else if (!cmp) {
            matched = 0;
            break;
        }
    }

    closeDB(&db);
    if (!saw_row) {
        return is_any ? 0 : 1;
    }
    return matched;
}

/**
 * Evaluate an OPERATOR node
 * @return 1 if evaluates to true; 0 if evaluates to false
 */
int evaluateOperatorNode (struct Table *tables, RowListIndex list_id, int row_index, struct Node *node) {
    if ((node->function & MASK_FUNC_FAMILY) != FUNC_FAM_OPERATOR) {
        fprintf(
            stderr,
            "Node must be an operator. Found function 0x%X\n",
            node->function
        );
        exit(-1);
    }

    if (node->function == OPERATOR_NEVER) {
        return 0;
    }

    if (node->function == OPERATOR_ALWAYS) {
        return 1;
    }

    if (node->function == OPERATOR_OR) {
        return evaluateOperatorNodeListOR(tables, list_id, row_index, node->children, node->child_count);
    }

    if (node->function == OPERATOR_AND) {
        return evaluateOperatorNodeListAND(tables, list_id, row_index, node->children, node->child_count);
    }

    if (node->function == OPERATOR_ANY || node->function == OPERATOR_ALL) {
        return evaluateQuantifiedOperator(tables, list_id, row_index, node);
    }

    char value_left[MAX_VALUE_LENGTH] = {0};
    char value_right[MAX_VALUE_LENGTH] = {0};

    int result = evaluateNode(
        tables,
        list_id,
        row_index,
        &node->children[0],
        value_left,
        MAX_VALUE_LENGTH
    );
    if (result < 0) {
        fprintf(stderr, "Unable to evaluate node\n");
        exit(-1);
    }

    result = evaluateNode(
        tables,
        list_id,
        row_index,
        &node->children[1],
        value_right,
        MAX_VALUE_LENGTH
    );
    if (result < 0) {
        fprintf(stderr, "Unable to evaluate node\n");
        exit(-1);
    }

    if (!evaluateExpression(node->function, value_left, value_right)) {
        return 0;
    }

    return 1;
}

/**
 * Evaluate a list of OPERATOR nodes
 * returns 1 if ALL of the nodes match
 */
int evaluateOperatorNodeListAND (struct Table *tables, int row_list, int row_index, struct Node *nodes, int node_count) {
    for (int j = 0; j < node_count; j++) {
        struct Node * p = nodes + j;

        if (!evaluateOperatorNode(tables, row_list, row_index, p)) {
            return 0;
        }
    }

    return 1;
}

/**
 * Evaluate a list of OPERATOR nodes
 * returns 1 if ANY of the nodes match
 */
int evaluateOperatorNodeListOR (struct Table *tables, int row_list, int row_index, struct Node *nodes, int node_count) {
    for (int j = 0; j < node_count; j++) {
        struct Node * p = nodes + j;

        if (evaluateOperatorNode(tables, row_list, row_index, p)) {
            return 1;
        }
    }

    return 0;
}

int evaluateExpression (enum Function op, const char *left, const char *right) {
    // printf("Evaluating %s OP %s\n", left, right);

    if (op == OPERATOR_NEVER) {
        return 0;
    }

    if (op == OPERATOR_ALWAYS) {
        return 1;
    }

    struct DateTime dt_left, dt_right;

    if (parseDateTime(left, &dt_left) && parseDateTime(right, &dt_right))
    {
        // Date comparison
        int unix_left = datetimeGetUnix(&dt_left);
        int unix_right = datetimeGetUnix(&dt_right);

        if (op == OPERATOR_EQ)
            return unix_left == unix_right;
        if (op == OPERATOR_NE)
            return unix_left != unix_right;
        if (op == OPERATOR_LT)
            return unix_left < unix_right;
        if (op == OPERATOR_LE)
            return unix_left <= unix_right;
        if (op == OPERATOR_GT)
            return unix_left > unix_right;
        if (op == OPERATOR_GE)
            return unix_left >= unix_right;

        return 0;
    }

    if (parseDate(left, &dt_left) && parseDate(right, &dt_right))
    {
        // Date comparison
        int julian_left = datetimeGetJulian(&dt_left);
        int julian_right = datetimeGetJulian(&dt_right);

        if (op == OPERATOR_EQ) return julian_left == julian_right;
        if (op == OPERATOR_NE) return julian_left != julian_right;
        if (op == OPERATOR_LT) return julian_left < julian_right;
        if (op == OPERATOR_LE) return julian_left <= julian_right;
        if (op == OPERATOR_GT) return julian_left > julian_right;
        if (op == OPERATOR_GE) return julian_left >= julian_right;

        return 0;
    }

    if (op == OPERATOR_LIKE) {
        size_t len = strlen(right);
        if (right[len-1] == '%') {
            return strncmp(left, right, len -1) == 0;
        }

        return strcmp(left, right) == 0;
    }

    size_t l_len = strlen(left);
    size_t r_len = strlen(right);

    if (strcmp(right, "NULL") == 0) {

        if (op == OPERATOR_EQ) return l_len == 0;
        if (op == OPERATOR_NE) return l_len != 0;

        return 0;
    }

    if (strcmp(left, "NULL") == 0) {

        if (op == OPERATOR_EQ) return r_len == 0;
        if (op == OPERATOR_NE) return r_len != 0;

        return 0;
    }

    // NULL values do not evaluate true with any other operator
    if (l_len == 0 || r_len == 0) {
        return 0;
    }

    // Complex: component EQ/NE only; ordered compares are undefined → false
    if (is_complex(left) || is_complex(right)) {
        double re_l, im_l, re_r, im_r;
        if (!parse_complex(left, &re_l, &im_l) ||
            !parse_complex(right, &re_r, &im_r))
        {
            // Fall through to string compare if either side is not parseable
        }
        else
        {
            int eq = (re_l == re_r && im_l == im_r);
            if (op == OPERATOR_EQ) return eq;
            if (op == OPERATOR_NE) return !eq;
            return 0;
        }
    }

    if (is_numeric(left)) {
        int cmp = compare_numeric(left, right);

        if (op == OPERATOR_EQ) return cmp == 0;
        if (op == OPERATOR_NE) return cmp != 0;
        if (op == OPERATOR_LT) return cmp < 0;
        if (op == OPERATOR_LE) return cmp <= 0;
        if (op == OPERATOR_GT) return cmp > 0;
        if (op == OPERATOR_GE) return cmp >= 0;
    }

    if (op == OPERATOR_EQ) return strcmp(left, right) == 0;
    if (op == OPERATOR_NE) return strcmp(left, right) != 0;
    if (op == OPERATOR_LT) return strcmp(left, right) < 0;
    if (op == OPERATOR_LE) return strcmp(left, right) <= 0;
    if (op == OPERATOR_GT) return strcmp(left, right) > 0;
    if (op == OPERATOR_GE) return strcmp(left, right) >= 0;

    fprintf(stderr, "Unrecognised operator: %d\n", op);
    exit(-1);
}

/**
 * @brief Will ensure field is on left and constant is on right
 *
 * @param p
 */
void normalisePredicate (struct Node *predicate) {
    if (predicate->function == OPERATOR_ANY ||
        predicate->function == OPERATOR_ALL ||
        predicate->function == OPERATOR_OR ||
        predicate->function == OPERATOR_AND)
    {
        return;
    }

    struct Node *left = &predicate->children[0];
    struct Node *right = &predicate->children[1];

    if (isConstantNode(left) && !isConstantNode(right)) {
        flipPredicate(predicate);
        return;
    }

    if (left->function != FUNC_PK && right->function == FUNC_PK) {
        flipPredicate(predicate);
        return;
    }
}

/**
 * Flips left and right operands of a predicate, adjusting operator as necessary
 * @returns 0 for success, -1 for error
 */
int flipPredicate (struct Node *predicate) {
    enum Function op = predicate->function;

    if (op == OPERATOR_ANY ||
        op == OPERATOR_ALL ||
        op == OPERATOR_OR ||
        op == OPERATOR_AND ||
        op == OPERATOR_LIKE)
    {
        return -1;
    }

    struct Node *left = &predicate->children[0];
    struct Node *right = &predicate->children[1];

    // copy struct automatically
    struct Node tmp = *left;

    // swap
    memcpy(left, right, sizeof(tmp));
    memcpy(right, &tmp, sizeof(tmp));

    // flip operator as necessary
    if (op == OPERATOR_LT) {
        op = OPERATOR_GT;
    } else if (op == OPERATOR_LE) {
        op = OPERATOR_GE;
    } else if (op == OPERATOR_GT) {
        op = OPERATOR_LT;
    } else if (op == OPERATOR_GE) {
        op = OPERATOR_LE;
    } else if (op == OPERATOR_EQ) {
        // no op
    } else if (op == OPERATOR_NE) {
        // no op
    } else {
        // Unable to swap — restore
        memcpy(right, left, sizeof(tmp));
        memcpy(left, &tmp, sizeof(tmp));
        return -1;
    }

    predicate->function = op;

    return 0;
}

/**
 * Checks if there are any predicates on this query. Ignores ALWAYS operators
 * @returns 1 if there are predicates, 0 if not
 */
int havePredicates (struct Query *query) {
    // Do we have any relevant predicates?
    int predicates_all_always = 1;
    for (int i = 0; i < query->predicate_count; i++) {
        if (query->predicate_nodes[i].function != OPERATOR_ALWAYS) {
            predicates_all_always = 0;
            break;
        }
    }

    return predicates_all_always == 0;
}