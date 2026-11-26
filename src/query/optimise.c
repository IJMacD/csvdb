#include <stdlib.h>
#include <string.h>

#include "./select.h"
#include "./node.h"
#include "../structs.h"
#include "../evaluate/evaluate.h"
#include "../evaluate/predicates.h"
#include "../functions/util.h"
#include "../debug.h"
#include "../fatal.h"

/**
 * Free child at index and compact the children array.
 */
static void removeChildAt(struct Node *node, int index)
{
    freeNode(&node->children[index]);

    if (index < node->child_count - 1)
    {
        memmove(
            &node->children[index],
            &node->children[index + 1],
            sizeof(node->children[0]) * (node->child_count - index - 1));
    }

    node->child_count--;
    // Last slot is a duplicate after memmove; drop without freeing.
    clearNode(&node->children[node->child_count]);
}

/**
 * Replace an AND/OR node that has exactly one child with that child.
 */
static void promoteSingleChild(struct Node *node)
{
    struct Node child = node->children[0];
    free(node->children);
    *node = child;
}

void optimiseCollapseConstantNode(struct Node *node)
{
    if (node->function == FUNC_UNITY)
    {
        return;
    }

    if (
        node->function == FUNC_INDEX ||
        node->function == FUNC_UNIQUE ||
        node->function == FUNC_PK)
    {
        return;
    }

    if (node->function == FUNC_RANDOM)
    {
        // RANDOM is non-deterministic so cannot be collapsed
        return;
    }

    if (node->function == FUNC_SUBQUERY)
    {
        // May be correlated; evaluated at runtime
        return;
    }

    if ((node->function & MASK_FUNC_FAMILY) == FUNC_FAM_AGG)
    {
        // Aggregates need a row list (FILTER / grouping); fold children only.
        for (int i = 0; i < node->child_count; i++)
        {
            optimiseCollapseConstantNode(&node->children[i]);
        }
        return;
    }

    // Optimise all children first
    for (int i = 0; i < node->child_count; i++)
    {
        optimiseCollapseConstantNode(&node->children[i]);
    }

    // If this is a self-child node, check if the field is constant
    if (node->child_count == -1 && node->field.index != FIELD_CONSTANT)
    {
        return;
    }

    if (node->function == OPERATOR_ALWAYS || node->function == OPERATOR_NEVER)
    {
        return;
    }

    if (node->function == OPERATOR_AND)
    {
        // NEVER annihilates AND
        for (int i = 0; i < node->child_count; i++)
        {
            if (node->children[i].function == OPERATOR_NEVER)
            {
                if (debug_verbosity >= 2)
                {
                    fprintf(stderr, "[OPTIMISE] Constant Collapse (AND)\n");
                }
                node->function = OPERATOR_NEVER;
                return;
            }
        }

        // Drop tautology children (ALWAYS)
        for (int i = 0; i < node->child_count;)
        {
            if (node->children[i].function == OPERATOR_ALWAYS)
            {
                if (debug_verbosity >= 2)
                {
                    fprintf(stderr, "[OPTIMISE] Constant Collapse (AND drop ALWAYS)\n");
                }
                removeChildAt(node, i);
                continue;
            }
            i++;
        }

        if (node->child_count == 0)
        {
            if (debug_verbosity >= 2)
            {
                fprintf(stderr, "[OPTIMISE] Constant Collapse (AND)\n");
            }
            node->function = OPERATOR_ALWAYS;
            return;
        }

        if (node->child_count == 1)
        {
            promoteSingleChild(node);
            // Re-collapse the promoted node (may itself be constant)
            optimiseCollapseConstantNode(node);
            return;
        }
    }

    if (node->function == OPERATOR_OR)
    {
        // ALWAYS annihilates OR
        for (int i = 0; i < node->child_count; i++)
        {
            if (node->children[i].function == OPERATOR_ALWAYS)
            {
                if (debug_verbosity >= 2)
                {
                    fprintf(stderr, "[OPTIMISE] Constant Collapse (OR)\n");
                }
                node->function = OPERATOR_ALWAYS;
                return;
            }
        }

        // Drop contradiction children (NEVER)
        for (int i = 0; i < node->child_count;)
        {
            if (node->children[i].function == OPERATOR_NEVER)
            {
                if (debug_verbosity >= 2)
                {
                    fprintf(stderr, "[OPTIMISE] Constant Collapse (OR drop NEVER)\n");
                }
                removeChildAt(node, i);
                continue;
            }
            i++;
        }

        if (node->child_count == 0)
        {
            if (debug_verbosity >= 2)
            {
                fprintf(stderr, "[OPTIMISE] Constant Collapse (OR)\n");
            }
            node->function = OPERATOR_NEVER;
            return;
        }

        if (node->child_count == 1)
        {
            promoteSingleChild(node);
            optimiseCollapseConstantNode(node);
            return;
        }
    }

    if (node->function == FUNC_TO_HEX)
    {
        // We don't want to re-parse a hex number back into an int later on
        return;
    }

    // Make sure all children are constant
    for (int i = 0; i < node->child_count; i++)
    {
        if (node->children[i].function != FUNC_UNITY ||
            node->children[i].field.index != FIELD_CONSTANT)
        {
            return;
        }
    }

    if ((node->function & MASK_FUNC_FAMILY) == FUNC_FAM_OPERATOR)
    {
        // Quantified / boolean-combo operators are not binary value compares.
        if (node->function == OPERATOR_ANY ||
            node->function == OPERATOR_ALL ||
            node->function == OPERATOR_OR ||
            node->function == OPERATOR_AND)
        {
            return;
        }

        // If we're evaluating an operator then we can definitively set the node
        // to OPERATOR_ALWAYS or OPERATOR_NEVER

        if (debug_verbosity >= 2)
        {
            fprintf(stderr, "[OPTIMISE] Constant Collapse (Operator)\n");
        }

        // TODO: What about cases with any child count other than 2?

        char value_left[MAX_VALUE_LENGTH] = {0};
        char value_right[MAX_VALUE_LENGTH] = {0};

        evaluateConstantNode(&node->children[0], value_left);
        evaluateConstantNode(&node->children[1], value_right);

        int result = evaluateExpression(node->function, value_left, value_right);

        if (result)
        {
            node->function = OPERATOR_ALWAYS;
        }
        else
        {
            node->function = OPERATOR_NEVER;
        }
    }
    else
    {
        if (debug_verbosity >= 2)
        {
            fprintf(stderr, "[OPTIMISE] Constant Collapse\n");
        }

        // Evaluate the function and write result to field
        int result = evaluateConstantNode(node, node->field.text);
        if (result < 0)
        {
            // (There might be an alias to help identify)
            fprintf(stderr, "Unable to evaluate constant node %s\n", node->alias);
            csvdb_fatal(-1);
        }

        // Mark the node as constant and remove function marker
        node->field.index = FIELD_CONSTANT;
        node->function = FUNC_UNITY;
    }
}

/**
 * If given node is an operator node which includes a rowid column on one side
 * then try to do some algebra if necessary to get rowid on its own.
 */
void optimiseRowidAlgebra(struct Node *node)
{
    int isOperator = (node->function & MASK_FUNC_FAMILY) == FUNC_FAM_OPERATOR;

    if (node->function == OPERATOR_ANY ||
        node->function == OPERATOR_ALL ||
        node->function == OPERATOR_OR ||
        node->function == OPERATOR_AND)
    {
        return;
    }

    if (isOperator && node->child_count == 2)
    {
        struct Node *left_child = &node->children[0];
        struct Node *right_child = &node->children[1];

        int isMathsOperator = (left_child->function & (MASK_FUNC_FAMILY | 0x10)) == 0x10;

        if (isMathsOperator && left_child->child_count == 2)
        {
            struct Node *left_grandchild = &left_child->children[0];
            struct Node *right_grandchild = &left_child->children[1];

            if (left_grandchild->field.index != FIELD_ROW_INDEX)
            {
                return;
            }

            // Constant factor on the right of the maths op
            if (right_grandchild->field.index != FIELD_CONSTANT)
            {
                return;
            }

            if (right_child->field.index != FIELD_CONSTANT)
            {
                return;
            }

            long factor = atol(right_grandchild->field.text);
            long constant = atol(right_child->field.text);

            if (factor == 0)
            {
                return;
            }

            if (debug_verbosity >= 2)
            {
                fprintf(stderr, "[OPTIMISE] RowID algebra\n");
            }

            // rowid + K = N  /  rowid - K = N
            if (left_child->function == FUNC_ADD ||
                left_child->function == FUNC_SUB)
            {
                struct Node tmp = *left_child;

                if (left_child->function == FUNC_ADD)
                {
                    tmp.function = FUNC_SUB;
                }
                else
                {
                    tmp.function = FUNC_ADD;
                }

                memcpy(&node->children[0], left_grandchild, sizeof tmp);
                memcpy(&tmp.children[0], right_child, sizeof tmp);
                memcpy(&node->children[1], &tmp, sizeof tmp);
                return;
            }

            // rowid * K = N  →  rowid = N/K if divisible, else NEVER
            if (left_child->function == FUNC_MUL)
            {
                if (node->function != OPERATOR_EQ)
                {
                    return;
                }
                if (constant % factor != 0)
                {
                    node->function = OPERATOR_NEVER;
                    return;
                }
                char buf[32];
                snprintf(buf, sizeof(buf), "%ld", constant / factor);
                memcpy(&node->children[0], left_grandchild, sizeof(*left_grandchild));
                strcpy(node->children[1].field.text, buf);
                node->children[1].field.index = FIELD_CONSTANT;
                node->children[1].function = FUNC_UNITY;
                node->children[1].child_count = 0;
                node->children[1].children = NULL;
                return;
            }

            // rowid / K = N  →  rowid = N*K
            // (Float division: only the exact multiple satisfies equality.
            //  Integer-range expansion [N*K, (N+1)*K) would be wrong now.)
            if (left_child->function == FUNC_DIV)
            {
                if (node->function != OPERATOR_EQ || factor == 0)
                {
                    return;
                }
                char buf[32];
                snprintf(buf, sizeof(buf), "%ld", constant * factor);
                memcpy(
                    &node->children[0],
                    left_grandchild,
                    sizeof(*left_grandchild));
                strcpy(node->children[1].field.text, buf);
                node->children[1].field.index = FIELD_CONSTANT;
                node->children[1].function = FUNC_UNITY;
                node->children[1].child_count = 0;
                node->children[1].children = NULL;
                return;
            }
        }
    }
}

void optimiseFlattenANDPredicates(struct Query *query)
{
    // Expand nested ANDs: each pass lifts one level into the top-level list.
    for (int pass = 0; pass < 32; pass++)
    {
        int extra = 0;
        for (int i = 0; i < query->predicate_count; i++)
        {
            if (query->predicate_nodes[i].function == OPERATOR_AND)
            {
                extra += query->predicate_nodes[i].child_count - 1;
            }
        }

        if (extra == 0)
        {
            return;
        }

        if (debug_verbosity >= 2)
        {
            fprintf(stderr, "[OPTIMISE] Flatten AND predicates (%d)\n", extra);
        }

        int new_predicate_count = query->predicate_count + extra;
        struct Node *new_predicates = malloc(
            new_predicate_count * sizeof *new_predicates);
        struct Node *curr_predicate = new_predicates;

        for (int i = 0; i < query->predicate_count; i++)
        {
            struct Node *predicate = &query->predicate_nodes[i];

            if (predicate->function == OPERATOR_AND)
            {
                for (int j = 0; j < predicate->child_count; j++)
                {
                    memcpy(
                        curr_predicate++,
                        &predicate->children[j],
                        sizeof *predicate);
                }
                // Children were moved by value; free only the array shell.
                free(predicate->children);
            }
            else
            {
                memcpy(curr_predicate++, predicate, sizeof *predicate);
            }
        }

        free(query->predicate_nodes);
        query->predicate_nodes = new_predicates;
        query->predicate_count = new_predicate_count;
    }
}

void optimiseWhereToOn(struct Query *query)
{
    // Nothing to do for single table queries
    if (query->table_count <= 1)
    {
        return;
    }

    for (int i = 0; i < query->predicate_count; i++)
    {
        struct Node *predicate = &query->predicate_nodes[i];

        if (predicate->function == OPERATOR_ALWAYS ||
            predicate->function == OPERATOR_NEVER)
        {
            continue;
        }

        // AND/OR bags are not binary comparisons; leave them in WHERE.
        if (
            predicate->function == OPERATOR_OR ||
            predicate->function == OPERATOR_AND)
        {
            continue;
        }

        if (predicate->child_count < 2)
        {
            continue;
        }

        struct Node *left_node = &predicate->children[0];
        struct Node *right_node = &predicate->children[1];

        // Scalar subqueries are residual filters (may be correlated).
        if (
            left_node->function == FUNC_SUBQUERY ||
            right_node->function == FUNC_SUBQUERY)
        {
            continue;
        }

        int bit_map = getTableBitMap(left_node);
        int left_table_id = whichBit(bit_map);

        int is_right_constant = getTableBitMap(right_node) == 0;

        if (left_table_id > 0 && is_right_constant)
        {
            // Success: Expression on a single table!

            if (debug_verbosity >= 2)
            {
                fprintf(stderr, "[OPTIMISE] WHERE to ON (Predicate #%d)\n", i);
            }

            struct Node *join_node = &query->tables[left_table_id].join;
            if (join_node->function == OPERATOR_ALWAYS)
            {
                // We can just overwrite
                copyNodeTree(join_node, predicate);
            }
            else if (join_node->function == OPERATOR_AND)
            {
                // Add a child and copy
                struct Node *child = addChildNode(join_node);
                copyNodeTree(child, predicate);
            }
            else
            {
                // Need to convert node
                cloneNodeIntoChild(join_node);
                join_node->function = OPERATOR_AND;
                // Add another child and copy
                struct Node *next_child = addChildNode(join_node);
                copyNodeTree(next_child, predicate);
            }

            // Predicate can now be wiped out
            predicate->function = OPERATOR_ALWAYS;
        }
    }
}

/**
 * Any predicates in the join node which only depend upon earlier tables can be
 * moved to the WHERE clause.
 * (Might get re-moved into an earlier join node by optimiseWhereToOn
 * afterwards.)
 */
void optimiseOnToWhere(
    int table_id,
    struct Node *joinNode,
    struct Query *query)
{
    if (joinNode->function == OPERATOR_AND)
    {
        for (int i = 0; i < joinNode->child_count; i++)
        {
            struct Node *child = &joinNode->children[i];
            optimiseOnToWhere(table_id, child, query);
        }

        return;
    }

    int table_bit_map = getTableBitMap(joinNode);
    if (table_bit_map < (1 << table_id))
    {
        // We can optimise

        if (debug_verbosity >= 2)
        {
            fprintf(stderr, "[OPTIMISE] WHERE to ON (Table %d)\n", table_id);
        }

        // Copy to predicate list
        struct Node *target = allocatePredicateNode(query);
        copyNodeTree(target, joinNode);

        // Mark this node as satisfied
        joinNode->function = OPERATOR_ALWAYS;
    }
}

void optimiseUniqueOr(struct Node *node)
{
    if (node->function != OPERATOR_OR)
    {
        return;
    }

    for (int i = 1; i < node->child_count; i++)
    {
        for (int j = 0; j < i; j++)
        {
            if (areNodesEqual(&node->children[i], &node->children[j]))
            {
                if (debug_verbosity >= 2)
                {
                    fprintf(
                        stderr,
                        "[OPTIMISE] Unique OR: remove duplicate child %d (matches %d)\n",
                        i,
                        j);
                }

                removeChildAt(node, i);
                i--;
                break;
            }
        }
    }

    if (node->child_count == 0)
    {
        node->function = OPERATOR_NEVER;
        return;
    }

    if (node->child_count == 1)
    {
        promoteSingleChild(node);
    }
}

void optimiseRemoveTrivialPredicates(struct Query *query)
{
    int write = 0;

    for (int i = 0; i < query->predicate_count; i++)
    {
        if (query->predicate_nodes[i].function == OPERATOR_ALWAYS)
        {
            if (debug_verbosity >= 2)
            {
                fprintf(stderr, "[OPTIMISE] Remove trivial ALWAYS predicate #%d\n", i);
            }
            freeNode(&query->predicate_nodes[i]);
            continue;
        }

        if (write != i)
        {
            query->predicate_nodes[write] = query->predicate_nodes[i];
        }
        write++;
    }

    for (int i = write; i < query->predicate_count; i++)
    {
        clearNode(&query->predicate_nodes[i]);
    }

    query->predicate_count = write;
}


static int sameColumn(struct Node *a, struct Node *b)
{
    return a->function == b->function &&
           a->field.table_id == b->field.table_id &&
           a->field.index == b->field.index &&
           a->field.index != FIELD_UNKNOWN;
}

static int isConstField(struct Node *n)
{
    return n->function == FUNC_UNITY && n->field.index == FIELD_CONSTANT;
}

/**
 * Tighten or eliminate redundant same-column comparisons against constants.
 */
void optimiseFoldRedundantPredicates(struct Query *query)
{
    for (int i = 0; i < query->predicate_count; i++)
    {
        struct Node *a = &query->predicate_nodes[i];
        if (a->function == OPERATOR_ALWAYS || a->function == OPERATOR_NEVER)
            continue;
        if (a->child_count < 2 || !isConstField(&a->children[1]))
            continue;

        for (int j = i + 1; j < query->predicate_count; j++)
        {
            struct Node *b = &query->predicate_nodes[j];
            if (b->function == OPERATOR_ALWAYS || b->function == OPERATOR_NEVER)
                continue;
            if (b->child_count < 2 || !isConstField(&b->children[1]))
                continue;
            if (!sameColumn(&a->children[0], &b->children[0]))
                continue;

            long va = atol(a->children[1].field.text);
            long vb = atol(b->children[1].field.text);

            // Two equalities
            if (a->function == OPERATOR_EQ && b->function == OPERATOR_EQ)
            {
                if (va != vb)
                {
                    a->function = OPERATOR_NEVER;
                    b->function = OPERATOR_ALWAYS;
                }
                else
                {
                    b->function = OPERATOR_ALWAYS;
                }
                continue;
            }

            // EQ vs range: keep EQ if it satisfies, else NEVER
            if (a->function == OPERATOR_EQ || b->function == OPERATOR_EQ)
            {
                struct Node *eq = a->function == OPERATOR_EQ ? a : b;
                struct Node *rg = a->function == OPERATOR_EQ ? b : a;
                long ve = atol(eq->children[1].field.text);
                long vr = atol(rg->children[1].field.text);
                int ok = 1;
                if (rg->function == OPERATOR_LT) ok = ve < vr;
                else if (rg->function == OPERATOR_LE) ok = ve <= vr;
                else if (rg->function == OPERATOR_GT) ok = ve > vr;
                else if (rg->function == OPERATOR_GE) ok = ve >= vr;
                else ok = 1;
                if (!ok)
                {
                    eq->function = OPERATOR_NEVER;
                }
                rg->function = OPERATOR_ALWAYS;
                continue;
            }

            // Two lower bounds → keep tighter
            if ((a->function == OPERATOR_GE || a->function == OPERATOR_GT) &&
                (b->function == OPERATOR_GE || b->function == OPERATOR_GT))
            {
                // Compare effective thresholds (GT t ~= GE t+1 for ints)
                long ea = (a->function == OPERATOR_GT) ? va + 1 : va;
                long eb = (b->function == OPERATOR_GT) ? vb + 1 : vb;
                if (ea >= eb)
                    b->function = OPERATOR_ALWAYS;
                else
                    a->function = OPERATOR_ALWAYS;
                continue;
            }

            // Two upper bounds → keep tighter
            if ((a->function == OPERATOR_LE || a->function == OPERATOR_LT) &&
                (b->function == OPERATOR_LE || b->function == OPERATOR_LT))
            {
                long ea = (a->function == OPERATOR_LT) ? va - 1 : va;
                long eb = (b->function == OPERATOR_LT) ? vb - 1 : vb;
                if (ea <= eb)
                    b->function = OPERATOR_ALWAYS;
                else
                    a->function = OPERATOR_ALWAYS;
                continue;
            }
        }
    }

    optimiseRemoveTrivialPredicates(query);
}

/**
 * OR of same-column constant ranges → loosest single bound when possible.
 * e.g. value < 3 OR value < 5  →  value < 5
 */
void optimiseMergeOrRanges(struct Node *node)
{
    if (node->function != OPERATOR_OR || node->child_count < 2)
        return;

    // Recurse first
    for (int i = 0; i < node->child_count; i++)
        optimiseMergeOrRanges(&node->children[i]);

    struct Node *first = &node->children[0];
    if (first->child_count < 2 || !isConstField(&first->children[1]))
        return;

    enum Function op = first->function;
    if (op != OPERATOR_LT && op != OPERATOR_LE && op != OPERATOR_GT &&
        op != OPERATOR_GE)
        return;

    for (int i = 1; i < node->child_count; i++)
    {
        struct Node *c = &node->children[i];
        if (c->function != op || c->child_count < 2 || !isConstField(&c->children[1]))
            return;
        if (!sameColumn(&first->children[0], &c->children[0]))
            return;
    }

    // All same op + column: pick loosest bound
    int best = 0;
    long best_v = atol(first->children[1].field.text);
    for (int i = 1; i < node->child_count; i++)
    {
        long v = atol(node->children[i].children[1].field.text);
        if (op == OPERATOR_LT || op == OPERATOR_LE)
        {
            if (v > best_v) { best_v = v; best = i; }
        }
        else
        {
            if (v < best_v) { best_v = v; best = i; }
        }
    }

    if (debug_verbosity >= 2)
        fprintf(stderr, "[OPTIMISE] Merge OR ranges → child %d\n", best);

    // Move best to slot 0, drop the rest, promote.
    if (best != 0)
    {
        swapNodes(&node->children[0], &node->children[best]);
    }
    while (node->child_count > 1)
    {
        removeChildAt(node, node->child_count - 1);
    }
    promoteSingleChild(node);
}

/**
 * From a=b (columns) and a=const, infer b=const (and vice versa).
 */
void optimiseTransitivePredicates(struct Query *query)
{
    int original_count = query->predicate_count;

    for (int i = 0; i < original_count; i++)
    {
        struct Node *join_eq = &query->predicate_nodes[i];
        if (join_eq->function != OPERATOR_EQ || join_eq->child_count < 2)
            continue;

        struct Node *jl = &join_eq->children[0];
        struct Node *jr = &join_eq->children[1];
        if (isConstField(jl) || isConstField(jr))
            continue;
        if (jl->function != FUNC_UNITY || jr->function != FUNC_UNITY)
            continue;
        if (jl->field.table_id == jr->field.table_id)
            continue;

        for (int j = 0; j < original_count; j++)
        {
            if (i == j) continue;
            struct Node *c = &query->predicate_nodes[j];
            if (c->function != OPERATOR_EQ || c->child_count < 2)
                continue;

            struct Node *cl = &c->children[0];
            struct Node *cr = &c->children[1];

            struct Node *col = NULL;
            struct Node *konst = NULL;
            struct Node *other = NULL;

            if (sameColumn(cl, jl) && isConstField(cr))
            {
                col = jl; konst = cr; other = jr;
            }
            else if (sameColumn(cl, jr) && isConstField(cr))
            {
                col = jr; konst = cr; other = jl;
            }
            else if (sameColumn(cr, jl) && isConstField(cl))
            {
                col = jl; konst = cl; other = jr;
            }
            else if (sameColumn(cr, jr) && isConstField(cl))
            {
                col = jr; konst = cl; other = jl;
            }
            else
                continue;

            (void)col;

            // Already have other = konst?
            int exists = 0;
            for (int k = 0; k < query->predicate_count; k++)
            {
                struct Node *p = &query->predicate_nodes[k];
                if (p->function == OPERATOR_EQ && p->child_count >= 2 &&
                    sameColumn(&p->children[0], other) &&
                    isConstField(&p->children[1]) &&
                    strcmp(p->children[1].field.text, konst->field.text) == 0)
                {
                    exists = 1;
                    break;
                }
            }
            if (exists)
                continue;

            if (debug_verbosity >= 2)
                fprintf(stderr, "[OPTIMISE] Transitive predicate\n");

            struct Node *target = allocatePredicateNode(query);
            target->function = OPERATOR_EQ;
            struct Node *tl = allocateNodeChildren(target, 2);
            struct Node *tr = tl + 1;
            copyNodeTree(tl, other);
            memcpy(tl->alias, other->alias, sizeof(tl->alias));
            copyNodeTree(tr, konst);
            memcpy(tr->alias, konst->alias, sizeof(tr->alias));
        }
    }
}

/**
 * Checks if the children of a node are a mathematical set (i.e. they are
 * all different)
 * @returns 1 if all children are unique; 0 if any two children are the same
 */
int areChildrenUnique(struct Node *node)
{
    for (int i = 1; i < node->child_count; i++)
    {
        for (int j = 0; j < i; j++)
        {
            int are_equal = areNodesEqual(&node->children[i], &node->children[j]);

            if (are_equal)
            {
                return 0;
            }
        }
    }

    return 1;
}

int isSameColumnEqOr(struct Node *node)
{
    if (node->function != OPERATOR_OR || node->child_count < 1)
    {
        return 0;
    }

    struct Node *first = &node->children[0];

    if (
        first->function != OPERATOR_EQ ||
        first->child_count < 2)
    {
        return 0;
    }

    struct Node *first_left = &first->children[0];

    for (int i = 1; i < node->child_count; i++)
    {
        struct Node *child = &node->children[i];

        if (
            child->function != OPERATOR_EQ ||
            child->child_count < 2)
        {
            return 0;
        }

        if (!sameColumn(first_left, &child->children[0]))
        {
            return 0;
        }
    }

    return 1;
}
