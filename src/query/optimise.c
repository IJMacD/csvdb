#include <stdlib.h>
#include <string.h>

#include "./select.h"
#include "./node.h"
#include "../structs.h"
#include "../evaluate/evaluate.h"
#include "../evaluate/predicates.h"
#include "../functions/util.h"
#include "../debug.h"

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
            exit(-1);
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

    if (isOperator && node->child_count == 2)
    {
        struct Node *left_child = &node->children[0];
        struct Node *right_child = &node->children[1];

        int isMathsOperator = (left_child->function & (MASK_FUNC_FAMILY | 0x10)) == 0x10;

        if (isMathsOperator && left_child->child_count == 2)
        {
            if (left_child->function != FUNC_ADD &&
                left_child->function != FUNC_SUB)
            {
                // Can't optimise for integer division etc. yet
                // need to return two nodes to specify a range
                // e.g. rowid / 10 = 5
                //   -> rowid >= 50 && rowid < 60
                //   -> rowid IN (50, 51, 52, 53, 54, 55, 56, 57, 58, 59)
                //
                // Integer multiplication has inverse problem
                // e.g. rowid * 10 = 21     (no matches)
                //  ->  rowid = 21 / 10     (rowid=2 matches)
                // e.g. rowid * A = B
                //  ->  rowid = B / A && B % A = 0
                return;
            }

            struct Node *left_grandchild = &left_child->children[0];
            // struct Node *right_grandchild = &left_child->children[1];

            if (left_grandchild->field.index == FIELD_ROW_INDEX)
            {
                // We should optimise!

                if (debug_verbosity >= 2)
                {
                    fprintf(stderr, "[OPTIMISE] RowID algebra\n");
                }

                // e.g. tree before
                //          =       |
                //       /    \     |
                //      +     10    |
                //    /   \         |
                // rowid   1        |

                // e.g. tree after
                //          =           |
                //        /   \         |
                //    rowid     -       |
                //            /   \     |
                //           10    1    |

                // e.g. tree before
                //          =           |
                //       /    \         |
                //      A      D        |
                //    /   \             |
                //   B     C            |

                // e.g. tree after
                //          =           |
                //        /   \         |
                //       B     A'       |
                //            /  \      |
                //           D    C     |

                struct Node tmp = *left_child;

                if (left_child->function == FUNC_ADD)
                {
                    tmp.function = FUNC_SUB;
                }
                else if (left_child->function == FUNC_SUB)
                {
                    tmp.function = FUNC_ADD;
                }

                memcpy(&node->children[0], left_grandchild, sizeof tmp);
                memcpy(&tmp.children[0], right_child, sizeof tmp);
                memcpy(&node->children[1], &tmp, sizeof tmp);
            }
        }
    }
}

void optimiseFlattenANDPredicates(struct Query *query)
{
    int have_AND_predicates = 0;

    // Count up how many extra predicate spaces we'll need
    for (int i = 0; i < query->predicate_count; i++)
    {
        struct Node *predicate = &query->predicate_nodes[i];
        if (predicate->function == OPERATOR_AND)
        {
            have_AND_predicates += predicate->child_count - 1;
        }
    }

    if (have_AND_predicates == 0)
    {
        return;
    }

    if (debug_verbosity >= 2)
    {
        fprintf(stderr, "[OPTIMISE] Flatten AND predicates (%d)\n", have_AND_predicates);
    }

    int new_predicate_count = query->predicate_count + have_AND_predicates;

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

        struct Node *left_node = &predicate->children[0];
        struct Node *right_node = &predicate->children[1];

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
