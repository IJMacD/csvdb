#include <stdlib.h>
#include <string.h>

#include "../structs.h"
#include "../debug.h"
#include "../evaluate/evaluate.h"
#include "../evaluate/predicates.h"

void optimiseCollapseConstantNode (struct Node *node)  {
    if (node->function == FUNC_UNITY) {
        return;
    }

    // Optimise all children first
    for (int i = 0; i < node->child_count; i++) {
        optimiseCollapseConstantNode(&node->children[i]);
    }

    // If this is a self-child node, check if the field is constant
    if (node->child_count == -1 && node->field.index != FIELD_CONSTANT) {
        return;
    }

    // Make sure all children are constant
    for (int i = 0; i < node->child_count; i++) {
        if (node->children[i].field.index != FIELD_CONSTANT) {
            return;
        }
    }

    if ((node->function & MASK_FUNC_FAMILY) == FUNC_FAM_OPERATOR) {
        // If we're evaluating an operator then we can definitively set the node
        // to OPERATOR_ALWAYS or OPERATOR_NEVER

        #if DEBUG
        if (debug_verbosity >= 2) {
            fprintf(stderr, "[OPTIMISE] Constant Collapse (Operator)\n");
        }
        #endif

        // TODO: What about cases with any child count other than 2?

        char value_left[MAX_VALUE_LENGTH] = {0};
        char value_right[MAX_VALUE_LENGTH] = {0};

        evaluateConstantNode(&node->children[0], value_left);
        evaluateConstantNode(&node->children[1], value_right);

        int result = evaluateExpression(node->function, value_left, value_right);

        if (result) {
            node->function = OPERATOR_ALWAYS;
        }
        else {
            node->function = OPERATOR_NEVER;
        }
    }
    else {
        #if DEBUG
        if (debug_verbosity >= 2) {
            fprintf(stderr, "[OPTIMISE] Constant Collapse\n");
        }
        #endif

        // Evaluate the function and write result to field
        evaluateConstantNode(node, node->field.text);

        // Mark the node as constant and remove function marker
        node->field.index = FIELD_CONSTANT;
        node->function = FUNC_UNITY;
    }
}

/**
 * If given node is an operator node which includes a rowid column on one side
 * then try to do some algebra if necessary to get rowid on its own.
 */
void optimiseRowidAlgebra (struct Node *node) {
    int isOperator = (node->function & MASK_FUNC_FAMILY) == FUNC_FAM_OPERATOR;

    if (isOperator && node->child_count == 2) {
        struct Node *left_child = &node->children[0];
        struct Node *right_child = &node->children[1];

        int isMathsOperator = (left_child->function & (MASK_FUNC_FAMILY | 0x10)) == 0x10;

        if (isMathsOperator && left_child->child_count == 2) {
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

            if (left_grandchild->field.index == FIELD_ROW_INDEX) {
                // We should optimise!

                #if DEBUG
                if (debug_verbosity >= 2) {
                    fprintf(stderr, "[OPTIMISE] RowID algebra\n");
                }
                #endif

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

                if (left_child->function == FUNC_ADD) {
                    tmp.function = FUNC_SUB;
                }
                else if (left_child->function == FUNC_SUB) {
                    tmp.function = FUNC_ADD;
                }

                memcpy(&node->children[0], left_grandchild, sizeof tmp);
                memcpy(&tmp.children[0], right_child, sizeof tmp);
                memcpy(&node->children[1], &tmp, sizeof tmp);
            }
        }
    }
}

void optimiseFlattenANDPredicates (struct Query * query) {
    int have_AND_predicates = 0;

    // Count up how many extra predicate spaces we'll need
    for (int i = 0; i < query->predicate_count; i++) {
        struct Node *predicate = &query->predicate_nodes[i];
        if (predicate->function == OPERATOR_AND) {
            have_AND_predicates += predicate->child_count - 1;
        }
    }

    if (have_AND_predicates == 0) {
        return;
    }

    #if DEBUG
    if (debug_verbosity >= 2) {
        fprintf(stderr, "[OPTIMISE] Flatten AND predicates (%d)\n", have_AND_predicates);
    }
    #endif

    int new_predicate_count = query->predicate_count + have_AND_predicates;

    struct Node *new_predicates = malloc(
        new_predicate_count * sizeof *new_predicates
    );

    struct Node *curr_predicate = new_predicates;
    for (int i = 0; i < query->predicate_count; i++) {
        struct Node *predicate = &query->predicate_nodes[i];

        if (predicate->function == OPERATOR_AND) {
            for (int j = 0; j < predicate->child_count; j++) {
                memcpy(
                    curr_predicate++,
                    &predicate->children[j],
                    sizeof *predicate
                );
            }
        }
        else {
            memcpy(curr_predicate++, predicate, sizeof *predicate);
        }
    }

    free(query->predicate_nodes);
    query->predicate_nodes = new_predicates;
    query->predicate_count = new_predicate_count;
}
