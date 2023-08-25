#include <string.h>

#include "../structs.h"
#include "../evaluate/evaluate.h"

void optimiseCollapseConstantNode (struct Node *node)  {
    if (node->function == FUNC_UNITY || node->child_count == -1) {
        return;
    }

    // Optimise all children first
    for (int i = 0; i < node->child_count; i++) {
        optimiseCollapseConstantNode(&node->children[i]);
    }

    // Make sure all children are constant
    for (int i = 0; i < node->child_count; i++) {
        if (node->children[i].field.index != FIELD_CONSTANT) {
            return;
        }
    }

    // Evaluate the function and write result to field
    evaluateConstantNode(node, node->field.text);

    // Mark the node as constant and remove function marker
    node->field.index = FIELD_CONSTANT;
    node->function = FUNC_UNITY;
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