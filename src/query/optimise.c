#include "../structs.h"
#include "../evaluate/evaluate.h"

void optimiseCollapseConstantNode (struct Node *node)  {
    if (node->function == FUNC_UNITY) {
        return;
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