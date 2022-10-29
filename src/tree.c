#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "structs.h"

void insertNode (struct TreeNode *root, struct TreeNode *node) {
    // We're responsible for prepping nodes
    node->left = NULL;
    node->right = NULL;

    // Root node will come though here for prep but will bail at this point.
    if (root == NULL) return;

    if (strcmp(node->value, root->value) < 0) {
        if (root->left == NULL) root->left = node;
        else {
            insertNode(root->left, node);
        }
    }
    else {
        if (root->right == NULL) root->right = node;
        else {
            insertNode(root->right, node);
        }
    }
}
