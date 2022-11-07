#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../structs.h"

static void walkTree (
    struct TreeNode *node,
    struct TreeNode **list,
    int *index
);

static struct TreeNode *buildBalancedTree (
    struct TreeNode **list,
    int start,
    int end
);

void insertNode (struct TreeNode *root, struct TreeNode *node) {
    // We're responsible for prepping nodes
    node->left = NULL;
    node->right = NULL;

    // Root node will come though here for prep but will bail at this point.
    if (root == NULL) return;

    int result = strcmp(node->value, root->value);
    if (result < 0) {
        if (root->left == NULL) root->left = node;
        else insertNode(root->left, node);
    }
    else {
        if (root->right == NULL) root->right = node;
        else insertNode(root->right, node);
    }
}


struct TreeNode *rebalanceTree (struct TreeNode *root, int count) {
    struct TreeNode **list = malloc(sizeof(root) * count);
    int index = 0;
    walkTree(root, list, &index);
    struct TreeNode *new_root = buildBalancedTree(list, 0, count -1);
    free(list);
    return new_root;
}

static void walkTree (
    struct TreeNode *node,
    struct TreeNode **list,
    int *index
) {
    if (node->left != NULL) {
        walkTree(node->left, list, index);
    }

    list[(*index)++] = node;

    if (node->right != NULL) {
        walkTree(node->right, list, index);
    }
}

static struct TreeNode *buildBalancedTree (
    struct TreeNode **list,
    int start,
    int end
) {
    if (start > end) {
        return NULL;
    }

    int midpoint = (start + end) / 2;
    struct TreeNode *root = list[midpoint];

    root->left = buildBalancedTree(list, start, midpoint-1);
    root->right = buildBalancedTree(list, midpoint+1, end);

    return root;
}