#include <stdlib.h>
#include <string.h>

#include "../structs.h"
#include "../evaluate/evaluate.h"
#include "../query/result.h"
#include "tree.h"
#include "../db/db.h"
#include "../functions/util.h"
#include "../debug.h"

static void sort_walkTree (
    struct TreeNode *node,
    struct RowList * source_list,
    struct RowList * target_list
);

static void sort_walkTreeBackwards (
    struct TreeNode *node,
    struct RowList * source_list,
    struct RowList * target_list
);

/**
 * @brief source_list must be different from target_list
 *
 * @param db
 * @param table_id
 * @param field_index
 * @param direction
 * @param source_list
 * @param target_list
 */
void sortResultRows (
    struct Table *tables,
    struct Node *node,
    int direction,
    struct RowList * source_list,
    struct RowList * target_list
) {
    if (source_list->row_count <= 0) {
        return;
    }

    struct TreeNode *pool = malloc(sizeof (*pool) * source_list->row_count);
    struct TreeNode *root = NULL;

    int numeric_mode = 0;

    for (int i = 0; i < source_list->row_count; i++) {
        struct TreeNode *treenode = &pool[i];
        treenode->key = i;

        evaluateNode(
            tables,
            source_list,
            i,
            node,
            treenode->value,
            sizeof(treenode->value)
        );

        // First value determines whether we're in numeric mode or not
        if (i == 0) {
            numeric_mode = is_numeric(treenode->value);
        }

        // Numeric values need to be fixed width for comparison.
        // After testing it make no difference whether numeric values are
        // compared or strings are compared. (There are other slower steps).
        if (numeric_mode) {
            long number = atol(treenode->value);
            sprintf(treenode->value, "%020ld", number);
        }

        // For first (root) node we do a dummy insert to make sure struct
        // has been initialised properly
        insertNode(root, treenode);

        if (i == 0) {
            // Now set the root node to the first position in the pool
            root = treenode;
        }
        // When the range of sort values is small the tree becomes unbalanced
        // resulting in *increadibly* slow sorts.
        // We'll periodically rebalance just in case.
        else if (i % 10000 == 0) {
            root = rebalanceTree(root, i + 1);
        }
    }

    // debugTree(root);

    // Walk tree writing result_rowids array
    if (direction == ORDER_ASC) {
        sort_walkTree(root, source_list, target_list);
    }
    else {
        sort_walkTreeBackwards(root, source_list, target_list);
    }

    free(pool);
}

/**
 * @brief Sort row_list based on multiple columns.
 * Warning! All sort directions must be the same.
 *
 * @param q
 * @param nodes
 * @param node_count
 * @param direction
 * @param source_list
 * @param target_list
 */
void sortResultRowsMultiple (
    struct Table *tables,
    struct Node *nodes,
    int node_count,
    int *sort_directions,
    RowListIndex source_list_id,
    RowListIndex target_list_id
) {
    struct RowList *source_list = getRowList(source_list_id);

    struct TreeNode *pool = malloc(sizeof (*pool) * source_list->row_count);
    struct TreeNode *root = NULL;

    for (int i = 0; i < source_list->row_count; i++) {
        struct TreeNode *node = &pool[i];
        node->key = i;

        evaluateNodeList(
            tables,
            source_list,
            i,
            nodes,
            node_count,
            node->value,
            sizeof(node->value)
        );

        // For first (root) node we do a dummy insert to make sure struct
        // has been initialised properly
        insertNode(root, node);

        if (i == 0) {
            // Now set the root node to the first position in the pool
            root = node;
        }
        // When the range of sort values is small the tree becomes unbalanced
        // resulting in *increadibly* slow sorts.
        // We'll periodically rebalance just in case.
        else if (i % 10000 == 0) {
            root = rebalanceTree(root, i + 1);
        }
    }

    // debugTree(root);

    // Walk tree writing result_rowids array
    if (sort_directions[0] == ORDER_ASC) {
        sort_walkTree(root, source_list, getRowList(target_list_id));
    }
    else {
        sort_walkTreeBackwards(root, source_list, getRowList(target_list_id));
    }

    free(pool);
}

static void sort_walkTree (
    struct TreeNode *node,
    struct RowList * source_list,
    struct RowList * target_list
) {

    if (node->left != NULL) {
        sort_walkTree(node->left, source_list, target_list);
    }

    // fprintf(stderr, "Write row %d (rowid: %d)\n", node->index, node->rowid);

    copyResultRow(target_list, source_list, node->key);

    if (node->right != NULL) {
        sort_walkTree(node->right, source_list, target_list);
    }
}

static void sort_walkTreeBackwards (
    struct TreeNode *node,
    struct RowList * source_list,
    struct RowList * target_list
) {
    if (node->right != NULL) {
        sort_walkTreeBackwards(node->right, source_list, target_list);
    }

    copyResultRow(target_list, source_list, node->key);

    if (node->left != NULL) {
        sort_walkTreeBackwards(node->left, source_list, target_list);
    }
}
