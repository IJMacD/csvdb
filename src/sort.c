#include <stdlib.h>

#include "structs.h"
#include "evaluate.h"
#include "result.h"
#include "tree.h"
#include "db.h"
#include "util.h"

static void sort_walkTree (struct TreeNode *node, struct RowList * source_list, struct RowList * target_list);

static void sort_walkTreeBackwards (struct TreeNode *node, struct RowList * source_list, struct RowList * target_list);

/**
 * @param db
 * @param sort_fields
 * @param sort_count
 * @param source_list
 * @param target_list
 */
// To implement better sort later
// void sortResultRows (struct DB *db, struct SortField *sort_fields, int sort_count, struct RowList * source_list, struct RowList * target_list) {

/**
 * @param db
 * @param table_id
 * @param field_index
 * @param direction
 * @param source_list
 * @param target_list
 */
void sortResultRows (struct Query *q, struct ColumnNode *column, int direction, struct RowList * source_list, struct RowList * target_list) {
    if (source_list->row_count <= 0) {
        return;
    }

    struct TreeNode *pool = malloc(sizeof (*pool) * source_list->row_count);
    struct TreeNode *root = NULL;

    for (int i = 0; i < source_list->row_count; i++) {
        struct TreeNode *node = pool++;
        node->key = i;

        evaluateNode(q, source_list, i, column, node->value, sizeof(node->value));

        // Must be fixed width to compare numerically.
        // Yes slower than comparing as native long, but how much?
        if (is_numeric(node->value)) {
            long value = atol(node->value);
            sprintf(node->value, "%32ld", value);
        }

        // For first (root) node we do a dummy insert to make sure struct
        // has been initialised properly
        insertNode(root, node);

        if (i == 0) {
            // Now set the root node to the first position in the pool
            root = node;
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

    free(root);
}

static void sort_walkTree (struct TreeNode *node, struct RowList * source_list, struct RowList * target_list) {

    if (node->left != NULL) {
        sort_walkTree(node->left, source_list, target_list);
    }

    // fprintf(stderr, "Write row %d (rowid: %d)\n", node->index, node->rowid);

    copyResultRow(target_list, source_list, node->key);

    if (node->right != NULL) {
        sort_walkTree(node->right, source_list, target_list);
    }
}

static void sort_walkTreeBackwards (struct TreeNode *node, struct RowList * source_list, struct RowList * target_list) {
    if (node->right != NULL) {
        sort_walkTreeBackwards(node->right, source_list, target_list);
    }

    copyResultRow(target_list, source_list, node->key);

    if (node->left != NULL) {
        sort_walkTreeBackwards(node->left, source_list, target_list);
    }
}