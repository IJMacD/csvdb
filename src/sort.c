#include <stdlib.h>
#include "sort.h"
#include "tree.h"
#include "query.h"
#include "debug.h"

static void sort_walkTree (struct tree *node, struct RowList * source_list, struct RowList * target_list);

static void sort_walkTreeBackwards (struct tree *node, struct RowList * source_list, struct RowList * target_list);

/**
 * @param db
 * @param field_index
 * @param direction
 * @param source_list Can be NULL to default to every row
 * @param target_list
 */
void sortResultRows (struct DB *db, int table_id, int field_index, int direction, struct RowList * source_list, struct RowList * target_list) {
    if (source_list->row_count <= 0) {
        return;
    }

    struct tree *pool = malloc(sizeof (struct tree) * source_list->row_count);
    struct tree *root = NULL;

    for (int i = 0; i < source_list->row_count; i++) {
        struct tree *node = pool++;
        node->index = i;

        node->rowid = getRowID(source_list, table_id, i);

        // For first (root) node we do a dummy insert to make sure struct
        // has been initialised properly
        insertNode(db, field_index, root, node);

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

static void sort_walkTree (struct tree *node, struct RowList * source_list, struct RowList * target_list) {

    if (node->left != NULL) {
        sort_walkTree(node->left, source_list, target_list);
    }

    // fprintf(stderr, "Write row %d (rowid: %d)\n", node->index, node->rowid);

    copyResultRow(target_list, source_list, node->index);

    if (node->right != NULL) {
        sort_walkTree(node->right, source_list, target_list);
    }
}

static void sort_walkTreeBackwards (struct tree *node, struct RowList * source_list, struct RowList * target_list) {
    if (node->right != NULL) {
        sort_walkTreeBackwards(node->right, source_list, target_list);
    }

    copyResultRow(target_list, source_list, node->index);

    if (node->left != NULL) {
        sort_walkTreeBackwards(node->left, source_list, target_list);
    }
}