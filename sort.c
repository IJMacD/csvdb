#include <stdlib.h>
#include "sort.h"
#include "tree.h"

/**
 * @param db
 * @param field_index
 * @param direction
 * @param rowids List of rowids to sort. Can be set to NULL to use 0..row_count
 * @param row_count
 * @param out_rowids
 */
void sortResultRows (struct DB *db, int field_index, int direction, const int *rowids, int row_count, int *out_rowids) {
    if (row_count <= 0) {
        return;
    }

    struct tree *pool = malloc(sizeof (struct tree) * row_count);
    struct tree *root = pool;

    for (int i = 0; i < row_count; i++) {
        struct tree *node = pool++;
        node->rowid = rowids == NULL ? i : rowids[i];

        if (i == 0) {
            // For root node we do a dummy insert to make sure struct
            // has been initialised properly
            insertNode(db, field_index, NULL, node);
        } else {
            insertNode(db, field_index, root, node);
        }
    }

    // Walk tree overwriting result_rowids array
    int *result_ptr = out_rowids;
    if (direction == ORDER_ASC) {
        walkTree(root, &result_ptr);
    }
    else {
        walkTreeBackwards(root, &result_ptr);
    }

    free(root);
}