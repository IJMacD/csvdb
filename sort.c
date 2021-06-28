#include <stdlib.h>
#include "sort.h"
#include "tree.h"

void sortResultRows (struct DB *db, int field_index, int direction, const int *result_rowids, int result_count, int *out_rowids) {
    struct tree *pool = malloc(sizeof (struct tree) * result_count);
    struct tree *root = pool;

    for (int i = 0; i < result_count; i++) {
        struct tree *node = pool++;
        node->rowid = result_rowids[i];

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