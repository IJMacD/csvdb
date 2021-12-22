#include <stdlib.h>
#include "sort.h"
#include "tree.h"

/**
 * @param db
 * @param field_index
 * @param direction
 * @param source_list Can be NULL to default to every row
 * @param target_list
 */
void sortResultRows (struct DB *db, int field_index, int direction, struct RowList * source_list, struct RowList * target_list) {
    if (source_list->row_count <= 0) {
        return;
    }

    struct tree *pool = malloc(sizeof (struct tree) * source_list->row_count);
    struct tree *root = pool;

    for (int i = 0; i < source_list->row_count; i++) {
        struct tree *node = pool++;
        node->rowid = getRowID(source_list, 0 , i);

        if (i == 0) {
            // For root node we do a dummy insert to make sure struct
            // has been initialised properly
            insertNode(db, field_index, NULL, node);
        } else {
            insertNode(db, field_index, root, node);
        }
    }

    // Walk tree overwriting result_rowids array
    target_list->row_count = 0;
    if (direction == ORDER_ASC) {
        walkTree(root, target_list);
    }
    else {
        walkTreeBackwards(root, target_list);
    }

    free(root);
}