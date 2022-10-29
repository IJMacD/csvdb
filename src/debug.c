#include <stdio.h>

#include "structs.h"
#include "result.h"

void debugRowList (struct RowList * list, int verbosity) {
    if (list == NULL) {
        fprintf(stderr, "RowList (NULL)\n");
        return;
    }

    fprintf(stderr, "RowList (%d joins x %d rows)\n", list->join_count, list->row_count);

    if (verbosity > 1) {
        for (int i = 0; i < list->row_count; i++) {
            fprintf(stderr, "Index %3d, Rowids: (", i);
            for (int j = 0; j < list->join_count; j++) {
                if (j > 0) {
                    fprintf(stderr, ", ");
                }
                fprintf(stderr, "%d", getRowID(list, j, i));
            }
            fprintf(stderr, ")\n");
        }
    }
}

void debugTree (struct tree * node) {
    printf("{ \"index\": %d, \"rowid\": %d, \"value\": %ld, \"left\": ", node->index, node->rowid, node->value);

    if (node->left != NULL) {
        debugTree(node->left);
    }
    else printf(" null ");

    printf(", \"right\": ");

    if (node->right != NULL) {
        debugTree(node->right);
    }
    else printf(" null ");

    printf(" }");
}