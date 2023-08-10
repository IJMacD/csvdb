#include <stdio.h>

#include "structs.h"
#include "query/result.h"

static void debugNodeInner (struct Node * node, int depth);

void debugRowList (struct RowList * list, int verbosity) {
    if (list == NULL) {
        fprintf(stderr, "RowList (NULL)\n");
        return;
    }

    fprintf(
        stderr,
        "RowList (%d joins x %d rows)\n",
        list->join_count,
        list->row_count
    );

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

void debugTree (struct TreeNode * node) {
    printf(
        "{ \"key\": %d, \"value\": \"%s\", \"left\": ",
        node->key,
        node->value
    );

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

void debugResultSet (struct ResultSet *results) {
    fprintf(stderr, "ResultSet (count = %d): ", results->count);
    for (int i = 0; i < results->count; i++) {
        fprintf(stderr, "%d, ", results->row_list_indices[i]);
    }
    fprintf(stderr, "\n");
}

void debugNode (struct Node * node) {
    debugNodeInner(node, 0);
}

static void debugNodeInner (struct Node * node, int depth) {
    fprintf(
        stderr,
        "%*s[NODE] ", depth, "");

    if (node->field.index == FIELD_CONSTANT) {
        fprintf(
            stderr,
            "Field: { CONSTANT text = %s } ",
            node->field.text
        );
    }
    else if (node->field.index != FIELD_UNKNOWN) {
        fprintf(
            stderr,
            "Field: { index = %d, table_id = %d, text = %s } ",
            node->field.index,
            node->field.table_id,
            node->field.text
        );
    }

    if (node->function != FUNC_UNITY) {
        fprintf(
            stderr,
            "Function: 0x%x ",
            node->function
        );
    }

    fprintf(stderr, "\n");

    for (int i = 0; i < node->child_count; i++) {
        debugNodeInner(node->children + i, depth + 1);
    }
}