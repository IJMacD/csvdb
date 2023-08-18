#include <stdio.h>

#include <unistd.h>

#include "structs.h"
#include "query/result.h"

static void debugNodeInner (struct Node * node, int depth);

void debugRowList (struct RowList * list, int verbosity) {
    if (list == NULL) {
        fprintf(stderr, "\tRowList (NULL)\n");
        return;
    }

    fprintf(
        stderr,
        "\tRowList (%d joins x %d rows)\n",
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
    fprintf(stderr, "\tResultSet (count = %d): ", results->count);
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
        "\t%*s[NODE] ", depth, "");

    if (node->function != FUNC_UNITY) {
        if ((node->function & MASK_FUNC_FAMILY) == FUNC_FAM_OPERATOR) {
            char *operators[] = {
                "NEVER","EQ","LT","LE","GT","GE","NE","ALWAYS"
            };
            char *s = operators[node->function & ~MASK_FUNC_FAMILY];
            fprintf(
                stderr,
                "OPERATOR_%s ",
                s
            );
        }
        else {
            fprintf(
                stderr,
                "Function: 0x%X ",
                node->function
            );
        }
    }

    if (node->function == FUNC_UNITY || node->child_count == -1) {
        if (node->field.index == FIELD_CONSTANT) {
            fprintf(
                stderr,
                "Field: { CONSTANT text = %s } ",
                node->field.text
            );
        }
        else if (node->field.index == FIELD_STAR) {
            fprintf(
                stderr,
                "Field: { *, table_id = %d } ",
                node->field.table_id
            );
        }
        else if (node->field.index != FIELD_UNKNOWN) {
            fprintf(
                stderr,
                "Field: { table_id = %d, index = %d, text = %s } ",
                node->field.table_id,
                node->field.index,
                node->field.text
            );
        }
        else {
            fprintf(
                stderr,
                "Field: { UNKNOWN text = %s } ",
                node->field.text
            );
        }
    }

    fprintf(stderr, "\n");

    for (int i = 0; i < node->child_count; i++) {
        debugNodeInner(node->children + i, depth + 1);
    }
}

void debugNodes (struct Node nodes[], int node_count) {
    for (int i = 0; i < node_count; i++) {
        debugNode(&nodes[i]);
    }
}

void debugLog (struct Query *query, const char *msg) {
    #ifdef DEBUG
    fprintf(stderr, "[Q%d.%d] %s\n", getpid(), query->id, msg);
    #endif
}