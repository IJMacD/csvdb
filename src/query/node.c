#include <stdlib.h>
#include <string.h>
#include "../structs.h"

/**
 * @brief Copies a node tree recursively to avoid double FREE
 * Will malloc for children
 *
 * @param dest
 * @param src
 * @return void
 */
void copyNodeTree (struct Node *dest, struct Node *src) {
    memcpy(&dest->field, &src->field, sizeof(dest->field));

    dest->function = src->function;

    dest->child_count = src->child_count;

    dest->children = NULL;

    if (src->children != NULL && src->child_count > 0) {

        dest->children = malloc(sizeof(*dest) * src->child_count);

        for (int i = 0; i < src->child_count; i++) {
            copyNodeTree(&dest->children[i], &src->children[i]);
        }
    }
}

void freeNode (struct Node *node) {
    if (node->child_count > 0) {
        for (int i = 0; i < node->child_count; i++) {
            freeNode(&node->children[i]);
        }
    }

    if (node->children != NULL) {
        free(node->children);
        node->children = NULL;
    }
}
