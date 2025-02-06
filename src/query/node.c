#include <stdlib.h>
#include <string.h>
#include "../structs.h"
#include "../db/db.h"
#include "node.h"

/**
 * Allocates additional child nodes on a node
 * returns the first new child added
 */
struct Node * allocateNodeChildren (struct Node *node, int new_children) {
    struct Node *mem;

    if (node->child_count == 0) {
        mem = malloc(sizeof (*mem) * new_children);
    } else {
        mem = realloc(
            node->children,
            sizeof(*mem) * (node->child_count + new_children)
        );
    }

    if (mem == NULL) {
        fprintf(stderr, "Out of memory\n");
        exit(-1);
    }

    node->children = mem;

    struct Node *first_new_child = &node->children[node->child_count];

    // NULL out children to avoid uninitialized read and set defaults
    struct Node *child_node = first_new_child;
    for (int i = 0; i < new_children; i++) {
        clearNode(child_node);

        child_node++;
    }

    node->child_count += new_children;

    return first_new_child;
}

/**
 * mallocs a new node and adds it as a child of the provided node.
 * Also returns the new child for convenience.
 *
 *       P
 *    +--+--+
 *    C  C  C'
 *
 * P is the original node passed to this function
 * C' is the new child. C' is returned from this function.
 */
struct Node *addChildNode (struct Node *parent_node) {
    // Optimistation where node is its own child
    if (parent_node->child_count == -1) {
        fprintf(
            stderr,
            "Cannot add a child to an optimised node.\n"
        );
        exit(-1);
    }

    return allocateNodeChildren(parent_node, 1);
}

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

    dest->filter = NULL;

    if (src->children != NULL && src->child_count > 0) {
        dest->children = calloc(src->child_count, sizeof(*dest));

        for (int i = 0; i < src->child_count; i++) {
            copyNodeTree(&dest->children[i], &src->children[i]);
        }
    }

    if (src->filter != NULL) {
        dest->filter = calloc(1, sizeof(*dest));
        copyNodeTree(dest->filter, src->filter);
    }
}

void clearNode (struct Node *node) {
    node->children = NULL;
    node->child_count = 0;
    node->field.index = FIELD_UNKNOWN;
    node->field.table_id = -1;
    node->field.text[0] = '\0';
    node->function = FUNC_UNITY;
    node->alias[0] = '\0';
    node->filter = NULL;
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

    if (node->filter != NULL) {
        free(node->filter);
        node->filter = NULL;
    }
}

/**
 * Given node A, this function will create a child B, and copy the contents of
 * A into B.
 * Returns new left child node as convenience
 */
struct Node * cloneNodeIntoChild (struct Node *node) {
    // Clone node into new child
    struct Node *clone = malloc(sizeof *clone);

    // Make shallow copy
    memcpy(clone, node, sizeof *clone);

    // Set clone to be child of root node
    node->children = clone;
    node->child_count = 1;

    return &node->children[0];
}

/**
 * Generates a bit map to indicate which tables are referenced by a node
 * Least significant bit is Table 0 etc.
 */
int getTableBitMap (struct Node *node) {
    int map = 0;

    if (node->child_count == -1 || node->function == FUNC_UNITY) {
        if (node->field.index != FIELD_CONSTANT) {
            int tableBit = 1 << node->field.table_id;
            map |= tableBit;
        }
    }
    else for (int i = 0; i < node->child_count; i++) {
        map |= getTableBitMap(&node->children[i]);
    }

    return map;
}

/**
 * Swap node from one pointer to another and vice versa
 */
void swapNodes (struct Node *nodeA, struct Node *nodeB) {
    // Copy to stack
    struct Node tmp = *nodeA;
    memcpy(nodeA, nodeB, sizeof(tmp));
    memcpy(nodeB, &tmp, sizeof(tmp));
}

/**
 * Helper function
 * Just for convenience
 */
const char *nodeGetFieldName (struct Node *node) {
    const char *field_name = NULL;
    if (
        (node->function == FUNC_UNITY && node->child_count == 0) ||
        node->child_count == -1
    ) {
        field_name = node->field.text;
    }
    else if (node->child_count == 1) {
        field_name = node->children[0].field.text;
    }
    return field_name;
}

/**
 * Performs deep comparison of two node trees to evaluate if they are
 * functionally equivalent.
 * @returns 1 if equal; 0 if not equal
 */
int areNodesEqual(struct Node *nodeA, struct Node *nodeB)
{
    if (
        nodeA->function != nodeB->function ||
        nodeA->field.table_id != nodeB->field.table_id ||
        nodeA->field.index != nodeB->field.index ||
        strcmp(nodeA->field.text, nodeB->field.text) != 0 ||
        nodeA->child_count != nodeB->child_count)
    {
        return 0;
    }

    for (int i = 0; i < nodeA->child_count; i++)
    {
        int children_equal = areNodesEqual(&nodeA->children[i], &nodeB->children[i]);

        if (!children_equal)
        {
            return 0;
        }
    }

    return 1;
}