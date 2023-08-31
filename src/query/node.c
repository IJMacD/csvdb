#include <stdlib.h>
#include <string.h>
#include "../structs.h"
#include "../db/db.h"

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
        child_node->children = NULL;
        child_node->child_count = 0;
        child_node->field.index = FIELD_UNKNOWN;
        child_node->field.table_id = -1;
        child_node->field.text[0] = '\0';
        child_node->function = FUNC_UNITY;
        child_node->alias[0] = '\0';

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

    if (src->children != NULL && src->child_count > 0) {

        dest->children = calloc( src->child_count, sizeof(*dest));

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
 * mallocs two new nodes and adds them as a child of the provided node.
 * It then copies the provided node to the first of the two new nodes and
 * set the original node to a new parent.
 * Also returns the second child for convenience.
 *
 *       P'
 *       +--+
 *       P  C
 *
 * P' is the original node modified to be a new parent
 * P is newly allocated and is a copy of the original node
 * C is newly allocated and is returned (blank) from this function.
 */
// static struct Node *replaceParentNode (
//     struct Node *node,
//     enum Function newFunction
// ) {

//     int new_child_count = 2;

//     struct Node * new_children = calloc(new_child_count, sizeof(*node));

//     // copy existing node to new child
//     struct Node *first_child = &new_children[0];
//     memcpy(first_child, node, sizeof(*node));

//     // set node function to new function
//     node->function = newFunction;

//     // set new children
//     node->children = new_children;

//     // set new child count
//     node->child_count = new_child_count;

//     // Clear current field
//     node->field.text[0] = '\0';

//     // Clear field index
//     node->field.index = FIELD_UNKNOWN;

//     // Return second child
//     struct Node *child_node = &node->children[1];

//     return child_node;
// }

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
 * Helper function as wrapper on findIndex
 * Tries to locate an index on a node
 */
enum IndexSearchType findNodeIndex (
    struct DB *db,
    const char *table_name,
    struct Node *node,
    enum IndexSearchType index_type_flags
) {
    if (node->function == FUNC_UNITY) {
        const char *field_name = node->field.text;
        return findIndex(db, table_name, field_name, index_type_flags);
    }

    // indexes on functions are not supported yet

    return INDEX_NONE;
}