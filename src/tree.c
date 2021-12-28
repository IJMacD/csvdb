#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "db.h"
#include "tree.h"
#include "limits.h"
#include "util.h"
#include "result.h"

#define MODE_ALPHA      0
#define MODE_NUMERIC    1

unsigned long charsToLong (const char *string);

/**
 * This function also prepares `node` for use
 */
void insertNode (struct DB *db, int field_index, struct tree *root, struct tree *node) {
    // Prepare `left` and `right`
    node->left = NULL;
    node->right = NULL;

    char field_value[VALUE_MAX_LENGTH] = {0};
    getRecordValue(db, node->rowid, field_index, field_value, VALUE_MAX_LENGTH);

    if (is_numeric(field_value)) {
        // Prepare `value`
        node->value = atol(field_value);
        insertNumericNode(root, node);
    } else {
        // Prepare `value` (optimisation)
        node->value = charsToLong(field_value);
        insertTextNode(db, field_index, root, node);
    }
}

void insertNumericNode (struct tree *root, struct tree *node) {
    if (root == NULL) return;

    if (node->value < root->value) {
        if (root->left == NULL) root->left = node;
        else {
            insertNumericNode(root->left, node);
        }
    }
    else {
        if (root->right == NULL) root->right = node;
        else {
            insertNumericNode(root->right, node);
        }
    }
}

void insertTextNode (struct DB *db, int field_index, struct tree *root, struct tree *node) {
    if (root == NULL) return;

    // Optimisation: Use cached numeric value first
    if (node->value < root->value) {
        if (root->left == NULL) root->left = node;
        else {
            insertTextNode(db, field_index, root->left, node);
        }
    }
    else if (node->value > root->value) {
        if (root->right == NULL) root->right = node;
        else {
            insertTextNode(db, field_index, root->right, node);
        }
    }
    // If numeric value is equal then fallback to comparing the rest of the string
    else {
        char root_value[VALUE_MAX_LENGTH] = {0};
        char node_value[VALUE_MAX_LENGTH] = {0};

        getRecordValue(db, root->rowid, field_index, root_value, VALUE_MAX_LENGTH);
        getRecordValue(db, node->rowid, field_index, node_value, VALUE_MAX_LENGTH);

        if (strcmp(node_value, root_value) < 0) {
            if (root->left == NULL) root->left = node;
            else {
                insertTextNode(db, field_index, root->left, node);
            }
        }
        else {
            if (root->right == NULL) root->right = node;
            else {
                insertTextNode(db, field_index, root->right, node);
            }
        }
    }
}

void walkTree (struct tree *node, struct RowList * row_list) {
    // printf("{ \"value\": %ld, \"left\": ", node->value);

    if (node->left != NULL) {
        walkTree(node->left, row_list);
    }
    // else printf(" null ");

    // printf(", \"right\": ");

    appendRowID(row_list, node->rowid);

    if (node->right != NULL) {
        walkTree(node->right, row_list);
    }
    // else printf(" null ");

    // printf(" }");
}

void walkTreeBackwards (struct tree *node, struct RowList * row_list) {
    if (node->right != NULL) {
        walkTreeBackwards(node->right, row_list);
    }

    appendRowID(row_list, node->rowid);

    if (node->left != NULL) {
        walkTreeBackwards(node->left, row_list);
    }
}

// Can't just use memcpy because of endianess/trailing nulls etc.
unsigned long charsToLong (const char *string) {
    unsigned long result = 0;
    size_t s = sizeof(result);
    size_t l = strlen(string);

    for (size_t i = 0; i < s; i++) {
        result <<= 8;
        if (i < l) {
            result |= (unsigned char)string[i];
        }
        // printf("%02ld: %02x %016lx\n", i, string[i], result);
    }

    // printf("%016lx %s\n", result, string);

    return result;
}