#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>

#include "db.h"
#include "tree.h"
#include "limits.h"

#define MODE_ALPHA      0
#define MODE_NUMERIC    1

int is_numeric (const char *string);

void makeNode (struct DB *db, int field_index, int rowid, struct tree *node) {
    char field_value[VALUE_MAX_LENGTH];
    getRecordValue(db, rowid, field_index, field_value, VALUE_MAX_LENGTH);

    long field_value_numeric = 0;

    if (is_numeric(field_value)) {
        field_value_numeric = atol(field_value);
    }

    node->rowid = rowid;
    node->value = field_value_numeric;
}

void walkTree (struct tree *node, int **rowids) {

    if (node->left != NULL) {
        walkTree(node->left, rowids);
    }

    **rowids = node->rowid;
    (*rowids)++;

    if (node->right != NULL) {
        walkTree(node->right, rowids);
    }
}

void insertNumericNode (struct tree *root, struct tree *node) {
    if (node->value < root->value) {
        if (root->left == NULL) root->left = node;
        else {
            insertNumericNode(root->left, node);
        }
        return;
    }
    
    if (root->right == NULL) root->right = node;
    else {
        insertNumericNode(root->right, node);
    }
}

int is_numeric (const char *string) {
    const char *ptr = string;
    while (*ptr != '\0') {
        if (!isdigit(*ptr)) return 0;
        ptr++;
    }
    return 1;
}