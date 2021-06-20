#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "db.h"
#include "tree.h"
#include "limits.h"

#define MODE_ALPHA      0
#define MODE_NUMERIC    1

int is_numeric (const char *string);

void insertNode (struct DB *db, int field_index, struct tree *root, struct tree *node) {
    char field_value[VALUE_MAX_LENGTH];
    getRecordValue(db, node->rowid, field_index, field_value, VALUE_MAX_LENGTH);

    if (is_numeric(field_value)) {
        node->value = atol(field_value);
        insertNumericNode(root, node);
    } else {
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

    char root_value[VALUE_MAX_LENGTH];
    char node_value[VALUE_MAX_LENGTH];

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

void walkTree (struct tree *node, int **rowids) {
    // printf("{ \"value\": %ld, \"left\": ", node->value);

    if (node->left != NULL) {
        walkTree(node->left, rowids);
    }
    // else printf(" null ");

    // printf(", \"right\": ");

    **rowids = node->rowid;
    (*rowids)++;

    if (node->right != NULL) {
        walkTree(node->right, rowids);
    }
    // else printf(" null ");

    // printf(" }");
}

void walkTreeBackwards (struct tree *node, int **rowids) {
    if (node->right != NULL) {
        walkTreeBackwards(node->right, rowids);
    }

    **rowids = node->rowid;
    (*rowids)++;

    if (node->left != NULL) {
        walkTreeBackwards(node->left, rowids);
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