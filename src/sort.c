#include <stdlib.h>

#include "structs.h"
#include "evaluate.h"
#include "result.h"
#include "tree.h"
#include "db.h"
#include "util.h"
#include "debug.h"

static void sort_walkTree (struct TreeNode *node, struct RowList * source_list, struct RowList * target_list);

static void sort_walkTreeBackwards (struct TreeNode *node, struct RowList * source_list, struct RowList * target_list);

/**
 * @param db
 * @param sort_fields
 * @param sort_count
 * @param source_list
 * @param target_list
 */
// To implement better sort later
// void sortResultRows (struct DB *db, struct SortField *sort_fields, int sort_count, struct RowList * source_list, struct RowList * target_list) {

/**
 * @brief source_list must be different from target_list
 *
 * @param db
 * @param table_id
 * @param field_index
 * @param direction
 * @param source_list
 * @param target_list
 */
void sortResultRows (struct Query *q, struct ColumnNode *column, int direction, struct RowList * source_list, struct RowList * target_list) {
    if (source_list->row_count <= 0) {
        return;
    }

    struct TreeNode *pool = malloc(sizeof (*pool) * source_list->row_count);
    struct TreeNode *root = NULL;

    int numeric_mode = 0;

    for (int i = 0; i < source_list->row_count; i++) {
        struct TreeNode *node = &pool[i];
        node->key = i;

        evaluateNode(q, source_list, i, column, node->value, sizeof(node->value));

        // First value determines whether we're in numeric mode or not
        if (i == 0) {
            numeric_mode = is_numeric(node->value);
        }

        // Numeric values need to be fixed width for comparison.
        // After testing it make no difference whether numeric values are
        // compared or strings are compared. (There are other slower steps).
        if (numeric_mode) {
            long number = atol(node->value);
            sprintf(node->value, "%020ld", number);
        }

        // For first (root) node we do a dummy insert to make sure struct
        // has been initialised properly
        insertNode(root, node);

        if (i == 0) {
            // Now set the root node to the first position in the pool
            root = node;
        }
        // When the range of sort values is small the tree becomes unbalanced
        // resulting in *increadibly* slow sorts.
        // We'll periodically rebalance just in case.
        else if (i % 10000 == 0) {
            root = rebalanceTree(root, i + 1);
        }
    }

    // debugTree(root);

    // Walk tree writing result_rowids array
    if (direction == ORDER_ASC) {
        sort_walkTree(root, source_list, target_list);
    }
    else {
        sort_walkTreeBackwards(root, source_list, target_list);
    }

    free(pool);
}

/**
 * @brief Sort row_list based on multiple columns.
 * Still a naive implementation
 *
 * @param q
 * @param columns
 * @param column_count
 * @param direction
 * @param source_list
 * @param target_list
 */
void sortResultRowsMultiple (struct Query *q, struct ColumnNode *columns, int column_count, int *sort_directions, RowListIndex source_list, RowListIndex target_list) {
    int join_count = getRowList(source_list)->join_count;
    int row_count = getRowList(source_list)->row_count;

    RowListIndex row_lists[2];
    row_lists[0] = createRowList(join_count, row_count);
    row_lists[1] = createRowList(join_count, row_count);

    for (int i = column_count - 1; i >= 0 ; i--) {

        if (i < column_count - 1 && sort_directions[i] == ORDER_DESC) {
            // If there are sort steps lower in precedence, and this one is
            // DESC then the results need to be flipped first.

            reverseRowList(getRowList(target_list), -1);
        }

        RowListIndex from = (i == column_count - 1) ? source_list : row_lists[i%2];
        RowListIndex to = (i == 0) ? target_list : row_lists[(i+1)%2];

        sortResultRows(q, &columns[i], sort_directions[i], getRowList(from), getRowList(to));
    }
}

static void sort_walkTree (struct TreeNode *node, struct RowList * source_list, struct RowList * target_list) {

    if (node->left != NULL) {
        sort_walkTree(node->left, source_list, target_list);
    }

    // fprintf(stderr, "Write row %d (rowid: %d)\n", node->index, node->rowid);

    copyResultRow(target_list, source_list, node->key);

    if (node->right != NULL) {
        sort_walkTree(node->right, source_list, target_list);
    }
}

static void sort_walkTreeBackwards (struct TreeNode *node, struct RowList * source_list, struct RowList * target_list) {
    if (node->right != NULL) {
        sort_walkTreeBackwards(node->right, source_list, target_list);
    }

    copyResultRow(target_list, source_list, node->key);

    if (node->left != NULL) {
        sort_walkTreeBackwards(node->left, source_list, target_list);
    }
}
