#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "../structs.h"
#include "../query/result.h"
#include "../evaluate/predicates.h"

int sequence_openDB (
    struct DB *db,
    const char *filename,
    __attribute__((unused)) char **resolved
) {
    if (strcmp(filename, "SEQUENCE") != 0) {
        return -1;
    }

    db->vfs = VFS_SEQUENCE;
    db->_record_count = 1e7;
    db->line_indices = NULL;
    db->field_count = 1;

    return 0;
}

void sequence_closeDB (__attribute__((unused)) struct DB *db) {}

int sequence_getFieldIndex (
    __attribute__((unused)) struct DB *db,
    const char *field
) {
    if (strcmp(field, "value") == 0) {
        return 0;
    }

    return -1;
}

char *sequence_getFieldName (
    __attribute__((unused)) struct DB *db,
    int field_index
) {
    if (field_index == 0)
        return "value";
    return "";
}

int sequence_getRecordCount (struct DB *db) {
    return db->_record_count;
}

int sequence_getRecordValue (
    __attribute__((unused)) struct DB *db,
    int record_index,
    __attribute__((unused)) int field_index,
    char *value,
    __attribute__((unused)) size_t value_max_length
) {
    return sprintf(value, "%d", record_index);
}

// All queries go through fullTableScan
enum IndexSearchType sequence_findIndex(
    __attribute__((unused)) struct DB *db,
    __attribute__((unused)) const char *table_name,
    __attribute__((unused)) struct Node *node,
    __attribute__((unused)) int index_type_flags,
    __attribute__((unused)) char **resolved
) {
    return INDEX_NONE;
}

/**
 * Guaranteed that all predicates are on this table
 *
 * @return int number of matched rows
 */
int sequence_fullTableAccess (
    struct DB *db,
    RowListIndex list_id,
    struct Node *predicates,
    int predicate_count,
    int limit_value
) {
    // Inclusive
    int start = 0;
    // Exclusive
    int end = db->_record_count;
    // TODO: parse mod predicate
    int step = 1;

    // Establish limits from predicates
    for (int i = 0; i < predicate_count; i++) {
        struct Node *predicate = &predicates[i];

        // Must be an operator
        if ((predicate->function & MASK_FUNC_FAMILY) != FUNC_FAM_OPERATOR) {
            continue;
        }

        enum Function op = predicate->function;

        struct Node *node_left = &predicate->children[0];

        struct Field *field_left = (struct Field *)node_left;
        struct Field *field_right = (struct Field *)&predicate->children[1];

        // Prep: We need field on the left and constant on the right, swap if
        // necessary
        normalisePredicate(predicate);

        // We're only looking for constants
        if (field_right->index != FIELD_CONSTANT) {
            continue;
        }

        if (node_left->function == FUNC_UNITY) {

            // This shouldn't be anything else but check just in case
            if (field_left->index != 0) {
                continue;
            }

            int value = atoi(field_right->text);

            if (op == OPERATOR_EQ) {
                start = value;
                end = value + 1;
            }
            else if (op == OPERATOR_LT) {
                end = MIN(end, value);
            }
            else if (op == OPERATOR_LE) {
                end = MIN(end, value + 1);
            }
            else if (op == OPERATOR_GT) {
                start = MAX(start, value + 1);
            }
            else if (op == OPERATOR_GE) {
                start = MAX(start, value);
            }
        }
    }

    struct RowList *row_list = getRowList(list_id);
    int start_row_count = row_list->row_count;
    for (int i = start; i < end; i += step) {
        if (limit_value >= 0 && row_list->row_count >= limit_value) {
            break;
        }

        appendRowID(row_list, i);
    }

    return row_list->row_count - start_row_count;
}
