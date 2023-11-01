#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "../structs.h"
#include "../query/result.h"
#include "../evaluate/evaluate.h"
#include "../evaluate/predicates.h"
#include "../debug.h"

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
    // We might get a single AND node rather than a list
    if (predicate_count == 1 && predicates[0].function == OPERATOR_AND) {
        // Replace node list with our child list
        predicates = predicates[0].children;
        predicate_count = predicates[0].child_count;
    }

    // Inclusive
    // Rest of system can't cope with negative RowIDs
    int start = 0;
    // Exclusive
    int end = db->_record_count;

    int step = 1;
    int step_offset = 0;

    int have_unprocessed_predicates = 0;

    // Establish limits from predicates
    for (int i = 0; i < predicate_count; i++) {
        struct Node *predicate = &predicates[i];

        // Must be an operator
        if ((predicate->function & MASK_FUNC_FAMILY) != FUNC_FAM_OPERATOR) {
            continue;
        }

        enum Function op = predicate->function;

        struct Node *node_left = &predicate->children[0];
        struct Node *node_right = &predicate->children[1];

        struct Field *field_left = (struct Field *)node_left;
        struct Field *field_right = (struct Field *)node_right;

        // Prep: We need field on the left and constant on the right, swap if
        // necessary
        normalisePredicate(predicate);

        // We're only looking for constants (which are direct node constants)
        if (node_right->function != FUNC_UNITY) {
            continue;
        }

        if (node_left->function == FUNC_UNITY) {

            // This shouldn't be anything else but check just in case
            if (field_left->index != 0) {
                continue;
            }

            int value = atoi(field_right->text);

            if (op == OPERATOR_EQ) {
                start = MAX(start, value);
                end = MIN(end, value + 1);
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
        else if (node_left->function == FUNC_MOD) {
            struct Node *node_left_left = &node_left->children[0];
            struct Node *node_left_right = &node_left->children[1];

            // This shouldn't be anything else but check just in case
            if (node_left_left->field.index != 0) {
                continue;
            }

            step = atoi(node_left_right->field.text);

            step_offset = atoi(field_right->text);

            if (step_offset >= step) {
                // Impossible
                start = 0;
                end = 0;
            }
        }
        else {
            have_unprocessed_predicates = 1;
        }
    }

    int start_offset = (step - (start % step) + step_offset) % step;

    #ifdef DEBUG
    if (debug_verbosity >= 3) {
        fprintf(stderr, "[SEQUENCE] start: %d, end: %d, step: %d\n", start, end, step);
    }
    #endif

    if (end == db->_record_count) {
        fprintf(stderr, "Error: Unbounded SEQUENCE\n");
        exit(-1);
    }

    struct Table table;
    table.db = db;

    struct RowList *row_list = getRowList(list_id);
    int start_row_count = row_list->row_count;
    for (int i = start + start_offset; i < end; i += step) {
        if (limit_value >= 0 && row_list->row_count >= (unsigned)limit_value) {
            break;
        }

        if (have_unprocessed_predicates) {
            // If we have unprocessed predicates then this is necessary
            int matching = evaluateOperatorNodeListAND(
                &table,
                ROWLIST_ROWID,
                i,
                predicates,
                predicate_count
            );

            if (matching) {
                // Add to result set
                appendRowID(row_list, i);
            }
        }
        else {
            appendRowID(row_list, i);
        }
    }

    return row_list->row_count - start_row_count;
}
