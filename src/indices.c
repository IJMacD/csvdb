#include <stdlib.h>
#include <string.h>

#include "indices.h"
#include "predicates.h"
#include "limits.h"
#include "query.h"
#include "sort.h"
#include "util.h"

int indexWalkForValue (struct DB *db, int rowid_column, int value_column, int operator, const char *search_value, struct RowList * row_list);

int primaryKeyScan (struct DB *db, const char *predicate_field, int predicate_op, const char *predicate_value, struct RowList *row_list) {
    // If we have a primary key predicate then we can binary search
    int pk_index = pkSearch(db, predicate_field, predicate_value);

    if (predicate_op == OPERATOR_EQ) {
        if (pk_index == RESULT_NO_ROWS) {
            return 0;
        }

        writeRowID(row_list, 0, 0, pk_index);
        row_list->row_count = 1;

        return row_list->row_count;
    }
    else {
        if (pk_index == RESULT_NO_ROWS) {
            if (predicate_op == OPERATOR_NE) {
                // If the predicate was NE then that's exactly waht we're looking for
                // We'll return an array 0..N
                row_list->row_count = indexWalk(db, -1, 0, db->record_count, row_list);
                return row_list->row_count;
            }
            else {
                // Slow implementation limitation

                // We'll walk the entire index looking for the value
                // It'll take a long time but the caller might be relying on an ordered output

                int predicate_field_index = getFieldIndex(db, predicate_field);
                row_list->row_count = indexWalkForValue(db, -1, predicate_field_index, predicate_op, predicate_value, row_list);
                return row_list->row_count;
            }
        }

        int lower_index = pk_index;
        int upper_index = pk_index + 1;

        if (pk_index == RESULT_BELOW_MIN) {
            lower_index = 0;
            upper_index = 0;
        } else if (pk_index == RESULT_ABOVE_MAX) {
            lower_index = db->record_count;
            upper_index = db->record_count;
        }

        rangeScan(db, predicate_op, lower_index, upper_index, -1, row_list);
        return row_list->row_count;
    }
}

/**
 * @return matched row count; RESULT_NO_ROWS if row not found; RESULT_NO_INDEX if index does not exist
 */
int indexUniqueScan (struct DB *db, const char *predicate_field, int predicate_op, const char *predicate_value, struct RowList *row_list) {
    // We can't handle LIKE with this index (yet)
    if (predicate_op == OPERATOR_LIKE) {
        return RESULT_NO_INDEX;
    }

    // If we have a unique index predicate then we can binary search

    int pk_search_result = pkSearch(db, predicate_field, predicate_value);

    if (pk_search_result == RESULT_NO_ROWS) {
        return 0;
    }

    return rangeScan(db, predicate_op, pk_search_result, pk_search_result + 1, 1, row_list);
}

/**
 * @param predicate_value If set to NULL then the whole index will be walked in the ascending order
 * @return number of matched rows; RESULT_NO_INDEX if index does not exist
 */
int indexRangeScan (struct DB *index_db, const char *predicate_field, int predicate_op, const char *predicate_value, struct RowList * row_list) {
    // We can't handle LIKE with this index (yet)
    if (predicate_op == OPERATOR_LIKE) {
        return RESULT_NO_INDEX;
    }

    char value[VALUE_MAX_LENGTH] = {0};

    // If the value is NULL then we're being asked to walk the entire index
    if (strlen(predicate_value) == 0) {
        return indexWalk(index_db, 1, 0, index_db->record_count, row_list);
    }

    int record_index = pkSearch(index_db, predicate_field, predicate_value);

    int lower_index = record_index;
    int upper_index = record_index;

    if (record_index < 0) {
        if (predicate_op == OPERATOR_EQ) {
            return RESULT_NO_ROWS;
        }

        if (predicate_op == OPERATOR_NE) {
            lower_index = 0;
            upper_index = index_db->record_count;
        }
        else if (record_index == RESULT_BELOW_MIN) {
            lower_index = 0;
            upper_index = 0;
        }
        else if (record_index == RESULT_ABOVE_MAX) {
            lower_index = index_db->record_count;
            upper_index = index_db->record_count;
        }
        else if (record_index == RESULT_NO_ROWS) {
            // Slow implementation limitation

            // We'll walk the entire index looking for the value
            // It'll take a long time but the caller might be relying on an ordered output

            return indexWalkForValue(index_db, 1, 0, predicate_op, predicate_value, row_list);
        }
    }

    // Backtrack until we find the first value
    while (lower_index >= 0) {
        getRecordValue(index_db, --lower_index, 0, value, VALUE_MAX_LENGTH);

        if (strcmp(value, predicate_value) != 0) {
            break;
        }
    }
    lower_index++;

    // Forward-track until we find the last value
    while (upper_index < index_db->record_count) {
        getRecordValue(index_db, ++upper_index, 0, value, VALUE_MAX_LENGTH);

        if (strcmp(value, predicate_value) != 0) {
            break;
        }
    }

    return rangeScan(index_db, predicate_op, lower_index, upper_index, 1, row_list);
}

int rangeScan (struct DB *db, int predicate_op, int lower_index, int upper_index, int rowid_column, struct RowList * row_list) {
    if (predicate_op == OPERATOR_LT) {
        upper_index = lower_index;
        lower_index = 0;
    } else if (predicate_op == OPERATOR_LE) {
        lower_index = 0;
    } else if (predicate_op == OPERATOR_GT) {
        lower_index = upper_index;
        upper_index = db->record_count;
    } else if (predicate_op == OPERATOR_GE) {
        upper_index = db->record_count;
    }

    // We've found the edges of the index range; now iterate as predicate operator requires

    // Special treatment for NOT EQUAL; do the two halves separately
    if (predicate_op == OPERATOR_NE) {
        indexWalk(db, rowid_column, 0, lower_index, row_list);
        indexWalk(db, rowid_column, upper_index, db->record_count, row_list);

        return row_list->row_count;
    }

    // Walk between the bounds we've set up
    indexWalk(db, rowid_column, lower_index, upper_index, row_list);
    return row_list->row_count;
}

int indexWalk(struct DB *db, int rowid_column, int lower_index, int upper_index, struct RowList * row_list) {
    char value[VALUE_MAX_LENGTH];

    // Always ascending
    for (int i = lower_index; i < upper_index; i++) {
        if (rowid_column == -1) {
            appendRowID(row_list, i);
        } else {
            getRecordValue(db, i, rowid_column, value, VALUE_MAX_LENGTH);
            appendRowID(row_list, atoi(value));
        }
    }

    return row_list->row_count;
}

int indexWalkForValue (struct DB *db, int rowid_column, int value_column, int operator, const char *search_value, struct RowList * row_list) {
    char value[VALUE_MAX_LENGTH];
    char rowid_value[VALUE_MAX_LENGTH];

    for (int i = 0; i < db->record_count; i++) {
        getRecordValue(db, i, value_column, value, VALUE_MAX_LENGTH);

        int rowid = i;
        if (rowid_column >= 0) {
            getRecordValue(db, i, rowid_column, rowid_value, VALUE_MAX_LENGTH);
            rowid = atoi(rowid_value);
        }

        int result = compareValues(value, search_value);

        if (result < 0) {
            if (operator & OPERATOR_LT) {
                appendRowID(row_list, rowid);
            }
        }
        else if (result == 0) {
            if (operator & OPERATOR_EQ) {
                appendRowID(row_list, rowid);
            }
        }
        else {
            if (operator & OPERATOR_GT) {
                appendRowID(row_list, rowid);
            }
            else {
                // We've come the the end of useful values
                break;
            }
        }

    }

    return row_list->row_count;
}