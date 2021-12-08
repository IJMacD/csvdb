#include <stdlib.h>
#include <string.h>

#include "indices.h"
#include "predicates.h"
#include "limits.h"
#include "query.h"
#include "sort.h"
#include "util.h"

int indexWalkForValue (struct DB *db, int rowid_column, int value_column, int operator, const char *search_value, int result_rowids[]);

int primaryKeyScan (struct DB *db, const char *predicate_field, int predicate_op, const char *predicate_value, int *result_rowids) {
    // If we have a primary key predicate then we can binary search
    int predicate_field_index = getFieldIndex(db, predicate_field);
    int pk_index = pk_search(db, predicate_field_index, predicate_value, FIELD_ROW_INDEX);

    if (predicate_op == OPERATOR_EQ) {
        if (pk_index == RESULT_NO_ROWS) {
            return 0;
        }

        result_rowids[0] = pk_index;
        return 1;
    }
    else {
        if (pk_index == RESULT_NO_ROWS) {
            if (predicate_op == OPERATOR_NE) {
                // If the predicate was NE then that's exactly waht we're looking for
                // We'll return an array 0..N
                return indexWalk(db, -1, 0, db->record_count, result_rowids);
            }
            else {
                // Slow implementation limitation

                // We'll walk the entire index looking for the value
                // It'll take a long time but the caller might be relying on an ordered output

                return indexWalkForValue(db, -1, predicate_field_index, predicate_op, predicate_value, result_rowids);
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

        return rangeScan(db, predicate_op, lower_index, upper_index, -1, result_rowids);
    }
}

/**
 * @return matched row count; RESULT_NO_ROWS if row not found; RESULT_NO_INDEX if index does not exist
 */
int indexUniqueScan (const char *table, const char *predicate_field, int predicate_op, const char *predicate_value, int *result_rowids) {
    // We can't handle LIKE with this index (yet)
    if (predicate_op == OPERATOR_LIKE) {
        return RESULT_NO_INDEX;
    }

    int result = RESULT_NO_INDEX;

    // If we have a unique index predicate then we can binary search
    struct DB index_db;

    if (findIndex(&index_db, table, predicate_field, INDEX_UNIQUE) == 0) {
        int pk_search_result = pk_search(&index_db, 0, predicate_value, FIELD_ROW_INDEX);

        if (pk_search_result != RESULT_NO_ROWS) {
            result = rangeScan(&index_db, predicate_op, pk_search_result, pk_search_result + 1, 1, result_rowids);
        }

        closeDB(&index_db);
    }

    return result;
}

/**
 * @param predicate_value If set to NULL then the whole index will be walked in the ascending order
 * @return number of matched rows; RESULT_NO_INDEX if index does not exist
 */
int indexRangeScan (const char *table, const char *predicate_field, int predicate_op, const char *predicate_value, int *result_rowids) {
    // We can't handle LIKE with this index (yet)
    if (predicate_op == OPERATOR_LIKE) {
        return RESULT_NO_INDEX;
    }

    int result = RESULT_NO_INDEX;

    // If we have a (non-unique) index predicate then we can binary search and scan
    struct DB index_db;

    char value[VALUE_MAX_LENGTH] = {0};

    if (findIndex(&index_db, table, predicate_field, INDEX_ANY) == 0) {

        // If the value is NULL then we're being asked to walk the entire index
        if (strlen(predicate_value) == 0) {
            int result = indexWalk(&index_db, 1, 0, index_db.record_count, result_rowids);
            closeDB(&index_db);
            return result;
        }

        int record_index = pk_search(&index_db, 0, predicate_value, FIELD_ROW_INDEX);

        int lower_index = record_index;
        int upper_index = record_index;

        if (record_index < 0) {
            if (predicate_op == OPERATOR_EQ) {
                closeDB(&index_db);
                return RESULT_NO_ROWS;
            }

            if (predicate_op == OPERATOR_NE) {
                lower_index = 0;
                upper_index = index_db.record_count;
            }
            else if (record_index == RESULT_BELOW_MIN) {
                lower_index = 0;
                upper_index = 0;
            }
            else if (record_index == RESULT_ABOVE_MAX) {
                lower_index = index_db.record_count;
                upper_index = index_db.record_count;
            }
            else if (record_index == RESULT_NO_ROWS) {
                // Slow implementation limitation

                // We'll walk the entire index looking for the value
                // It'll take a long time but the caller might be relying on an ordered output

                int result_count = indexWalkForValue(&index_db, 1, 0, predicate_op, predicate_value, result_rowids);
                closeDB(&index_db);
                return result_count;
            }
        }

        // Backtrack until we find the first value
        while (lower_index >= 0) {
            getRecordValue(&index_db, --lower_index, 0, value, VALUE_MAX_LENGTH);

            if (strcmp(value, predicate_value) != 0) {
                break;
            }
        }
        lower_index++;

        // Forward-track until we find the last value
        while (upper_index < index_db.record_count) {
            getRecordValue(&index_db, ++upper_index, 0, value, VALUE_MAX_LENGTH);

            if (strcmp(value, predicate_value) != 0) {
                break;
            }
        }

        result = rangeScan(&index_db, predicate_op, lower_index, upper_index, 1, result_rowids);

        closeDB(&index_db);
    }

    return result;
}

/**
 * @returns 0.. rowid of match; -1 if not found; -3 if below minimum; -4 if above maximum
 */
int pk_search(struct DB *db, int pk_index, const char *value, int result_index) {
    int index_a = 0;
    int index_b = db->record_count - 1;
    int index_match = -1;
    int numeric_mode = is_numeric(value);

    long search_value = atol(value);

    char val[VALUE_MAX_LENGTH] = {0};

    // Check boundary cases before commencing search

    // Check lower boundary
    getRecordValue(db, index_a, pk_index, val, VALUE_MAX_LENGTH);
    int res = compare(numeric_mode, value, search_value, val);

    // Search value is below minimum
    if (res < 0) {
        return -3;
    }

    // Found a match at lower boundary
    if (res == 0)
        index_match = index_a;

    else {
        // Check upper boundary
        getRecordValue(db, index_b, pk_index, val, VALUE_MAX_LENGTH);
        res = compare(numeric_mode, value, search_value, val);

        // Search value is above maximum
        if (res > 0) {
            return -4;
        }

        // Found a match at upper boundary
        if (res == 0)
            index_match = index_b;

        // Iterate as a binary search
        else while (index_a < index_b - 1) {
            int index_curr = (index_a + index_b) / 2;

            getRecordValue(db, index_curr, pk_index, val, VALUE_MAX_LENGTH);
            res = compare(numeric_mode, value, search_value, val);

            if (res == 0) {
                // printf("pk_search [%d   <%d>   %d]: %s\n", index_a, index_curr, index_b, val);
                index_match = index_curr;
                break;
            }

            if (res > 0) {
                // printf("pk_search [%d   (%d) x %d]: %s\n", index_a, index_curr, index_b, val);
                index_a = index_curr;

            } else {
                // printf("pk_search [%d x (%d)   %d]: %s\n", index_a, index_curr, index_b, val);
                index_b = index_curr;
            }
        }
    }

    if (index_match < 0) {
        return -1;
    }

    // If the requested result column was this special value
    // we return the row index as the result
    if (result_index == FIELD_ROW_INDEX) {
        return index_match;
    }

    // Otherwise fetch the value for the requested column and convert to an int
    if (getRecordValue(db, index_match, result_index, val, VALUE_MAX_LENGTH) > 0) {
        return atoi(val);
    }

    return -1;
}

int rangeScan (struct DB *db, int predicate_op, int lower_index, int upper_index, int rowid_column, int *result_rowids) {
    int result_count = 0;

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
        result_count = indexWalk(db, rowid_column, 0, lower_index, result_rowids);
        result_count += indexWalk(db, rowid_column, upper_index, db->record_count, result_rowids + result_count);

        return result_count;
    }

    // Walk between the bounds we've set up
    return indexWalk(db, rowid_column, lower_index, upper_index, result_rowids);
}

int indexWalk(struct DB *db, int rowid_column, int lower_index, int upper_index, int *result_rowids) {
    int result_count = 0;
    char value[VALUE_MAX_LENGTH];

    // Always ascending
    for (int i = lower_index; i < upper_index; i++) {
        if (rowid_column == -1) {
            result_rowids[result_count++] = i;
        } else {
            getRecordValue(db, i, rowid_column, value, VALUE_MAX_LENGTH);
            result_rowids[result_count++] = atoi(value);
        }
    }

    return result_count;
}

int indexWalkForValue (struct DB *db, int rowid_column, int value_column, int operator, const char *search_value, int result_rowids[]) {
    int result_count = 0;
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
                result_rowids[result_count++] = rowid;
            }
        }
        else if (result == 0) {
            if (operator & OPERATOR_EQ) {
                result_rowids[result_count++] = rowid;
            }
        }
        else {
            if (operator & OPERATOR_GT) {
                result_rowids[result_count++] = rowid;
            }
            else {
                // We've come the the end of useful values
                break;
            }
        }

    }

    return result_count;
}