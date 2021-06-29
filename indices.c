#include <stdlib.h>
#include <string.h>

#include "indices.h"
#include "predicates.h"
#include "limits.h"
#include "query.h"
#include "util.h"

int primaryKeyScan (struct DB *db, const char *predicate_field, char predicate_op, const char *predicate_value, int *result_rowids) {
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
            // We weren't able to use the index (implementation limitation)
            // So pretend the PK doesn't exist and fallback to slower implentation
            return RESULT_NO_INDEX;
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
int indexUniqueScan (const char *predicate_field, char predicate_op, const char *predicate_value, int *result_rowids) {
    // If we have a unique index predicate then we can binary search
    char index_filename[TABLE_MAX_LENGTH + 10];
    sprintf(index_filename, "%s.unique.csv", predicate_field);

    struct DB index_db;

    if (openDB(&index_db, index_filename) == 0) {
        int pk_search_result = pk_search(&index_db, 0, predicate_value, 1);

        if (pk_search_result == RESULT_NO_ROWS) {
            return RESULT_NO_ROWS;
        }

        return rangeScan(&index_db, predicate_op, pk_search_result, pk_search_result + 1, 1, result_rowids);
    }

    return RESULT_NO_INDEX;
}

/**
 * @return number of matched rows; RESULT_NO_INDEX if index does not exist
 */
int indexRangeScan (int *result_rowids, const char *predicate_field, char predicate_op, const char *predicate_value, int flags) {

    // If we have a (non-unique) index predicate then we can binary search and scan
    if (flags & FLAG_HAVE_PREDICATE) {
        char index_filename[TABLE_MAX_LENGTH + 10];
        sprintf(index_filename, "%s.index.csv", predicate_field);

        struct DB index_db;

        char value[VALUE_MAX_LENGTH];

        if (openDB(&index_db, index_filename) == 0) {
            int record_index = pk_search(&index_db, 0, predicate_value, FIELD_ROW_INDEX);

            int lower_index = record_index;
            int upper_index = record_index;

            if (record_index < 0) {
                if (predicate_op == OPERATOR_EQ) {
                    return RESULT_NO_ROWS;
                }

                if (record_index == RESULT_BELOW_MIN) {
                    lower_index = 0;
                    upper_index = 0;
                } else if (record_index == RESULT_ABOVE_MAX) {
                    lower_index = index_db.record_count;
                    upper_index = index_db.record_count;
                } else if (record_index == RESULT_NO_ROWS) {
                    // Implemetation limitiation
                    // Pretend the index doesn't exist
                    return RESULT_NO_INDEX;
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

            return rangeScan(&index_db, predicate_op, lower_index, upper_index, 1, result_rowids);
        }
    }

    return RESULT_NO_INDEX;
}

/**
 * @return number of matched rows
 */
int fullTableScan (struct DB *db, int *result_rowids, const char *predicate_field, char predicate_op, const char *predicate_value, int limit_value, int offset_value, int flags) {
    int result_count = 0;

    int predicate_field_index = getFieldIndex(db, predicate_field);

    for (int i = 0; i < db->record_count; i++) {
        // Perform filtering if necessary
        if (flags & FLAG_HAVE_PREDICATE) {
            char value[VALUE_MAX_LENGTH];
            getRecordValue(db, i, predicate_field_index, value, VALUE_MAX_LENGTH);

            if (!evaluateExpression(predicate_op, value, predicate_value)) {
                continue;
            }
        }

        // Add to result set
        result_rowids[result_count++] = i;

        // Implement early exit FETCH FIRST/LIMIT for cases with no ORDER clause
        if (!(flags & FLAG_ORDER) && limit_value >= 0 && (result_count - offset_value) >= limit_value) {
            break;
        }
    }

    return result_count;
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

    char val[VALUE_MAX_LENGTH];

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

int rangeScan (struct DB *db, char predicate_op, int lower_index, int upper_index, int rowid_column, int *result_rowids) {
    int result_count = 0;
    char value[VALUE_MAX_LENGTH];

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

    // Special treatment for NOT EQUAL
    if (predicate_op == OPERATOR_NE) {
        for (int i = 0; i < lower_index; i++) {
            if (rowid_column == -1) {
                result_rowids[result_count++] = i;
            } else {
                getRecordValue(db, i, rowid_column, value, VALUE_MAX_LENGTH);
                result_rowids[result_count++] = atoi(value);
            }
        }

        for (int i = upper_index; i < db->record_count; i++) {
            if (rowid_column == -1) {
                result_rowids[result_count++] = i;
            } else {
                getRecordValue(db, i, rowid_column, value, VALUE_MAX_LENGTH);
                result_rowids[result_count++] = atoi(value);
            }
        }

        return result_count;
    }

    // Iterate between the bounds we've set up
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