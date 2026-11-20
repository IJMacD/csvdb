#include <stdlib.h>
#include <string.h>

#include "../structs.h"
#include "db.h"
#include "../query/result.h"
#include "indices.h"

static int indexWalk(
    struct DB *db,
    int rowid_column,
    int lower_index,
    int upper_index,
    struct RowList * row_list
);

int indexRowidColumn (struct DB *index_db) {
    return index_db->field_count - 1;
}

/**
 * Just a unique seek but we know that [index rowid] == [table rowid]
 */
enum IndexSearchResult indexPrimarySeek (
    struct DB *db,
    enum Function predicate_op,
    const char *predicate_value,
    struct RowList * row_list,
    int limit
) {
    return indexUniqueSeek(
        db,
        FIELD_ROW_INDEX,
        predicate_op,
        predicate_value,
        row_list,
        limit
    );
}

/**
 * Just a non-unique seek but we know that [index rowid] == [table rowid].
 * Useful when the index is a covering index so we want the *index* rowid to be
 * returned.
 */
enum IndexSearchResult indexCoveringSeek (
    struct DB *db,
    enum Function predicate_op,
    const char *predicate_value,
    struct RowList * row_list,
    int limit
) {
    return indexSeek(
        db,
        FIELD_ROW_INDEX,
        predicate_op,
        predicate_value,
        row_list,
        limit
    );
}

/**
 * Pretty much duplicated from indexSeek but telling the underlying DB VFS that
 * the index is unique.
 * Also: only need to do one index search, rather than two.
 */
enum IndexSearchResult indexUniqueSeek (
    struct DB *index_db,
    int rowid_column,
    enum Function predicate_op,
    const char *predicate_value,
    struct RowList * row_list,
    int limit
) {
    // Prefix LIKE is a range; reuse the non-unique seek path.
    if (predicate_op == OPERATOR_LIKE) {
        return indexSeek(
            index_db,
            rowid_column,
            predicate_op,
            predicate_value,
            row_list,
            limit);
    }

    // Output Flag: 0: value found; RESULT_BETWEEN: value not found but just
    // before returned rowid; RESULT_BELOW_MIN: value below minimum;
    // RESULT_ABOVE_MAX: value above maximum
    int search_status;

    // (inclusive)
    int lower_bound;
    // (exclusive)
    int upper_bound;

    if (predicate_op == OPERATOR_ALWAYS)
    {
        // Should be a SCAN not a SEEK but we can deal with it anyway
        lower_bound = 0;
        upper_bound = getRecordCount(index_db);
    }
    else
    {
        int index_rowid = indexSearch(
            index_db,
            predicate_value,
            MODE_UNIQUE,
            &search_status);

        if (predicate_op == OPERATOR_EQ && search_status)
        {
            // We want an exact match but value is not in index
            // Just bail out now
            return RESULT_NO_ROWS;
        }

        if (predicate_op == OPERATOR_EQ)
        {
            lower_bound = index_rowid;
            upper_bound = index_rowid + 1;
        }
        else if (predicate_op == OPERATOR_LT || predicate_op == OPERATOR_LE)
        {
            lower_bound = 0;
            upper_bound = index_rowid;

            if (predicate_op == OPERATOR_LE && search_status == RESULT_FOUND)
            {
                upper_bound++;
            }
        }
        else if (predicate_op == OPERATOR_GT || predicate_op == OPERATOR_GE)
        {
            lower_bound = index_rowid;
            upper_bound = getRecordCount(index_db);

            if (predicate_op == OPERATOR_GT && search_status == RESULT_FOUND)
            {
                lower_bound++;
            }
        }
        // Special treatment for NOT EQUAL; do the two halves separately
        else if (predicate_op == OPERATOR_NE)
        {
            int record_count = getRecordCount(index_db);

            int a = indexWalk(index_db, rowid_column, 0, index_rowid, row_list);
            int b = indexWalk(
                index_db,
                rowid_column,
                index_rowid + 1,
                record_count,
                row_list);

            return a + b;
        }
        else
        {
            fprintf(
                stderr,
                "Not Implemented: Unique Index range scan for operator: %d\n",
                predicate_op);
            exit(-1);
        }
    }

    if (limit >= 0) {
        upper_bound = MIN(upper_bound, lower_bound + limit);
    }

    return indexWalk(
        index_db,
        rowid_column,
        lower_bound,
        upper_bound,
        row_list
    );
}

/**
 * @brief Scan the entire index writing each rowid to specified row_list
 *
 * @param index_db
 * @param rowid_column column containing the base-table rowid (last column;
 * use indexRowidColumn()); FIELD_ROW_INDEX means return the index's own
 * row number (e.g. Primary Key / covering)
 * @param row_list
 * @param limit
 *
 * @return number of matched rows; RESULT_NO_INDEX if index does not exist
 */
enum IndexSearchResult indexScan (
    struct DB *index_db,
    int rowid_column,
    struct RowList * row_list,
    int limit
) {
    // (inclusive)
    int lower_bound = 0;
    // (exclusive)
    int upper_bound = getRecordCount(index_db);

    if (limit >= 0) {
        upper_bound = MIN(upper_bound, lower_bound + limit);
    }

    return indexWalk(
        index_db,
        rowid_column,
        lower_bound,
        upper_bound,
        row_list
    );
}

/**
 * @brief Given an operator and a value, fill in rowids within the relevant
 * range in the order of the index
 *
 * @param index_db
 * @param rowid_column column containing the base-table rowid (last column;
 * use indexRowidColumn()); FIELD_ROW_INDEX means return the index's own
 * row number (e.g. Primary Key / covering)
 * @param predicate_op comparison operator;
 * @param predicate_value
 * @param row_list
 * @param limit
 *
 * @return number of matched rows; RESULT_NO_INDEX if index does not exist
 */
enum IndexSearchResult indexSeek (
    struct DB *index_db,
    int rowid_column,
    enum Function predicate_op,
    const char *predicate_value,
    struct RowList * row_list,
    int limit
) {
    // (inclusive)
    int lower_bound;
    // (exclusive)
    int upper_bound;

    char value[MAX_VALUE_LENGTH];
    strcpy(value, predicate_value);
    size_t len = strlen(value);

    if (predicate_op == OPERATOR_LIKE) {
        if (value[len - 1] != '%') {
            fprintf(
                stderr,
                "Cannot do INDEX SEEK on LIKE with without '%%' at the end\n"
            );
            exit(-1);
        }
        value[len - 1] = '\0';
    }

    // Find the upper and lower rowid for the index
    // (maybe we don't need both but it's quite cheap)

    // Output Flag: 0: value found; 1: value not found but just before returned
    // rowid; 2: value below minimum; 3: value above maximum
    int search_status1;
    int search_status2;

    enum IndexSearchResult lower_index_rowid = indexSearch(
        index_db,
        value,
        MODE_LOWER_BOUND,
        &search_status1
    );

    if (predicate_op == OPERATOR_EQ && search_status1) {
        // We want an exact match but value is not in index
        // Just bail out now
        return RESULT_NO_ROWS;
    }

    // For LIKE the upper bound search will use the next letter in the alphabet
    // e.g. M -> N
    if (predicate_op == OPERATOR_LIKE) {
        value[len - 2]++;
    }

    enum IndexSearchResult upper_index_rowid = indexSearch(
        index_db,
        value,
        MODE_UPPER_BOUND,
        &search_status2
    );

    if (predicate_op == OPERATOR_EQ) {
        lower_bound = lower_index_rowid;
        upper_bound = upper_index_rowid + 1;
    }
    else if (predicate_op == OPERATOR_LIKE) {
        if (search_status1 == RESULT_BELOW_MIN) {
            lower_bound = 0;
        } else {
            lower_bound = lower_index_rowid;
        }

        if (search_status2 == RESULT_ABOVE_MAX) {
            upper_bound = getRecordCount(index_db);
        }
        else {
            upper_bound = upper_index_rowid;
        }
    }
    else if (predicate_op == OPERATOR_LT) {
        lower_bound = 0;
        upper_bound = lower_index_rowid;
    }
    else if (predicate_op == OPERATOR_LE) {
        lower_bound = 0;
        upper_bound = upper_index_rowid;

        if (search_status2 == RESULT_FOUND) {
            upper_bound++;
        }
    }
    else if (predicate_op == OPERATOR_GT) {
        lower_bound = upper_index_rowid;

        if (search_status1 == RESULT_FOUND) {
            lower_bound++;
        }

        upper_bound = getRecordCount(index_db);
    }
    else if (predicate_op == OPERATOR_GE) {
        lower_bound = lower_index_rowid;
        upper_bound = getRecordCount(index_db);
    }
    // Special treatment for NOT EQUAL; do the two halves separately
    else if (predicate_op == OPERATOR_NE) {
        int record_count = getRecordCount(index_db);
        int a = indexWalk(
            index_db,
            rowid_column,
            0,
            lower_index_rowid,
            row_list
        );
        int b = indexWalk(
            index_db,
            rowid_column,
            upper_index_rowid,
            record_count,
            row_list
        );

        return a + b;
    }
    else {
        fprintf(
            stderr,
            "Not Implemented: Index seek for operator: %d\n",
            predicate_op
        );
        exit(-1);
    }

    if (limit >= 0) {
        upper_bound = MIN(upper_bound, lower_bound + limit);
    }

    return indexWalk(
        index_db,
        rowid_column,
        lower_bound,
        upper_bound,
        row_list
    );
}

/**
 * @brief Walk from lower to upper (exclusive) bounds adding rowids to a RowList
 *
 * @param db
 * Seek with both a lower and upper bound on the same index column.
 * Bound math mirrors indexSeek() for GE/GT/LE/LT.
 *
 * @param rowid_column last index column (indexRowidColumn()), or
 * FIELD_ROW_INDEX for the index's own row number
 */
enum IndexSearchResult indexBoundedSeek (
    struct DB *index_db,
    int rowid_column,
    enum Function lower_op,
    const char *lower_value,
    enum Function upper_op,
    const char *upper_value,
    struct RowList * row_list,
    int limit
) {
    int status_lo_lower = 0;
    int status_lo_upper = 0;
    int status_hi_lower = 0;
    int status_hi_upper = 0;

    int lo_lower = indexSearch(
        index_db, lower_value, MODE_LOWER_BOUND, &status_lo_lower);
    int lo_upper = indexSearch(
        index_db, lower_value, MODE_UPPER_BOUND, &status_lo_upper);
    int hi_lower = indexSearch(
        index_db, upper_value, MODE_LOWER_BOUND, &status_hi_lower);
    int hi_upper = indexSearch(
        index_db, upper_value, MODE_UPPER_BOUND, &status_hi_upper);

    int lower_bound = 0;
    int upper_bound = getRecordCount(index_db);

    if (lower_op == OPERATOR_GE)
    {
        lower_bound = lo_lower;
    }
    else if (lower_op == OPERATOR_GT)
    {
        lower_bound = lo_upper;
        if (status_lo_lower == RESULT_FOUND)
        {
            lower_bound++;
        }
    }

    if (upper_op == OPERATOR_LT)
    {
        upper_bound = hi_lower;
    }
    else if (upper_op == OPERATOR_LE)
    {
        upper_bound = hi_upper;
        if (status_hi_upper == RESULT_FOUND)
        {
            upper_bound++;
        }
    }

    if (lower_bound < 0)
    {
        lower_bound = 0;
    }
    if (upper_bound < 0)
    {
        upper_bound = 0;
    }
    if (upper_bound > getRecordCount(index_db))
    {
        upper_bound = getRecordCount(index_db);
    }
    if (lower_bound >= upper_bound)
    {
        return RESULT_NO_ROWS;
    }

    if (limit >= 0)
    {
        upper_bound = MIN(upper_bound, lower_bound + limit);
    }

    return indexWalk(
        index_db,
        rowid_column,
        lower_bound,
        upper_bound,
        row_list);
}

static int indexWalk(
    struct DB *db,
    int rowid_column,
    int lower_index,
    int upper_index,
    struct RowList * row_list
) {
    char value[MAX_VALUE_LENGTH];

    // Always ascending
    for (int i = lower_index; i < upper_index; i++) {
        if (rowid_column == FIELD_ROW_INDEX) {
            appendRowID(row_list, i);
        } else {
            int result = getRecordValue(
                db,
                i,
                rowid_column,
                value,
                MAX_VALUE_LENGTH
            );

            if (result < 0) {
                fprintf(stderr, "unable to get value for rowid %d\n", i);
                exit(-1);
            }

            appendRowID(row_list, atoi(value));
        }
    }

    return upper_index - lower_index;
}
