#include <stdlib.h>
#include <string.h>

#include "structs.h"

#include "db-csv.h"
#include "db-csv-mem.h"
#include "db-csv-mmap.h"
#include "db-calendar.h"
#include "db-sequence.h"
#include "db-sample.h"
#include "db-dir.h"
#include "db-view.h"
#include "predicates.h"
#include "evaluate.h"
#include "result.h"
#include "db.h"
#include "util.h"
#include "function.h"

static int evaluateTableNode (char * value, struct DB *db, struct ColumnNode *column, int rowid);

static int evaluateTableField (char * output, struct DB *db, struct Field *field, int row_id);

struct VFS VFS_Table[VFS_COUNT] = {
    {
        0
    },
    {
        .openDB = &csv_openDB,
        .closeDB = &csv_closeDB,
        .getFieldIndex = &csv_getFieldIndex,
        .getFieldName = &csv_getFieldName,
        .getRecordCount = &csv_getRecordCount,
        .getRecordValue = &csv_getRecordValue,
        .findIndex = &csv_findIndex,
    },
    {
        .openDB = &csvMem_openDB,
        .closeDB = &csvMem_closeDB,
        .getFieldIndex = &csvMem_getFieldIndex,
        .getFieldName = &csvMem_getFieldName,
        .getRecordCount = &csvMem_getRecordCount,
        .getRecordValue = &csvMem_getRecordValue,
    },
    {
        .openDB = &view_openDB,
    },
    {
        .openDB = &calendar_openDB,
        .closeDB = &calendar_closeDB,
        .getFieldIndex = &calendar_getFieldIndex,
        .getFieldName = &calendar_getFieldName,
        .getRecordCount = &calendar_getRecordCount,
        .getRecordValue = &calendar_getRecordValue,
        .findIndex = &calendar_findIndex,
        .fullTableScan = &calendar_fullTableScan,
        .indexSearch = &calendar_indexSearch,
    },
    {
        .openDB = &sequence_openDB,
        .getFieldIndex = &sequence_getFieldIndex,
        .getFieldName = &sequence_getFieldName,
        .getRecordCount = &sequence_getRecordCount,
        .getRecordValue = &sequence_getRecordValue,
    },
    {
        .openDB = &sample_openDB,
        .getFieldIndex = &sample_getFieldIndex,
        .getFieldName = &sample_getFieldName,
        .getRecordCount = &sample_getRecordCount,
        .getRecordValue = &sample_getRecordValue,
    },
    {
        .openDB = &dir_openDB,
        .closeDB = &dir_closeDB,
        .getFieldIndex = &dir_getFieldIndex,
        .getFieldName = &dir_getFieldName,
        .getRecordCount = &dir_getRecordCount,
        .getRecordValue = &dir_getRecordValue,
    },
    {
        .openDB = &csvMmap_openDB,
        .closeDB = &csvMmap_closeDB,
        .getFieldIndex = &csvMmap_getFieldIndex,
        .getFieldName = &csvMmap_getFieldName,
        .getRecordCount = &csvMmap_getRecordCount,
        .getRecordValue = &csvMmap_getRecordValue,
    },
};

/**
 * @brief Try to open a database by filename
 *
 * @return 0 on success, -1 on failure
 */
int openDB (struct DB *db, const char *filename) {
    // Process explicit CSV_MEM first
    if (strncmp(filename, "memory:", 7) == 0) {
        return csvMem_openDB(db, filename + 7);
    }

    // Need to skip CSV and CSV_MEM for now because they are not very picky and
    // will attempt to find files with the same name, which might not be what
    // we want.
    for (int i = VFS_VIEW; i < VFS_COUNT; i++) {
        int (*vfs_openDB) (struct DB *, const char *filename) = VFS_Table[i].openDB;

        if (vfs_openDB != NULL) {
            int result = vfs_openDB(db, filename);

            if (result == 0) {
                return 0;
            }
        }
    }

    // None of the other VFSs wanted to open the file
    // Fallback to CSV
    return csv_openDB(db, filename);
}

void closeDB (struct DB *db) {
    if (db == NULL) {
        return;
    }

    void (*vfs_closeDB) (struct DB *) = VFS_Table[db->vfs].closeDB;

    if (vfs_closeDB != NULL) {
        vfs_closeDB(db);
    }
}

int getFieldIndex (struct DB *db, const char *field) {
    int (*vfs_getFieldIndex) (struct DB *, const char *) = VFS_Table[db->vfs].getFieldIndex;

    if (vfs_getFieldIndex != NULL) {
        return vfs_getFieldIndex(db, field);
    }

    return -1;
}

char *getFieldName (struct DB *db, int field_index) {
    char * (*vfs_getFieldName) (struct DB *, int) = VFS_Table[db->vfs].getFieldName;

    if (vfs_getFieldName != NULL) {
        return vfs_getFieldName(db, field_index);
    }

    return NULL;
}

int getRecordCount (struct DB *db) {
    int (*vfs_getRecordCount) (struct DB *) = VFS_Table[db->vfs].getRecordCount;

    if (vfs_getRecordCount != NULL) {
        return vfs_getRecordCount(db);
    }

    // Should we return -1 to indicate an error; or 0 to avoid breaking things?
    return 0;
}

/**
 * Returns the number of bytes read, or -1 on error
 */
int getRecordValue (struct DB *db, int record_index, int field_index, char *value, size_t value_max_length) {
    // A NULL rowid could come from a LEFT JOIN
    if (record_index == ROWID_NULL) {
        value[0] = '\0';
        return 0;
    }

    // This is the only field we can handle generically
    if (field_index == FIELD_ROW_INDEX) {
        return sprintf(value, "%d", record_index);
    }

    int (*vfs_getRecordValue) (struct DB *, int, int, char *, size_t) = VFS_Table[db->vfs].getRecordValue;

    if (vfs_getRecordValue != NULL) {
        return vfs_getRecordValue(db, record_index, field_index, value, value_max_length);
    }

    return -1;
}

/**
 * Searches for an index file with an explict name `x` i.e. UNIQUE(x) or INDEX(x) or
 * the autogenerated name `table__field`
 * Returns 0 on failure; 1 for a regular index, 2 for unique index
 *
 * @param db struct DB * OUT - Database to populate with index (Can be NULL)
 * @param table_name
 * @param index_name
 * @param index_type_flags INDEX_ANY|INDEX_REGULAR|INDEX_UNIQUE|INDEX_PRIMARY
 */
enum IndexSearchType findIndex(struct DB *db, const char *table_name, const char *index_name, int index_type_flags) {
    if (strcmp(table_name, "CALENDAR") == 0) {
        return calendar_findIndex(db, table_name, index_name, index_type_flags);
    }

    if (strncmp(table_name, "SEQUENCE(", 9) == 0) {
        return sequence_findIndex(db, table_name, index_name, index_type_flags);
    }

    return csv_findIndex(db, table_name, index_name, index_type_flags);
}

/**
 * @brief Scan table, filtering rows
 *
 * @return int number of matched rows
 */
int fullTableScan (struct DB *db, struct RowList * row_list, struct Predicate *predicates, int predicate_count, int limit_value) {
    if (db->vfs == 0) {
        fprintf(stderr, "Trying to scan unititialised DB\n");
        exit(-1);
    }

    int (*vfs_fullTableScan) (struct DB *, struct RowList *, struct Predicate *, int, int) = VFS_Table[db->vfs].fullTableScan;

    if (vfs_fullTableScan != NULL) {
        return vfs_fullTableScan(db, row_list, predicates, predicate_count, limit_value);
    }

    // VFS-agnostic implementation

    char value_left[MAX_VALUE_LENGTH] = {0};
    char value_right[MAX_VALUE_LENGTH] = {0};

    int result;
    int record_count = getRecordCount(db);

    for (int i = 0; i < record_count; i++) {
        int matching = 1;

        // Perform filtering if necessary
        for (int j = 0; j < predicate_count && matching; j++) {
            struct Predicate *predicate = predicates + j;

            // All fields in predicates MUST be on this table or constant

            result = evaluateTableNode(value_left, db, &predicate->left, i);
            if (result < 0) {
                return -1;
            }
            result = evaluateTableNode(value_right, db, &predicate->right, i);
            if (result < 0) {
                return -1;
            }

            if (!evaluateExpression(predicate->op, value_left, value_right)) {
                matching = 0;
                break;
            }
        }

        if (matching) {
            // Add to result set
            appendRowID(row_list, i);
        }

        // Implement early exit FETCH FIRST/LIMIT for cases with no ORDER clause
        if (limit_value >= 0 && row_list->row_count >= limit_value) {
            break;
        }
    }

    return row_list->row_count;
}

/**
 * A sort of dummy access function to just populate the result_rowids
 * array with all rowids in ascending numerical order.
 *
 * Equivalent to FULL TABLE SCAN with no predicates
 */
int fullTableAccess (struct DB *db, struct RowList * row_list, int limit_value) {
    if (db->vfs == 0) {
        fprintf(stderr, "Trying to access unititialised DB\n");
        exit(-1);
    }

    // VFS-agnostic implementation

    int record_count = getRecordCount(db);
    int l = record_count;
    if (limit_value >= 0 && limit_value < l) {
        l = limit_value;
    }

    for (int i = 0; i < l; i++) {
        appendRowID(row_list, i);
    }

    return l;
}

/**
 * @brief Special case of uniqueIndexSearch where [index rowid] = [table rowid]
 *
 * @return rowid, or RESULT_NO_ROWS (-1) if not found
 */
int pkSearch (struct DB *db, const char * value) {
    int output_flag;

    int rowid = uniqueIndexSearch(db, value, -1, &output_flag);

    if (output_flag) {
        return RESULT_NO_ROWS;
    }

    return rowid;
}

/**
 * @brief (Binary) Search a sorted table for a value and return associated rowid
 *
 * TODO: Just ignores rowid_field at the moment and assumes -1
 *
 * @param db must be an index with sorted column 0
 * @param value value to search for in cilumn 0 of db
 * @param rowid_field which column contains the rowid? (FIELD_ROW_INDEX means index rowid is returned)
 * @param mode MODE_UNIQUE: index is unique; MODE_LOWER_BOUND: return first matching rowid; MODE_UPPER_BOUND: return last matching rowid
 * @param output_flag 0: value found; RESULT_BETWEEN: value not found but would appear just before returned rowid; RESULT_BELOW_MIN: value below minimum; RESULT_ABOVE_MAX: value above maximum
 *
 * @returns rowid of match (or closest match); or RESULT_NO_ROWS (-1) if out of bounds;
 */
int indexSearch (struct DB *db, const char *search_value, int rowid_field, int mode, int * output_flag) {
    if (db->vfs == 0) {
        fprintf(stderr, "Trying to perform index search on unititialised DB\n");
        exit(-1);
    }

    int (*vfs_indexSearch) (struct DB *, const char *, int, int, int *) = VFS_Table[db->vfs].indexSearch;

    if (vfs_indexSearch != NULL) {
        return vfs_indexSearch(db, search_value, rowid_field, mode, output_flag);
    }

    // VFS-Agnostic implementation

    // By definition
    int index_column = 0;
    int record_count = getRecordCount(db);

    int index_a = 0;
    int index_b = record_count - 1;
    int index_match = -1;
    int numeric_mode = is_numeric(search_value);

    // Just in case we're in numeric mode
    long number_value = atol(search_value);

    char record_value[MAX_VALUE_LENGTH] = {0};

    // Check boundary cases before commencing search

    // Check lower boundary (index_a = 0)
    getRecordValue(db, index_a, index_column, record_value, MAX_VALUE_LENGTH);
    int res = compare(numeric_mode, search_value, number_value, record_value);

    // Search value is below minimum
    if (res < 0) {
        *output_flag = RESULT_BELOW_MIN;
        return RESULT_NO_ROWS;
    }

    // Found a match at lower boundary
    if (res == 0) {
        index_match = 0;
    }
    else {
        // Check upper boundary (index_b = record_count - 1)
        getRecordValue(db, index_b, index_column, record_value, MAX_VALUE_LENGTH);
        res = compare(numeric_mode, search_value, number_value, record_value);

        // Search value is above maximum
        if (res > 0) {
            *output_flag = RESULT_ABOVE_MAX;
            return RESULT_NO_ROWS;
        }

        // Found a match at upper boundary
        if (res == 0) {
            index_match = index_b;
        }
        // Perform binary search
        else while (index_a < index_b - 1) {
            int index_curr = (index_a + index_b) / 2;

            getRecordValue(db, index_curr, index_column, record_value, MAX_VALUE_LENGTH);
            res = compare(numeric_mode, search_value, number_value, record_value);

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

        // We didn't find the value but it should be inbetween index_a and index_b
        if (index_match < 0) {
            *output_flag = RESULT_BETWEEN;
            return index_b;
        }
    }

    // If we're down here we must have found the value in the index

    // We've been told index is unique so we're done
    if (mode == MODE_UNIQUE) {
        *output_flag = RESULT_FOUND;
        return index_match;
    }

    // Should we walk backwards to first instance of value?
    if (mode == MODE_LOWER_BOUND) {
        // Backtrack until we find the first value
        while (index_match >= 0) {
            getRecordValue(db, --index_match, index_column, record_value, MAX_VALUE_LENGTH);

            if (strcmp(record_value, search_value) < 0) {
                break;
            }
        }

        *output_flag = RESULT_FOUND;
        return index_match + 1;
    }

    // Should we walk forwards to last instance of value?
    if (mode == MODE_UPPER_BOUND) {
        int record_count = getRecordCount(db);

        // Forward-track until we find the last value
        while (index_match < record_count) {
            getRecordValue(db, ++index_match, index_column, record_value, MAX_VALUE_LENGTH);

            if (strcmp(record_value, search_value) > 0) {
                break;
            }
        }

        *output_flag = RESULT_FOUND;
        return index_match - 1;
    }

    fprintf(stderr, "Errr, I don't think we should be here\n");
    return -1;
}


/**
 * @brief Search a sorted table for a value and return associated rowid
 *
 * @param db must be an index with sorted column 0
 * @param value value to search for in column 0 of db
 * @param rowid_field which column contains the rowid? (-1 means index rowid is returned)
 * @param output_flag 0: value found; 1: value not found but just before returned rowid; 2: value below minimum; 3: value above maximum
 * @returns 0.. rowid of match; -1 if not found;
 */
int uniqueIndexSearch (struct DB *db, const char * value, int rowid_field, int * output_flag) {
    return indexSearch(db, value, rowid_field, 0, output_flag);
}

/**
 * @brief Like evaluateNode() but with the restriction that all fields must be
 * constant or fields on this table
 *
 * @return int
 */
static int evaluateTableNode (char * output, struct DB *db, struct ColumnNode *column, int row_id) {
    struct Field *field1 = &(column->fields[0]);
    struct Field *field2 = &(column->fields[1]);

    char value1[MAX_VALUE_LENGTH] = {0};
    char value2[MAX_VALUE_LENGTH] = {0};

    char *values[2];
    values[0] = value1;
    values[1] = value2;

    int result;

    result = evaluateTableField(value1, db, field1, row_id);
    if (result < 0) {
        return -1;
    }

    if (column->function == FUNC_UNITY) {
        strcpy(output, value1);
        return strlen(output);
    }

    result = evaluateTableField(value2, db, field2, row_id);
    if (result < 0) {
        return -1;
    }

    return evaluateFunction(output, column->function, values, 2);
}
/**
 * @brief Like evaluateField() but only operates on a single table
 *
 * @param output
 * @param tables
 * @param rowlist
 * @param field
 * @param result_index
 * @return int number of chars written
 */
static int evaluateTableField (char * output, struct DB *db, struct Field *field, int row_id) {

    if (field->index == FIELD_CONSTANT) {
        return evaluateConstantField(output, field);
    }

    if (field->table_id < 0) {
        return 0;
    }

    if (field->index == FIELD_ROW_INDEX) {
        return sprintf(output, "%d", row_id);
    }

    return getRecordValue(db, row_id, field->index, output, MAX_VALUE_LENGTH) > 0;
}