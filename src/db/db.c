#include <stdlib.h>
#include <string.h>

#include "../structs.h"

#include "csv.h"
#include "csv-mem.h"
#include "csv-mmap.h"
#include "calendar.h"
#include "sequence.h"
#include "sample.h"
#include "dir.h"
#include "view.h"
#include "temp.h"
#include "../evaluate/predicates.h"
#include "../evaluate/evaluate.h"
#include "../query/result.h"
#include "db.h"
#include "../functions/util.h"
#include "../evaluate/function.h"

static int evaluateTableNode (
    struct DB *db,
    int row_id,
    struct Node *node,
    char * output
);

static int evaluateTableField (
    struct DB *db,
    int row_id,
    struct Field *field,
    char * output
);

struct VFS VFS_Table[VFS_COUNT] = {
    // 0 = VFS_NULL
    {
        0
    },
    // 1 = VFS_CSV
    {
        .openDB = &csv_openDB,
        .closeDB = &csv_closeDB,
        .getFieldIndex = &csv_getFieldIndex,
        .getFieldName = &csv_getFieldName,
        .getRecordCount = &csv_getRecordCount,
        .getRecordValue = &csv_getRecordValue,
        .findIndex = &csv_findIndex,
        .insertRow = &csv_insertRow,
        .insertFromQuery = &csv_insertFromQuery,
    },
    // 2 = VFS_CSV_MEM
    {
        .openDB = &csvMem_openDB,
        .closeDB = &csvMem_closeDB,
        .getFieldIndex = &csvMem_getFieldIndex,
        .getFieldName = &csvMem_getFieldName,
        .getRecordCount = &csvMem_getRecordCount,
        .getRecordValue = &csvMem_getRecordValue,
        .insertRow = &csvMem_insertRow,
    },
    // 3 = VFS_VIEW
    {
        .openDB = &view_openDB,
    },
    // 4 = VFS_CALENDAR
    {
        .openDB = &calendar_openDB,
        .closeDB = &calendar_closeDB,
        .getFieldIndex = &calendar_getFieldIndex,
        .getFieldName = &calendar_getFieldName,
        .getRecordCount = &calendar_getRecordCount,
        .getRecordValue = &calendar_getRecordValue,
        .findIndex = &calendar_findIndex,
        .fullTableAccess = &calendar_fullTableAccess,
        .indexSearch = &calendar_indexSearch,
    },
    // 5 = VFS_SEQUENCE
    {
        .openDB = &sequence_openDB,
        .getFieldIndex = &sequence_getFieldIndex,
        .getFieldName = &sequence_getFieldName,
        .getRecordCount = &sequence_getRecordCount,
        .getRecordValue = &sequence_getRecordValue,
    },
    // 6 = VFS_SAMPLE
    {
        .openDB = &sample_openDB,
        .getFieldIndex = &sample_getFieldIndex,
        .getFieldName = &sample_getFieldName,
        .getRecordCount = &sample_getRecordCount,
        .getRecordValue = &sample_getRecordValue,
    },
    // 7 = VFS_DIR
    {
        .openDB = &dir_openDB,
        .closeDB = &dir_closeDB,
        .getFieldIndex = &dir_getFieldIndex,
        .getFieldName = &dir_getFieldName,
        .getRecordCount = &dir_getRecordCount,
        .getRecordValue = &dir_getRecordValue,
    },
    // 8 = VFS_CSV_MMAP
    {
        .openDB = &csvMmap_openDB,
        .closeDB = &csvMmap_closeDB,
        .getFieldIndex = &csvMmap_getFieldIndex,
        .getFieldName = &csvMmap_getFieldName,
        .getRecordCount = &csvMmap_getRecordCount,
        .getRecordValue = &csvMmap_getRecordValue,
    },
    // 9 = VFS_TEMP
    {
        .openDB = &temp_openDB,
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
        int (*vfs_openDB) (struct DB *, const char *filename)
            = VFS_Table[i].openDB;

        if (vfs_openDB != NULL) {
            int result = vfs_openDB(db, filename);

            if (result == 0) {
                return 0;
            }
        }
    }

    // None of the other VFSs wanted to open this DB.
    // Now we'll determine whether to use VFS_CSV or VFS_CSV_MEM.

    FILE *f;

    if (strcmp(filename, "stdin") == 0) {
        f = stdin;
    }
    else {
        f = fopen(filename, "r");
    }

    if (!f) {
        char buffer[FILENAME_MAX];
        sprintf(buffer, "%s.csv", filename);
        f = fopen(buffer, "r");

        if (!f) {
            return -1;
        }
    }

    // Try to seek to see if we have a stream
    if (fseek(f, 0, SEEK_SET)) {
        // File is not seekable
        // Must use VFS_CSV_MEM
        int result = csvMem_makeDB(db, f);
        fclose(f);
        return result;
    }

    // OK so we don't have a stream but memory access could still be 20x faster
    // Seek to end get file size; if it's below limit then use faster memory
    // implementation.
    fseek(f, 0, SEEK_END);
    size_t size = ftell(f);
    rewind(f);
    if (size < MEMORY_FILE_LIMIT) {
        // Use faster VFS_CSV_MEM
        int result = csvMem_makeDB(db, f);
        fclose(f);
        return result;
    }

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

/**
 * @brief Get the Field Index of a field in this table
 *
 * @param db
 * @param field
 * @return int -1 if not found
 */
int getFieldIndex (struct DB *db, const char *field) {
    int (*vfs_getFieldIndex) (struct DB *, const char *)
        = VFS_Table[db->vfs].getFieldIndex;

    if (vfs_getFieldIndex != NULL) {
        return vfs_getFieldIndex(db, field);
    }

    return -1;
}

char *getFieldName (struct DB *db, int field_index) {
    char * (*vfs_getFieldName) (struct DB *, int)
        = VFS_Table[db->vfs].getFieldName;

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
int getRecordValue (
    struct DB *db,
    int record_index,
    int field_index,
    char *value,
    size_t value_max_length
) {
    // A NULL rowid could come from a LEFT JOIN
    if (record_index == ROWID_NULL) {
        value[0] = '\0';
        return 0;
    }

    // This is the only field we can handle generically
    if (field_index == FIELD_ROW_INDEX) {
        return sprintf(value, "%d", record_index);
    }

    int (*vfs_getRecordValue) (struct DB *, int, int, char *, size_t)
        = VFS_Table[db->vfs].getRecordValue;

    if (vfs_getRecordValue != NULL) {
        return vfs_getRecordValue(
            db,
            record_index,
            field_index,
            value,
            value_max_length
        );
    }

    return -1;
}

/**
 * @brief Searches for an index file with an explict name `x` i.e. UNIQUE(x) or
 * INDEX(x) or the autogenerated name `table__field`.
 * Returns 0 on failure; 1 for a regular index, 2 for unique index
 *
 * @param db struct DB * OUT - Database to populate with index (Can be NULL)
 * @param table_name
 * @param index_name
 * @param index_type_flags INDEX_ANY|INDEX_REGULAR|INDEX_UNIQUE|INDEX_PRIMARY
 * @returns enum IndexSearchType INDEX_REGULAR|INDEX_UNIQUE|INDEX_PRIMARY
 * |INDEX_NONE
 */
enum IndexSearchType findIndex(
    struct DB *db,
    const char *table_name,
    const char *index_name,
    enum IndexSearchType index_type_flags
) {
    if (strcmp(table_name, "CALENDAR") == 0) {
        return calendar_findIndex(db, table_name, index_name, index_type_flags);
    }

    if (strncmp(table_name, "SEQUENCE(", 9) == 0) {
        return sequence_findIndex(db, table_name, index_name, index_type_flags);
    }

    return csv_findIndex(db, table_name, index_name, index_type_flags);
}

/**
 * @brief Access table, filtering rows
 *
 * @return int number of matched rows
 */
int fullTableAccess (
    struct DB *db,
    struct RowList * row_list,
    struct Node *predicates,
    int predicate_count,
    int limit_value
) {
    if (db->vfs == 0) {
        fprintf(stderr, "Trying to access uninitialised DB\n");
        exit(-1);
    }

    int (*vfs_fullTableAccess) (
        struct DB *,
        struct RowList *,
        struct Node *,
        int,
        int
    ) = VFS_Table[db->vfs].fullTableAccess;

    if (vfs_fullTableAccess != NULL) {
        return vfs_fullTableAccess(
            db,
            row_list,
            predicates,
            predicate_count,
            limit_value
        );
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
            struct Node *predicate = &predicates[j];
            struct Node *left = &predicate->children[0];
            struct Node *right = &predicate->children[1];

            // All fields in predicates MUST be on this table or constant

            result = evaluateTableNode(db, i, left, value_left);
            if (result < 0) {
                return -1;
            }
            result = evaluateTableNode(db, i, right, value_right);
            if (result < 0) {
                return -1;
            }

            if (!evaluateExpression(
                predicate->function,
                value_left,
                value_right
            )) {
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
int fullTableScan (
    struct DB *db,
    struct RowList * row_list,
    int start_rowid,
    int limit_value
) {
    // VFS-agnostic implementation

    if (db->vfs == 0) {
        fprintf(stderr, "Trying to access uninitialised DB\n");
        exit(-1);
    }

    int count = limit_value;

    if (limit_value < 0) {
        count = getRecordCount(db) - start_rowid;
    }

    // Exclusive
    int end_rowid = start_rowid + count;

    // We hope that end_rowid < getRecordCount(db)

    // Just push all rowids in range
    for (int i = start_rowid; i < end_rowid; i++) {
        appendRowID(row_list, i);
    }

    return count;
}

/**
 * @brief Special case of uniqueIndexSearch where [index rowid] = [table rowid]
 *
 * @return rowid, or RESULT_NO_ROWS (-1) if not found
 */
int pkSearch (struct DB *db, const char * value) {
    int output_flag;

    int rowid = uniqueIndexSearch(db, value, &output_flag);

    if (output_flag) {
        return RESULT_NO_ROWS;
    }

    return rowid;
}

/**
 * @brief (Binary) Search a sorted table for a value and return associated rowid
 *
 * @param db must be an index with sorted column 0
 * @param value value to search for in cilumn 0 of db
 * @param mode MODE_UNIQUE: index is unique; MODE_LOWER_BOUND: return first
 * matching rowid; MODE_UPPER_BOUND: return last matching rowid
 * @param output_flag 0: value found; RESULT_BETWEEN: value not found but would
 * appear just before returned rowid; RESULT_BELOW_MIN: value below minimum;
 * RESULT_ABOVE_MAX: value above maximum
 *
 * @returns rowid of match (or closest match) in index; or RESULT_NO_ROWS (-1)
 * if out of bounds;
 */
int indexSearch (
    struct DB *db,
    const char *search_value,
    int mode,
    int * output_flag
) {
    if (db->vfs == 0) {
        fprintf(stderr, "Trying to perform index search on uninitialised DB\n");
        exit(-1);
    }

    int (*vfs_indexSearch) (struct DB *, const char *, int, int *)
        = VFS_Table[db->vfs].indexSearch;

    if (vfs_indexSearch != NULL) {
        return vfs_indexSearch(db, search_value, mode, output_flag);
    }

    // VFS-Agnostic implementation

    // By definition
    int index_column = 0;

    int index_a = 0;
    int index_b = getRecordCount(db) - 1;
    int index_match = -1;
    int numeric_mode = is_numeric(search_value);

    long search_as_number;

    if (numeric_mode) {
        search_as_number = atol(search_value);
    }

    char record_value[MAX_VALUE_LENGTH] = {0};

    int res;

    // Check boundary cases before commencing search

    // Check lower boundary (index_a = 0)
    getRecordValue(db, index_a, index_column, record_value, MAX_VALUE_LENGTH);
    if (numeric_mode) {
        res = search_as_number - atol(record_value);
    } else {
        res = strcmp(search_value, record_value);
    }

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
        getRecordValue(
            db,
            index_b,
            index_column,
            record_value,
            MAX_VALUE_LENGTH
        )
        ;
        if (numeric_mode) {
            res = search_as_number - atol(record_value);
        } else {
            res = strcmp(search_value, record_value);
        }

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

            getRecordValue(
                db,
                index_curr,
                index_column,
                record_value,
                MAX_VALUE_LENGTH
            )
            ;
            if (numeric_mode) {
                res = search_as_number - atol(record_value);
            } else {
                res = strcmp(search_value, record_value);
            }

            if (res == 0) {
                // printf(
                //     "pk_search [%d   <%d>   %d]: %s\n",
                //     index_a,
                //     index_curr,
                //     index_b,
                //     val
                // );
                index_match = index_curr;
                break;
            }

            if (res > 0) {
                // printf(
                //     "pk_search [%d   (%d) x %d]: %s\n",
                //     index_a,
                //     index_curr,
                //     index_b,
                //     val
                // );
                index_a = index_curr;

            } else {
                // printf(
                //     "pk_search [%d x (%d)   %d]: %s\n",
                //     index_a,
                //     index_curr,
                //     index_b,
                //     val
                // );
                index_b = index_curr;
            }
        }

        // We didn't find the value but it should be inbetween index_a and
        // index_b.
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
            getRecordValue(
                db,
                --index_match,
                index_column,
                record_value,
                MAX_VALUE_LENGTH
            );

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
            getRecordValue(
                db,
                ++index_match,
                index_column,
                record_value,
                MAX_VALUE_LENGTH
            );

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
 * @brief Search a sorted table for a value and return rowid
 *
 * @param db must be an index with sorted column 0
 * @param value value to search for in column 0 of db
 * @param output_flag 0: value found; 1: value not found but just before
 * returned rowid; 2: value below minimum; 3: value above maximum
 * @returns 0.. rowid of match in index; -1 if not found;
 */
int uniqueIndexSearch (struct DB *db, const char * value, int * output_flag) {
    return indexSearch(db, value, 0, output_flag);
}

/**
 * @brief Like evaluateNode() but with the restriction that all fields must be
 * constant or fields on this table
 *
 * @return int
 */
static int evaluateTableNode (
    struct DB *db,
    int row_id,
    struct Node *node,
    char * output
) {
    if (node->function == FUNC_UNITY) {
        // With FUNC_UNITY we'll just output directly to parent
        return evaluateTableField(db, row_id, (struct Field *)node, output);
    }

    if (node->child_count == -1) {
        // Optimistation where node is its own child

        char value[MAX_VALUE_LENGTH];
        char *values[] = { value };
        evaluateTableField(db, row_id, (struct Field *)node, value);
        return evaluateFunction(output, node->function, values, 1);
    }

    int n = node->child_count;
    char *values = malloc(n * MAX_VALUE_LENGTH);
    char **values_ptrs = malloc(n * sizeof(values));
    for (int i = 0; i < n; i++) {
        values_ptrs[i] = values + MAX_FIELD_LENGTH * i;
    }

    if (values == NULL) {
        fprintf(
            stderr,
            __FILE__ " Unable to allocate %d bytes for %d child node values\n",
            node->child_count * MAX_VALUE_LENGTH,
            node->child_count
        );
        exit(-1);
    }

    for (int i = 0; i < node->child_count; i++) {
        evaluateTableNode(
            db,
            row_id,
            &node->children[i],
            values_ptrs[i]
        );
    }

    int result = evaluateFunction(
        output,
        node->function,
        values_ptrs,
        node->child_count
    );

    free(values);
    free(values_ptrs);

    return result;
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
static int evaluateTableField (
    struct DB *db,
    int row_id,
    struct Field *field,
    char * output
) {

    if (field->index == FIELD_CONSTANT) {
        return evaluateConstantField(output, field);
    }

    if (field->table_id < 0) {
        return 0;
    }

    if (field->index == FIELD_ROW_INDEX) {
        return sprintf(output, "%d", row_id);
    }

    return getRecordValue(
        db,
        row_id,
        field->index,
        output,
        MAX_VALUE_LENGTH
    ) > 0;
}

int insertRow (struct DB *db, const char *row) {
    if (db->vfs == 0) {
        fprintf(stderr, "Trying to perform insert on uninitialised DB\n");
        exit(-1);
    }

    int (*vfs_insertRow) (struct DB *, const char *)
        = VFS_Table[db->vfs].insertRow;

    if (vfs_insertRow != NULL) {
        return vfs_insertRow(db, row);
    }

    fprintf(stderr, "VFS doesn't support insertion\n");
    exit(-1);
}

int insertFromQuery (struct DB *db, const char *query, const char **end_ptr) {
    if (db->vfs == 0) {
        fprintf(stderr, "Trying to perform insert on uninitialised DB\n");
        exit(-1);
    }

    int (*vfs_insertFromQuery) (struct DB *, const char *, const char **)
        = VFS_Table[db->vfs].insertFromQuery;

    if (vfs_insertFromQuery != NULL) {
        return vfs_insertFromQuery(db, query, end_ptr);
    }

    fprintf(stderr, "VFS doesn't support insertion\n");
    exit(-1);
}