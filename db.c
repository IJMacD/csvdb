#include <string.h>

#include "db.h"
#include "db-csv.h"
#include "db-calendar.h"
#include "limits.h"

int openDB (struct DB *db, const char *filename) {
    if (strcmp(filename, "CALENDAR") == 0) {
        return calendar_openDB(db, filename);
    }

    int result = csv_openDB(db, filename);

    if (result == 0) {
        return 0;
    }

    return result;
}

void closeDB (struct DB *db) {
    if (db->vfs == VFS_CSV) {
        csv_closeDB(db);
    }
    else if (db->vfs == VFS_CALENDAR) {
        calendar_closeDB(db);
    }
}

int getFieldIndex (struct DB *db, const char *field) {
    if (db->vfs == VFS_CSV) {
        return csv_getFieldIndex(db, field);
    }
    else if (db->vfs == VFS_CALENDAR) {
        return calendar_getFieldIndex(db, field);
    }

    return -1;
}

char *getFieldName (struct DB *db, int field_index) {
    if (db->vfs == VFS_CSV) {
        return csv_getFieldName(db, field_index);
    }
    else if (db->vfs == VFS_CALENDAR) {
        return calendar_getFieldName(db, field_index);
    }

    return NULL;
}

int getRecordValue (struct DB *db, int record_index, int field_index, char *value, size_t value_max_length) {
    if (db->vfs == VFS_CSV) {
        return csv_getRecordValue(db, record_index, field_index, value, value_max_length);
    }
    else if (db->vfs == VFS_CALENDAR) {
        return calendar_getRecordValue(db, record_index, field_index, value, value_max_length);
    }

    return -1;
}

int findIndex(struct DB *db, const char *table_name, const char *index_name, int index_type_flags) {
    if (strcmp(table_name, "CALENDAR") == 0) {
        return calendar_findIndex(db, table_name, index_name, index_type_flags);
    }

    return csv_findIndex(db, table_name, index_name, index_type_flags);
}

/**
 * @return number of matched rows
 */
int fullTableScan (struct DB *db, int *result_rowids, struct Predicate *predicates, int predicate_count, int limit_value) {
    // Special implementation for calendar
    if (db->vfs == VFS_CALENDAR) {
        return calendar_fullTableScan(db, result_rowids, predicates, predicate_count, limit_value);
    }

    // VFS-agnostic implementation
    int result_count = 0;

    char value[VALUE_MAX_LENGTH] = {0};

    for (int i = 0; i < db->record_count; i++) {
        int matching = 1;

        // Perform filtering if necessary
        for (int j = 0; j < predicate_count && matching; j++) {
            struct Predicate *predicate = predicates + j;

            // Note: Could factor out next line if there are performance issues
            int predicate_field_index = getFieldIndex(db, predicate->field);
            getRecordValue(db, i, predicate_field_index, value, VALUE_MAX_LENGTH);

            if (!evaluateExpression(predicate->op, value, predicate->value)) {
                matching = 0;
                break;
            }
        }

        if (matching) {
            // Add to result set
            result_rowids[result_count++] = i;
        }

        // Implement early exit FETCH FIRST/LIMIT for cases with no ORDER clause
        if (limit_value >= 0 && result_count >= limit_value) {
            break;
        }
    }

    return result_count;
}

/**
 * A sort of dummy access function to just populate the result_rowids
 * array with all rowids in ascending numerical order
 */
int fullTableAccess (struct DB *db, int result_rowids[], int limit_value) {
    if (db->vfs == VFS_CALENDAR) {
        return calendar_fullTableScan(db, result_rowids, NULL, 0, limit_value);
    }

    // VFS-agnostic implementation

    int l = db->record_count;
    if (limit_value >= 0 && limit_value < l) {
        l = limit_value;
    }

    for (int i = 0; i < l; i++) {
        result_rowids[i] = i;
    }

    return l;
}
