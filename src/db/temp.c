#include <string.h>
#include <stdlib.h>

#include "../structs.h"
#include "../query/query.h"
#include "db.h"
#include "temp.h"
#include "csv-mem.h"
#include "csv-mmap.h"
#include "csv.h"

struct DB *temp_mapping = NULL;

/**
 * @brief
 *
 * @param db
 * @param table_name
 * @return int 0 on success; -1 on failure
 */
int temp_openDB (struct DB *db, const char *table_name) {
    char filename[MAX_TABLE_LENGTH];

    if (temp_findTable(table_name, filename) == 0) {
        return csvMmap_openDB(db, filename);
    }

    return -1;
}

void temp_setMappingDB (struct DB *db) {
    temp_mapping = db;
}

/**
 * @brief
 *
 * @param name
 * @param query
 * @return int 0 on success; -1 on failure
 */
int temp_create (const char *name, const char *query, const char ** end_ptr) {
    if (temp_mapping == NULL) {
        temp_mapping = temp_openMappingDB(NULL);
    }

    char filename[MAX_TABLE_LENGTH];

    // select_subquery() execute the creation query, write the results to a temp
    // file and save the filename to file_names array.
    int result = select_subquery_file(query, filename, end_ptr);

    // failed to open so just delete temp file
    if (result < 0) {
        remove(filename);
        return -1;
    }

    char *row = malloc(MAX_TABLE_LENGTH * 2 + 1);
    sprintf(row, "%s,%s", name, filename);

    insertRow(temp_mapping, row);

    free(row);

    return 0;
}

void temp_drop (const char *name) {
    if (temp_mapping == NULL) {
        return;
    }

    char filename[MAX_TABLE_LENGTH];

    if (temp_findTable(name, filename) == 0) {
        remove(filename);
    }
}

void temp_dropAll () {
    if (temp_mapping == NULL) {
        return;
    }

    int filename_index = getFieldIndex(temp_mapping, "filename");

    char filename[MAX_TABLE_LENGTH];

    for (int i = 0; i < getRecordCount(temp_mapping); i++) {
        getRecordValue(
            temp_mapping,
            i,
            filename_index,
            filename,
            MAX_TABLE_LENGTH
        );

        remove(filename);
    }
}

/**
 * @brief Searches mapping db for table named `table_name`. If it finds one it
 * writes the corresponding filename to `filename`.
 *
 * @param table_name
 * @param filename
 * @return int 0 if found; -1 if not found
 */
int temp_findTable (const char *table_name, char *filename) {
    if (temp_mapping == NULL) {
        temp_mapping = temp_openMappingDB(NULL);
    }

    int table_index = getFieldIndex(temp_mapping, "table");
    int filename_index = getFieldIndex(temp_mapping, "filename");

    char value[MAX_FIELD_LENGTH];

    for (int i = 0; i < getRecordCount(temp_mapping); i++) {
        getRecordValue(temp_mapping, i, table_index, value, MAX_FIELD_LENGTH);

        if (strcmp(table_name, value) == 0) {
            getRecordValue(
                temp_mapping,
                i,
                filename_index,
                filename,
                MAX_TABLE_LENGTH
            );

            return 0;
        }
    }

    return -1;
}

/**
 * @brief Open or create a mapping DB at the filename. if filename is NULL then
 * an in memory mapping DB will be created which needs to be closed and free'd
 * later.
 *
 * @param filename
 * @return struct DB*
 */
struct DB *temp_openMappingDB (const char *filename) {
    struct DB *db = malloc(sizeof(*db));

    if (filename == NULL) {
        csvMem_fromHeaders(db, "table,filename");
        return db;
    }

    // Must specifically choose VFS_CSV to make sure changes are persisted to
    // disk.
    if (csv_openDB(db, filename) == 0) {
        return db;
    }

    // Must specifically choose VFS_CSV to make sure it is persisted to disk.
    int result = csv_fromHeaders(db, filename, "table,filename");

    if (result < 0) {
        fprintf(stderr, "Unable to create mapping DB at %s\n", filename);
        exit(-1);
    }

    return db;
}