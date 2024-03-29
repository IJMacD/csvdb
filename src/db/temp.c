#include <string.h>
#include <stdlib.h>

#include "../structs.h"
#include "../query/select.h"
#include "db.h"
#include "temp.h"
#include "csv-mem.h"
#include "csv-mmap.h"
#include "csv.h"

struct DB *temp_mapping = NULL;

int find_table_rowid(const char *table_name);

/**
 * @brief
 *
 * @param db
 * @param table_name
 * @return int 0 on success; -1 on failure
 */
int temp_openDB(
    struct DB *db,
    const char *table_name,
    char **resolved)
{
    char filename[MAX_TABLE_LENGTH];

    if (temp_findTable(table_name, filename) == 0)
    {
        return csvMmap_openDB(db, filename, resolved);
    }

    return -1;
}

void temp_setMappingDB(struct DB *db)
{
    temp_mapping = db;
}

/**
 * @brief
 *
 * @param name
 * @param query
 * @return int 0 on success; -1 on failure
 */
int temp_create(const char *name, const char *query, const char **end_ptr)
{
    if (temp_mapping == NULL)
    {
        temp_mapping = temp_openMappingDB(NULL);
    }

    // Check if table already exists in mapping.
    int result = find_table_rowid(name);
    if (result >= 0)
    {
        fprintf(stderr, "TEMP table '%s' already exists.\n", name);
        return -1;
    }

    char filename[MAX_TABLE_LENGTH];

    // select_subquery() execute the creation query, write the results to a temp
    // file and save the filename to file_names array.
    result = select_subquery_file(query, filename, end_ptr);

    // failed to open so just delete temp file
    if (result < 0)
    {
        remove(filename);
        return -1;
    }

    char *row = malloc(MAX_TABLE_LENGTH * 2 + 1);
    sprintf(row, "%s,%s", name, filename);

    insertRow(temp_mapping, row);

    free(row);

    return 0;
}

void temp_drop(const char *name)
{
    if (temp_mapping == NULL)
    {
        return;
    }

    char filename[MAX_TABLE_LENGTH];

    if (temp_findTable(name, filename) == 0)
    {
        remove(filename);
    }
}

void temp_dropAll()
{
    if (temp_mapping == NULL)
    {
        return;
    }

    int filename_index = getFieldIndex(temp_mapping, "filename");

    char filename[MAX_TABLE_LENGTH];

    for (int i = 0; i < getRecordCount(temp_mapping); i++)
    {
        getRecordValue(
            temp_mapping,
            i,
            filename_index,
            filename,
            MAX_TABLE_LENGTH);

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
int temp_findTable(const char *table_name, char *filename)
{
    // If we haven't been configured with an on-disk mapping table, then create
    // one now in memory.
    if (temp_mapping == NULL)
    {
        temp_mapping = temp_openMappingDB(NULL);
    }

    int filename_index = getFieldIndex(temp_mapping, "filename");

    int result = find_table_rowid(table_name);

    if (result < 0)
    {
        return -1;
    }

    getRecordValue(
        temp_mapping,
        result,
        filename_index,
        filename,
        MAX_TABLE_LENGTH);

    return 0;
}

/**
 * @brief Open or create a mapping DB at the filename. If filename is NULL then
 * an in memory mapping DB will be created which needs to be closed and free'd
 * later.
 *
 * @param filename can be NULL for an in-memory mapping
 * @return struct DB*
 */
struct DB *temp_openMappingDB(const char *filename)
{
    struct DB *db = malloc(sizeof(*db));

    // First deal with in-memory mapping.
    if (filename == NULL)
    {
        csvMem_fromHeaders(db, "table,filename");
        return db;
    }

    // Next, check if the mapping file already exists.
    // Must specifically choose VFS_CSV to make sure changes are persisted to
    // disk.
    if (csv_openDB(db, filename, NULL) == 0)
    {
        return db;
    }

    // Finally, create a new mapping file.
    // Must specifically choose VFS_CSV to make sure it is persisted to disk.
    int result = csv_fromHeaders(db, filename, "table,filename");

    if (result < 0)
    {
        fprintf(stderr, "Unable to create mapping DB at %s\n", filename);
        exit(-1);
    }

    return db;
}

int find_table_rowid(const char *table_name)
{
    int table_index = getFieldIndex(temp_mapping, "table");

    char value[MAX_FIELD_LENGTH];

    for (int i = 0; i < getRecordCount(temp_mapping); i++)
    {
        getRecordValue(temp_mapping, i, table_index, value, MAX_FIELD_LENGTH);

        if (strcmp(table_name, value) == 0)
        {
            return i;
        }
    }

    return -1;
}
