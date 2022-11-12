#include <stdio.h>

#include "../structs.h"

int csvMem_openDB (struct DB *db, const char *filename);

void csvMem_closeDB (struct DB *db);

int csvMem_makeDB (struct DB *db, FILE *f);

/**
 * Max header length 1024 characters
 */
int csvMem_getFieldIndex (struct DB *db, const char *field);

char *csvMem_getFieldName (struct DB *db, int field_index);

int csvMem_getRecordCount (struct DB *db);

int csvMem_getRecordValue (
    struct DB *db,
    int record_index,
    int field_index,
    char *value,
    size_t value_max_length
);

int csvMem_findIndex(
    struct DB *db,
    const char *table_name,
    const char *index_name,
    int index_type_flags
);

const char *csvMem_fromValues (struct DB *db, const char *values, int length);

void csvMem_fromHeaders (struct DB *db, const char *headers);

int csvMem_insertRow (struct DB *db, const char *row);
