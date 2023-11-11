#include <stdio.h>

#include "../structs.h"

int wsvMem_openDB (struct DB *db, const char *filename, char **resolved);

void wsvMem_closeDB (struct DB *db);

/**
 * Max header length 1024 characters
 */
int wsvMem_getFieldIndex (struct DB *db, const char *field);

char *wsvMem_getFieldName (struct DB *db, int field_index);

int wsvMem_getRecordCount (struct DB *db);

int wsvMem_getRecordValue (
    struct DB *db,
    int record_index,
    int field_index,
    char *value,
    size_t value_max_length
);

enum IndexSearchType wsvMem_findIndex(
    struct DB *db,
    const char *table_name,
    struct Node *node,
    int index_type_flags,
    char **resolved
);

const char *wsvMem_fromValues (struct DB *db, const char *values, int length);

void wsvMem_fromHeaders (struct DB *db, const char *headers);

int wsvMem_fromQuery (struct DB *db, struct Query *query);

int wsvMem_insertRow (struct DB *db, const char *row);
