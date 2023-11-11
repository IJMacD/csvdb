#include <stdio.h>

#include "../structs.h"

int tsvMem_openDB (struct DB *db, const char *filename, char **resolved);

void tsvMem_closeDB (struct DB *db);

/**
 * Max header length 1024 characters
 */
int tsvMem_getFieldIndex (struct DB *db, const char *field);

char *tsvMem_getFieldName (struct DB *db, int field_index);

int tsvMem_getRecordCount (struct DB *db);

int tsvMem_getRecordValue (
    struct DB *db,
    int record_index,
    int field_index,
    char *value,
    size_t value_max_length
);

enum IndexSearchType tsvMem_findIndex(
    struct DB *db,
    const char *table_name,
    struct Node *node,
    int index_type_flags,
    char **resolved
);

const char *tsvMem_fromValues (struct DB *db, const char *values, int length);

void tsvMem_fromHeaders (struct DB *db, const char *headers);

int tsvMem_fromQuery (struct DB *db, struct Query *query);

int tsvMem_insertRow (struct DB *db, const char *row);
