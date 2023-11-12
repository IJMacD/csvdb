#include <stdio.h>

#include "../structs.h"

int colMem_openDB (struct DB *db, const char *filename, char **resolved);

void colMem_closeDB (struct DB *db);

/**
 * Max header length 1024 characters
 */
int colMem_getFieldIndex (struct DB *db, const char *field);

char *colMem_getFieldName (struct DB *db, int field_index);

int colMem_getRecordCount (struct DB *db);

int colMem_getRecordValue (
    struct DB *db,
    int record_index,
    int field_index,
    char *value,
    size_t value_max_length
);

enum IndexSearchType colMem_findIndex(
    struct DB *db,
    const char *table_name,
    struct Node *node,
    int index_type_flags,
    char **resolved
);
