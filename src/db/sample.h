#include "../structs.h"

int sample_openDB (struct DB *db, const char *filename, char **resolved);

void sample_closeDB (struct DB *db);

/**
 * Max header length 1024 characters
 */
int sample_getFieldIndex (struct DB *db, const char *field);

char *sample_getFieldName (struct DB *db, int field_index);

int sample_getRecordCount (struct DB *db);

int sample_getRecordValue (
    struct DB *db,
    int record_index,
    int field_index,
    char *value,
    size_t value_max_length
);

int sample_findIndex(
    struct DB *db,
    const char *table_name,
    struct Node *node,
    int index_type_flags,
    char **resolved
);

