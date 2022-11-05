#include "structs.h"

int dir_openDB (struct DB *db, const char *filename);

void dir_closeDB (struct DB *db);

/**
 * Max header length 1024 characters
 */
int dir_getFieldIndex (struct DB *db, const char *field);

char *dir_getFieldName (struct DB *db, int field_index);

int dir_getRecordCount (struct DB *db);

int dir_getRecordValue (
    struct DB *db,
    int record_index,
    int field_index,
    char *value,
    size_t value_max_length
);

int dir_findIndex(
    struct DB *db,
    const char *table_name,
    const char *index_name,
    int index_type_flags
);

