#include <stdio.h>

#include "structs.h"

int csvMmap_openDB (struct DB *db, const char *filename);

void csvMmap_closeDB (struct DB *db);

int csvMmap_makeDB (struct DB *db, FILE *f);

int csvMmap_getFieldIndex (struct DB *db, const char *field);

char *csvMmap_getFieldName (struct DB *db, int field_index);

int csvMmap_getRecordCount (struct DB *db);

int csvMmap_getRecordValue (
    struct DB *db,
    int record_index,
    int field_index,
    char *value,
    size_t value_max_length
);
