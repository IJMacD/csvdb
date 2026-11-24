#pragma once

#include "../structs.h"

int permutations_openDB (struct DB *db, const char *filename, char **resolved);

void permutations_closeDB (struct DB *db);

int permutations_getFieldIndex (struct DB *db, const char *field);

char *permutations_getFieldName (struct DB *db, int field_index);

int permutations_getRecordCount (struct DB *db);

int permutations_getRecordValue (
    struct DB *db,
    int record_index,
    int field_index,
    char *value,
    size_t value_max_length
);
