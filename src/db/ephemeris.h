#pragma once

#include "../structs.h"

int ephemeris_openDB (struct DB *db, const char *filename, char **resolved);

void ephemeris_closeDB (struct DB *db);

int ephemeris_getFieldIndex (struct DB *db, const char *field);

char *ephemeris_getFieldName (struct DB *db, int field_index);

int ephemeris_getRecordCount (struct DB *db);

int ephemeris_getRecordValue (
    struct DB *db,
    int record_index,
    int field_index,
    char *value,
    size_t value_max_length
);

enum IndexSearchType ephemeris_findIndex (
    struct DB *db,
    const char *table_name,
    struct Node *node,
    int index_type_flags,
    char **resolved
);

int ephemeris_fullTableAccess (
    struct DB *db,
    RowListIndex list_id,
    struct Node *nodes,
    int node_count,
    int limit_value
);

int ephemeris_indexSearch (
    struct DB *db,
    const char *value,
    int mode,
    int *output_flag
);
