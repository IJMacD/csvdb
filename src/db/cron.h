#pragma once

#include "../structs.h"

int cron_openDB (struct DB *db, const char *filename, char **resolved);

void cron_closeDB (struct DB *db);

int cron_getFieldIndex (struct DB *db, const char *field);

char *cron_getFieldName (struct DB *db, int field_index);

int cron_getRecordCount (struct DB *db);

int cron_getRecordValue (
    struct DB *db,
    int record_index,
    int field_index,
    char *value,
    size_t value_max_length
);

int cron_fullTableAccess (
    struct DB *db,
    RowListIndex list_id,
    struct Node *nodes,
    int node_count,
    int limit_value
);
