#pragma once

#include <stdio.h>

int csv_openDB (struct DB *db, const char *filename);

void csv_closeDB (struct DB *db);

/**
 * Max header length 1024 characters
 */
int csv_getFieldIndex (struct DB *db, const char *field);

char *csv_getFieldName (struct DB *db, int field_index);

int csv_getRecordValue (struct DB *db, int record_index, int field_index, char *value, size_t value_max_length);
