#pragma once

#include <stdio.h>

int calendar_openDB (struct DB *db, const char *filename);

void calendar_closeDB (struct DB *db);

/**
 * Max header length 1024 characters
 */
int calendar_getFieldIndex (struct DB *db, const char *field);

char *calendar_getFieldName (struct DB *db, int field_index);

int calendar_getRecordValue (struct DB *db, int record_index, int field_index, char *value, size_t value_max_length);

int calendar_findIndex(struct DB *db, const char *table_name, const char *index_name, int index_type_flags);

int calendar_fullTableScan (struct DB *db, int *result_rowids, struct Predicate *predicates, int predicate_count, int limit_value);
