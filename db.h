#pragma once

#include <stdio.h>
#include "predicates.h"
#include "result.h"

#define VFS_NULL        0
#define VFS_CSV         1
#define VFS_INTERNAL    2
#define VFS_CALENDAR    3
#define VFS_CSV_MEM     4

struct DB {
    int vfs;
    FILE *file;
    char *fields;
    int field_count;
    long *line_indices;
    int record_count;
    char * data;
};

int openDB (struct DB *db, const char *filename);

void closeDB (struct DB *db);

/**
 * Max header length 1024 characters
 */
int getFieldIndex (struct DB *db, const char *field);

char * getFieldName (struct DB *db, int field_index);

int getRecordValue (struct DB *db, int record_index, int field_index, char *value, size_t value_max_length);

int findIndex(struct DB *db, const char *table_name, const char *index_name, int index_type_flags);

int fullTableScan (struct DB *db, struct RowList * row_list, struct Predicate *predicates, int predicate_count, int limit_value);

int fullTableAccess (struct DB *db, struct RowList * row_list, int limit);
