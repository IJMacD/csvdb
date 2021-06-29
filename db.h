#ifndef DB_H
#define DB_H

#include <stdio.h>

struct DB {
    FILE *file;
    char *fields;
    int field_count;
    long *line_indices;
    int record_count;
};

void makeDB (struct DB *db, FILE *f);

int openDB (struct DB *db, const char *filename);

void closeDB (struct DB *db);

/**
 * Max header length 1024 characters
 */
int getFieldIndex (struct DB *db, const char *field);

char * getFieldName (struct DB *db, int field_index);

int getRecordValue (struct DB *db, int record_index, int field_index, char *value, size_t value_max_length);

#endif