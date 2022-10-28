#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "structs.h"

int sequence_openDB (struct DB *db, const char *filename) {
    if (strncmp(filename, "SEQUENCE(", 9) != 0) {
        return -1;
    }

    db->vfs = VFS_SEQUENCE;
    db->record_count = atoi(filename + 9);
    db->line_indices = NULL;
    db->field_count = 1;

    return 0;
}

void sequence_closeDB (__attribute__((unused)) struct DB *db) {}

int sequence_getFieldIndex (__attribute__((unused)) struct DB *db, const char *field) {
    if (strcmp(field, "value") == 0) {
        return 0;
    }

    return -1;
}

char *sequence_getFieldName (__attribute__((unused)) struct DB *db, int field_index) {
    if (field_index == 0)
        return "value";
    return "";
}

int sequence_getRecordValue (__attribute__((unused)) struct DB *db, int record_index, __attribute__((unused)) int field_index, char *value, __attribute__((unused)) size_t value_max_length) {
    return sprintf(value, "%d", record_index);
}

// All queries go through fullTableScan
int sequence_findIndex(__attribute__((unused)) struct DB *db, __attribute__((unused)) const char *table_name, __attribute__((unused)) const char *index_name, __attribute__((unused)) int index_type_flags) {
    return 0;
}
