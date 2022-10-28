#include <stdio.h>

#include "structs.h"

void test (const char *filename, const char *field) {
    struct DB db;

    if (openDB(&db, filename) != 0) {
        fprintf(stderr, "File not found: %s\n", filename);
        exit(-1);
    }

    printf("%d records\n", db.record_count);

    printf("'age' found at %d\n", getFieldIndex(&db, "age"));
    printf("'classes' found at %d\n", getFieldIndex(&db, "classes"));
    printf("'registered' found at %d\n", getFieldIndex(&db, "registered"));
    printf("'name' found at %d\n", getFieldIndex(&db, "name"));
    printf("'id' found at %d\n", getFieldIndex(&db, "id"));

    if (field[0] != '\0') {
        int field_index = getFieldIndex(&db, field);

        if (field_index < 0) {
            printf("Couldn't find field '%s'\n", field);
            return -1;
        }

        for (int i = 0; i < db.record_count; i++) {
            char value[255];
            if (getRecordValue(&db, i, field_index, value, 255) == 0) {
                printf("%s\n", value);
            } else {
                printf("Error with record %d\n", i);
            }
        }
    }
}