#include <string.h>
#include <stdlib.h>

#include "../structs.h"
#include "../query/query.h"
#include "csv-mmap.h"

static char table_names[MAX_TEMP_TABLES][32] = {0};
static char file_names[MAX_TEMP_TABLES][32] = {0};

/**
 * @brief
 *
 * @param db
 * @param table_name
 * @return int 0 on success; -1 on failure
 */
int temp_openDB (struct DB *db, const char *table_name) {
    for (int i = 0; i < MAX_TEMP_TABLES; i++) {
        if (strcmp(table_name, table_names[i]) == 0) {
            return csvMmap_openDB(db, file_names[i]);
        }
    }

    return -1;
}

/**
 * @brief
 *
 * @param name
 * @param query
 * @return int 0 on success; -1 on failure
 */
int temp_create (const char *name, const char *query, const char ** end_ptr) {
    int next_index = 0;

    while (table_names[next_index][0] != '\0') next_index++;

    strcpy(table_names[next_index], name);

    // select_subquery() execute the creation query, write the results to a temp
    // file and save the filename to file_names array.
    int result = select_subquery(query, file_names[next_index], end_ptr);

    // failed to open so just delete temp file
    if (result < 0) {
        remove(file_names[next_index]);
        return -1;
    }

    return 0;
}

void temp_drop (const char *name) {
    for (int i = 0; i < MAX_TEMP_TABLES; i++) {
        if (strcmp(name, table_names[i]) == 0) {
           remove(file_names[i]);
           return;
        }
    }
}

void temp_dropAll () {
    for (int i = 0; i < MAX_TEMP_TABLES; i++) {
        if (table_names[i][0] != '\0') {
           remove(file_names[i]);
        }
    }
}

int temp_findTable (const char *name, char *filename) {
    for (int i = 0; i < MAX_TEMP_TABLES; i++) {
        if (strcmp(name, table_names[i]) == 0) {
           strcpy(filename, file_names[i]);
           return 0;
        }
    }
    return -1;
}
