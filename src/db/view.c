#include <string.h>
#include <stdlib.h>

#include "../structs.h"
#include "../query/query.h"
#include "csv-mem.h"

/**
 * @brief
 *
 * @param db
 * @param filename
 * @return int 0 on success; -1 on failure
 */
int view_openDB (struct DB *db, const char *filename, char **resolved) {
    char filename_buffer[FILENAME_MAX] = {0};
    char query_buffer[FILENAME_MAX] = {0};
    FILE *f;

    int len = strlen(filename);
    if (len > 4 && strcmp(filename + len - 4, ".sql") == 0) {
        f = fopen(filename, "r");

        if (f && resolved != NULL) {
            *resolved = realpath(filename, *resolved);
        }
    } else {
        sprintf(filename_buffer, "%s.sql", filename);
        f = fopen(filename_buffer, "r");

        if (f && resolved != NULL) {
            *resolved = realpath(filename_buffer, *resolved);
        }
    }

    if (f == NULL) {
        return -1;
    }

    size_t count = fread(query_buffer, 1, 1024, f);

    fclose(f);

    if (count == 0) {
        fprintf(stderr, "File '%s' was empty\n", filename);
        return -1;
    }

    query_buffer[count] = '\0';

    // select_subquery_mem() will execute the view, and write the results to a
    // memory buffer handing off to CSV_MEM
    int result = select_subquery_mem(query_buffer, db, NULL);
    if (result < 0) {
        return -1;
    }

    return result;
}