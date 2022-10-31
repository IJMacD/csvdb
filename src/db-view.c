#include <string.h>
#include <stdlib.h>

#include "structs.h"
#include "query.h"
#include "db-csv-mem.h"

extern char *process_name;

int view_openDB (struct DB *db, const char *filename) {
    char filename_buffer[FILENAME_MAX] = {0};
    char query_buffer[FILENAME_MAX] = {0};
    FILE *f;

    int len = strlen(filename);
    if (strcmp(filename + len - 4, ".sql") == 0) {
        f = fopen(filename, "r");
    } else {
        sprintf(filename_buffer, "%s.sql", filename);
        f = fopen(filename_buffer, "r");
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

    // select_subquery() execute the view, write the results to a temp file and
    // write the tmp filename to filename_buffer.
    int result = select_subquery(query_buffer, filename_buffer);
    if (result < 0) {
        remove(filename_buffer);
        return -1;
    }

    // Pass the temp filename to CSV VFS
    result = csvMem_openDB(db, filename_buffer);

    // Delete the temp file from disk
    remove(filename_buffer);

    return result;
}