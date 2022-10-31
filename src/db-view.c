#include <string.h>
#include <stdlib.h>

#include "structs.h"
#include "db-csv-mem.h"

extern char *process_name;

int view_openDB (struct DB *db, const char *filename) {
    char buffer[FILENAME_MAX] = {0};

    int len = strlen(filename);
    if (strcmp(filename + len - 4, ".sql") == 0) {
        sprintf(buffer, "%s -0 -H -F csv -f %s", process_name, filename);
    } else {
        sprintf(buffer, "%s.sql", filename);
        FILE *f = fopen(buffer, "r");
        if (f) {
            fclose(f);
            sprintf(buffer, "%s -0 -H -F csv -f %s.sql", process_name, filename);
        }
        else {
            buffer[0] = '\0';
        }
    }

    if (buffer[0] == '\0') {
        return -1;
    }

    FILE *f = popen(buffer, "r");

    if (f == NULL) {
        fprintf(stderr, "Unable to open process\n");
        exit(-1);
    }

    db->file = NULL;

    int result = csvMem_makeDB(db, f);

    pclose(f);

    return result;
}