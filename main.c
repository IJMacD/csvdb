#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "query.h"

void printUsage (const char* name) {
    printf("Usage:\n\t%1$s \"SELECT <fields, ...> FROM <file> [WHERE <field> = <value>]\"\n\t%1$s -- file.sql\n", name);
}

int main (int argc, char * argv[]) {
    char buffer[1024];

    if (argc > 2) {
        if (strcmp(argv[1], "--") == 0) {
            FILE *f = fopen(argv[2], "r");

            if (!f) {
                fprintf(stderr, "Couldn't open file %s\n", argv[2]);
                return -1;
            }

            size_t count = fread(buffer, 1, 1024, f);

            if (count > 0) {
                query(buffer);
                return 0;
            }

            printf("File '%s' was empty\n", argv[2]);
            return -1;
        }

        printUsage(argv[0]);
        return -1;
    }

    if (argc > 1) {
        query(argv[1]);
        return 0;
    }

    // If stdin is something more than a tty (i.e pipe or redirected file) then
    // we should read from it.
    if (!isatty(fileno(stdin))) {
        size_t count = fread(buffer, 1, 1024, stdin);
        if (count > 0) {
            buffer[count] = '\0';
            query(buffer);
            return 0;
        }
    }
    
    printUsage(argv[0]);
    return -1;
}
