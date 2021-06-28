#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "query.h"

void printUsage (const char* name) {
    printf(
        "Usage:\n"
        "\t%1$s [-h|--headers] \"SELECT <fields, ...> FROM <file> [WHERE] [ORDER BY] [OFFSET FETCH FIRST]\"\n"
        "\t%1$s [-h|--headers] \"CREATE [UNIQUE] INDEX <index_file> ON <file> (<field>)\"\n"
        "\t%1$s [-h|--headers] -- file.sql\n"
        "\t%1$s [-h|--headers] (expects input on stdin)\n"
    , name);
}

int main (int argc, char * argv[]) {
    char buffer[1024];
    int flags = 0;

    int arg = 1;

    if (argc > arg && (strcmp(argv[arg], "-h") == 0 || strcmp(argv[arg], "--headers") == 0)) {
        flags |= OUTPUT_FLAG_HEADERS;
        arg++;
    }

    if (argc > arg + 1) {
        if (strcmp(argv[arg], "--") == 0) {
            FILE *f = fopen(argv[arg + 1], "r");

            if (!f) {
                fprintf(stderr, "Couldn't open file %s\n", argv[2]);
                return -1;
            }

            size_t count = fread(buffer, 1, 1024, f);

            if (count > 0) {
                buffer[count] = '\0';
                query(buffer, flags);
                return 0;
            }

            printf("File '%s' was empty\n", argv[2]);
            return -1;
        }

        printUsage(argv[0]);
        return -1;
    }

    if (argc > arg) {
        query(argv[arg], flags);
        return 0;
    }

    // If stdin is something more than a tty (i.e pipe or redirected file) then
    // we should read from it.
    if (!isatty(fileno(stdin))) {
        size_t count = fread(buffer, 1, 1024, stdin);
        if (count > 0) {
            buffer[count] = '\0';
            query(buffer, flags);
            return 0;
        }
    }

    printUsage(argv[0]);
    return -1;
}
