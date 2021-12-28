#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>

#include "query.h"
#include "output.h"

void printUsage (const char* name) {
    printf(
        "Usage:\n"
        "\t%1$s <options> \"SELECT <fields, ...> FROM <file> [WHERE] [ORDER BY] [OFFSET FETCH FIRST]\"\n"
        "\t%1$s <options> \"SELECT <fields, ...> FROM stdin [WHERE] [ORDER BY] [OFFSET FETCH FIRST]\"\n"
        "\t%1$s <options> -f file.sql\n"
        "\t%1$s <options> -f - (expects SQL on stdin)\n"
        "\t%1$s \"CREATE [UNIQUE] INDEX [<index_file>] ON <file> (<field>)\"\n"
        "\t%1$s -h|--help\n"
        "\n"
        "Options:\n"
        "\t[-H|--headers] [(-F| --format=)(tsv|csv|html|json|json_array)]\n"
    , name);
}

int main (int argc, char * argv[]) {
    char buffer[1024];
    int flags = 0;

    int arg = 1;

    if (argc > arg && (strcmp(argv[arg], "-h") == 0 || strcmp(argv[arg], "--help") == 0)) {
        printUsage(argv[0]);
        return 0;
    }

    if (argc > arg && (strcmp(argv[arg], "-H") == 0 || strcmp(argv[arg], "--headers") == 0)) {
        flags |= OUTPUT_OPTION_HEADERS;
        arg++;
    }

    char * format_val = NULL;

    if (argc > arg && strcmp(argv[arg], "-F") == 0) {
        arg++;

        if (argc > arg) {
            format_val = argv[arg];
            arg++;
        }
    }
    else if (argc > arg && strncmp(argv[arg], "--format=", 9) == 0) {
        format_val = argv[arg] + 9;
        arg++;
    }

    if (format_val != NULL) {
        if(strcmp(format_val, "tsv") == 0) {
            flags |= OUTPUT_FORMAT_TAB;
        } else if (strcmp(format_val, "csv") == 0) {
            flags |= OUTPUT_FORMAT_COMMA;
        } else if (strcmp(format_val, "html") == 0) {
            flags |= OUTPUT_FORMAT_HTML;
        } else if (strcmp(format_val, "json_array") == 0) {
            flags |= OUTPUT_FORMAT_JSON_ARRAY;
        } else if (strcmp(format_val, "json") == 0) {
            flags |= OUTPUT_FORMAT_JSON;
        } else {
            fprintf(stderr, "Unrecognised format: %s\n", format_val);
            return -1;
        }
    }

    if (argc > arg && strcmp(argv[arg], "-f") == 0) {
        if (argc > arg + 1) {
            FILE *f;

            if(strcmp(argv[arg + 1], "-") == 0) {
                f = stdin;
            }
            else {
                f = fopen(argv[arg + 1], "r");

                if (!f) {
                    fprintf(stderr, "Couldn't open file '%s'\n", argv[arg + 1]);
                    return -1;
                }
            }

            size_t count = fread(buffer, 1, 1024, f);

            if (count > 0) {
                buffer[count] = '\0';
                return query(buffer, flags);
            }

            fprintf(stderr, "File '%s' was empty\n", argv[2]);
            return -1;
        }

        fprintf(stderr, "Expecting file to be specified\n");
        printUsage(argv[0]);
        return -1;
    }

    if (argc > arg) {
        if (argv[arg][0] == '-') {
            fprintf(stderr, "Unknown option %s\n", argv[arg]);
            printUsage(argv[0]);
            return -1;
        }

        return query(argv[arg], flags);
    }

    // If we're here it means we don't yet have a query.
    // If stdin is something more than a tty (i.e pipe or redirected file)
    // then we will assume the following query:
    if (!isatty(fileno(stdin))) {
        query("SELECT * FROM stdin", flags);
        return 0;
    }

    printUsage(argv[0]);
    return -1;
}
