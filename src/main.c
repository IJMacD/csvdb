#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <unistd.h>

#include "query.h"
#include "output.h"

char **global_argv;

void printUsage (const char* name) {
    printf(
        "Usage:\n"
        "\t%1$s <options> \"SELECT <fields, ...> FROM <file> [WHERE] [ORDER BY] [OFFSET FETCH FIRST]\"\n"
        "\t%1$s <options> -f file.sql\n"
        "\t%1$s <options> -f - (expects SQL on stdin)\n"
        "\t%1$s \"CREATE [UNIQUE] INDEX [<index_file>] ON <file> (<field>)\"\n"
        "\t%1$s -h|--help\n"
        "\n"
        "\t<file> can be a CSV file which behaves as a table, or an SQL file which will behave as a view.\n"
        "\tIf <file> is the string 'stdin' then an attempt will be made to read the table from stdin.\n"
        "\tIf an exact filename match cannot be found, %1$s will try to append '.csv' and then '.sql'\n"
        "\tand attempt to open the file as either a table or a view respectively.\n"
        "\n"
        "Options:\n"
        "\t[-E|--explain]\n"
        "\t[-H|--headers]\n"
        "\t[(-F |--format=)(tsv|csv|html|json|json_array|sql)]\n"
        "\t[(-o |--output=)<filename>]\n"
    , name);
}

int main (int argc, char * argv[]) {
    global_argv = argv;

    char buffer[1024];
    int flags = 0;

    int arg = 1;

    FILE * output = stdout;

    srand((unsigned) time(NULL) * getpid());

    if (argc > arg && (strcmp(argv[arg], "-h") == 0 || strcmp(argv[arg], "--help") == 0)) {
        printUsage(argv[0]);
        return 0;
    }

    if (argc > arg && (strcmp(argv[arg], "-E") == 0 || strcmp(argv[arg], "--explain") == 0)) {
        flags |= FLAG_EXPLAIN;
        arg++;
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
        } else if (strcmp(format_val, "sql") == 0) {
            flags |= OUTPUT_FORMAT_SQL_INSERT;
        } else {
            fprintf(stderr, "Unrecognised format: %s\n", format_val);
            return -1;
        }
    }

    char * output_name = NULL;

    if (argc > arg && strcmp(argv[arg], "-o") == 0) {
        arg++;

        if (argc > arg) {
            output_name = argv[arg];
            arg++;
        }
    }
    else if (argc > arg && strncmp(argv[arg], "--output=", 9) == 0) {
        output_name = argv[arg] + 9;
        arg++;
    }

    if (output_name != NULL) {

        if(strcmp(output_name, "-") == 0) {
            output = stdout;
        }
        else {
            output = fopen(output_name, "w");

            if (!output) {
                fprintf(stderr, "Couldn't open file '%s' for writing\n", output_name);
                return -1;
            }
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
                return query(buffer, flags, output);
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

        return query(argv[arg], flags, output);
    }

    // If we're here it means we don't yet have a query.
    // If stdin is something more than a tty (i.e pipe or redirected file)
    // then we will assume the following query:
    if (!isatty(fileno(stdin))) {
        query("SELECT * FROM stdin", flags, output);
        return 0;
    }

    printUsage(argv[0]);
    return -1;
}
