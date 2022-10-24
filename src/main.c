#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <unistd.h>

#include "query.h"
#include "output.h"

char *process_name;

void printUsage (const char* name) {
    printf(
        "Usage:\n"
        "\t%1$s <options> \"<query>\"\n"
        "\t%1$s <options> -f file.sql\n"
        "\t%1$s <options> -f - (expects SQL on stdin)\n"
        "\t%1$s \"CREATE [UNIQUE] INDEX [<index_file>] ON <file> (<field>)\"\n"
        "\t%1$s \"CREATE TABLE <file> AS <query>\"\n"
        "\t%1$s \"INSERT INTO <file> <query>\"\n"
        "\t%1$s -h|--help\n"
        "\n"
        "Where <query> is one of:\n"
        "\tSELECT <fields, ...> FROM <file> [JOIN <file>] [WHERE] [ORDER BY] [OFFSET FETCH FIRST]\n"
        "\tSELECT <fields, ...> FROM (<query>) [JOIN <file>] [WHERE] [ORDER BY] [OFFSET FETCH FIRST]\n"
        "\tVALUES (value,...),...\n"
        "\n"
        "\t<file> can be a CSV file which behaves as a table, or an SQL file which will behave as a view.\n"
        "\tIf <file> is the string 'stdin' then an attempt will be made to read the table from stdin.\n"
        "\tIf an exact filename match cannot be found, %1$s will try to append '.csv' and then '.sql'\n"
        "\tand attempt to open the file as either a table or a view respectively.\n"
        "\n"
        "Options:\n"
        "\t[-E|--explain]\n"
        "\t[-H|--headers] (default)\n"
        "\t[--no-headers]\n"
        "\t[(-F |--format=)(tsv|csv|html|json|json_array|sql)]\n"
        "\t[(-o |--output=)<filename>]\n"
    , name);
}

int main (int argc, char * argv[]) {
    process_name = argv[0];

    srand((unsigned) time(NULL) * getpid());

    int flags = 0;

    // Default: with headers
    flags |= OUTPUT_OPTION_HEADERS;

    FILE * output = stdout;
    const char * format_val = NULL;
    const char * output_name = NULL;
    char buffer[1024] = {0};

    int argi = 1;

    while (argi < argc && argv[argi][0] == '-') {
        const char *arg = argv[argi];

        if (strcmp(arg, "-h") == 0 || strcmp(arg, "--help") == 0) {
            printUsage(argv[0]);
            return 0;
        }

        if (strcmp(arg, "-E") == 0 || strcmp(arg, "--explain") == 0) {
            flags |= FLAG_EXPLAIN;
        }
        else if ((strcmp(arg, "-H") == 0 || strcmp(arg, "--headers") == 0)) {
            flags |= OUTPUT_OPTION_HEADERS;
        }
        else if (strcmp(arg, "--no-headers") == 0) {
            flags &= !OUTPUT_OPTION_HEADERS;
        }
        else if (strcmp(arg, "-F") == 0) {
            if (argi + 1 >= argc) {
                fprintf(stderr, "Expected format to be specified after -F\n");
                printUsage(argv[0]);
                exit(-1);
            }

            format_val = argv[++argi];
        }
        else if (strncmp(arg, "--format=", 9) == 0) {
            format_val = arg + 9;
        }
        else if (strcmp(arg, "-o") == 0) {
            if (argi + 1 >= argc) {
                fprintf(stderr, "Expected output name to be specified after -o\n");
                printUsage(argv[0]);
                exit(-1);
            }

            output_name = argv[++argi];
        }
        else if (strncmp(arg, "--output=", 9) == 0) {
            output_name = arg + 9;
        }
        else if (strcmp(arg, "-f") == 0) {
            if (argi + 1 >= argc) {
                fprintf(stderr, "Expected file to be specified after -f\n");
                printUsage(argv[0]);
                exit(-1);
            }

            const char *filename = argv[++argi];

            FILE *f;

            if(strcmp(filename, "-") == 0) {
                f = stdin;
            }
            else {
                f = fopen(filename, "r");

                if (!f) {
                    fprintf(stderr, "Couldn't open file '%s'\n", filename);
                    return -1;
                }
            }

            size_t count = fread(buffer, 1, 1024, f);

            if (count == 0) {
                fprintf(stderr, "File '%s' was empty\n", filename);
                return -1;
            }

            buffer[count] = '\0';
        }
        else if (strcmp(arg, "-0") == 0) {
            // Secret internal argument to force read-only mode
            flags |= FLAG_READ_ONLY;
        }
        else {
            fprintf(stderr, "Unknown option %s\n", arg);
            printUsage(argv[0]);
            return -1;
        }

        argi++;
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
        } else if (strcmp(format_val, "table") == 0) {
            flags |= OUTPUT_FORMAT_TABLE;
        } else if (strcmp(format_val, "record") == 0) {
            flags |= OUTPUT_FORMAT_INFO_SEP;
        } else if (strcmp(format_val, "xml") == 0) {
            flags |= OUTPUT_FORMAT_XML;
        } else {
            fprintf(stderr, "Unrecognised format: %s\n", format_val);
            return -1;
        }
    } else {
        // Disable next line for some fun just dumping data with no delineation
        flags |= OUTPUT_FORMAT_TAB;
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


    int bare_args = argc - argi;

    if (bare_args == 1) {
        strcpy(buffer, argv[argi]);
    }
    else if (bare_args > 1) {
        int offset = 0;

        while (argi < argc) {
            offset += sprintf(buffer + offset, "%s ", argv[argi]);
            argi++;
        }
    }

    if (buffer[0] != '\0') {
        return query(buffer, flags, output);
    }

    // If we're here it means we don't yet have a query.
    // If stdin is something more than a tty (i.e pipe or redirected file)
    // then we will assume the following query:
    if (!isatty(fileno(stdin))) {
        return query("SELECT * FROM stdin", flags, output);
    }

    printUsage(argv[0]);
    return -1;
}
