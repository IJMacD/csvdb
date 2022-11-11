#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <unistd.h>

#include "structs.h"
#include "query/query.h"
#include "query/output.h"
#include "repl.h"

static int read_file(FILE *file, char **output);

extern char* gitversion;

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
        "\tSELECT <fields, ...> FROM <file> [JOIN <file>] [WHERE] [ORDER BY] "
        "[OFFSET FETCH FIRST]\n"
        "\tSELECT <fields, ...> FROM (<query>) [JOIN <file>] [WHERE] [ORDER BY]"
        " [OFFSET FETCH FIRST]\n"
        "\tVALUES (value,...),...\n"
        "\n"
        "\t<file> can be a CSV file which behaves as a table, or an SQL file "
        "which will behave as a view.\n"
        "\tIf <file> is the string 'stdin' then an attempt will be made to read"
        " the table from stdin.\n"
        "\tIf an exact filename match cannot be found, %1$s will try to append "
        "'.csv' and then '.sql'\n"
        "\tand attempt to open the file as either a table or a view "
        "respectively.\n"
        "\n"
        "Options:\n"
        "\t[-E|--explain]\n"
        "\t[-H|--headers] (default)\n"
        "\t[--no-headers]\n"
        "\t[(-F |--format=)(tsv|csv|html|json|json_array|sql|sql_values|xml|"
        "record)]\n"
        "\t[(-o |--output=)<filename>]\n"
        "\n"
        "Version: %2$s\n"
    , name, gitversion);
}

int main (int argc, char * argv[]) {
    srand((unsigned) time(NULL) * getpid());

    int flags = 0;

    // Default: with headers
    flags |= OUTPUT_OPTION_HEADERS;

    FILE * output = stdout;
    const char * format_val = NULL;
    const char * output_name = NULL;
    char *buffer = NULL;

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
                fprintf(stderr,
                    "Expected output name to be specified after -o\n"
                );
                printUsage(argv[0]);
                exit(-1);
            }

            output_name = argv[++argi];
        }
        else if (strcmp(arg, "-v") == 0) {
            flags |= OUTPUT_OPTION_VERBOSE;
        }
        else if (strncmp(arg, "--output=", 9) == 0) {
            output_name = arg + 9;
        }
        else if (strcmp(arg, "--stats") == 0) {
            flags |= OUTPUT_OPTION_STATS;
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

            size_t count = read_file(f, &buffer);

            fclose(f);

            if (count == 0) {
                fprintf(stderr, "File '%s' was empty\n", filename);
                return -1;
            }
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
        } else if (strcmp(format_val, "sql_values") == 0) {
            flags |= OUTPUT_FORMAT_SQL_VALUES;
        } else {
            fprintf(stderr, "Unrecognised format: %s\n", format_val);
            return -1;
        }
    } else {
        // Disable next line for some fun just dumping data with no delineation
        flags |= OUTPUT_FORMAT_TABLE;
    }

    if (output_name != NULL) {
        if(strcmp(output_name, "-") == 0) {
            output = stdout;
        }
        else {
            output = fopen(output_name, "w");

            if (!output) {
                fprintf(
                    stderr,
                    "Couldn't open file '%s' for writing\n",
                    output_name
                );
                return -1;
            }
        }
    }


    int bare_args = argc - argi;

    if (bare_args) {
        buffer = malloc(1024);

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
    }

    if (buffer != NULL) {
        #ifdef DEBUG
        flags |= OUTPUT_OPTION_VERBOSE;
        #endif

        int result = runQueries(buffer, flags, output);
        free(buffer);
        return result;
    }

    // If we're here it means we don't yet have a query.
    // If stdin is something more than a tty (i.e pipe or redirected file)
    // then we will assume the following query:
    if (!isatty(fileno(stdin))) {
        #ifdef DEBUG
        flags |= OUTPUT_OPTION_VERBOSE;
        #endif

        return query("SELECT * FROM stdin", flags, output, NULL);
    }

    if (flags & OUTPUT_OPTION_VERBOSE) {
        printf("%s\n", gitversion);
        return 0;
    }

    repl();

    return 0;
}

/**
 * @brief Read a file into a buffer created by this function. It is the caller's
 * responsibility to free the buffer returned in output
 *
 * @param file
 * @param output OUT a buffer will be malloc'd and written to this pointer
 * @return int Size of file read in bytes
 */
static int read_file (FILE *file, char **output) {
    size_t read_size = 1024;
    size_t alloc_size = read_size;

    char *buffer = malloc(alloc_size);

    size_t count = 0;
    size_t bytes_read;

    while(
        (bytes_read = fread(buffer + count, 1, read_size, file)) == read_size
    ) {
        alloc_size += read_size;
        buffer = realloc(buffer, alloc_size);

        if (buffer == NULL) {
            fprintf(
                stderr,
                "Unable to allocate read buffer of size %ld\n",
                alloc_size
            );
            exit(-1);
        }

        count += bytes_read;
    }

    count += bytes_read;

    buffer[count] = '\0';

    *output = buffer;

    return count;
}