#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <unistd.h>

#include "structs.h"
#include "query/query.h"
#include "query/output.h"
#include "db/temp.h"
#include "repl.h"

static int read_file(FILE *file, char **output);
static enum OutputOption get_format_flag (const char *format_val);

extern char* gitversion;
int debug_verbosity = 0;

#ifdef DEBUG
const char *version_debug = "DEBUG";
#else
const char *version_debug = "";
#endif

void printUsage (const char* name, FILE *file) {
    fprintf(
        file,
        "Usage:\n"
        "\t%1$s <options> \"<query>\"\n"
        "\t%1$s <options> -f file.sql\n"
        "\t%1$s <options> -f - (expects SQL on stdin)\n"
        "\t%1$s \"CREATE [UNIQUE] INDEX [<index_file>] ON <file> (<field>)\"\n"
        "\t%1$s \"CREATE TABLE <file> AS <query>\"\n"
        "\t%1$s \"INSERT INTO <file> <query>\"\n"
        "\t%1$s \"CREATE VIEW <file> AS <query>\"\n"
        "\t%1$s -h|--help\n"
        "\n"
        "Where <query> is one of:\n"
        "\tSELECT <fields, ...> FROM <file> [JOIN <file> [ON ...]] [WHERE]\n"
        "\t    [ORDER BY] [OFFSET <n> ROWS] [FETCH (FIRST|NEXT) <m> ROWS]\n"
        "\tSELECT <fields, ...> FROM (<query>) ...\n"
        "\tVALUES (value, ...), ...\n"
        "\tTABLE <file>\n"
        "\tWITH <name> AS (<query>) FROM <name> ...\n"
        "\n"
        "\t<file> can be a CSV file, which behaves as a table; or a SQL file, which\n"
        "\tbehaves as a view. Filetype is determined from the filename extension.\n"
        "\tIf an exact filename match cannot be found, %1$s will automatically\n"
        "\tappend '.csv' and then '.sql' and attempt to open the file as either a\n"
        "\t table or a view respectively.\n"
        "\tIf <file> is the string 'stdin' then an attempt will be made to read the\n"
        "\ttable data from stdin.\n"
        "\n"
        "Options:\n"
        "\t[-h|--help]\n"
        "\t[-f (<filename.sql>|-)] Read SQL from file ('-' for stdin)\n"
        "\t[-E|--explain]\n"
        "\t[-H|--headers] (default)\n"
        "\t[-N|--no-headers]\n"
        "\t[(-F |--format=)<format>] Set output formatting (TTY default: table)\n"
        "\t[(-i |--input=)<filename>] Read from file instead of stdin\n"
        "\t[(-o |--output=)<filename>] Write output to file instead of stdout\n"
        "\t[--stats] Write timing data to 'stats.csv'\n"
#ifdef DEBUG
        "\t[-v|-vv|-vvv|--verbose=n] Set DEBUG verbosity\n"
        "\t[-A] Output AST"
#endif
        "\n"
        "\n"
        "Where <format> is one of:\n"
        "\t(table|box|tsv|csv[:excel]|html|json[:(object|array)]|"
        "sql[:(insert|create|values)]|xml|record)"
        "\n"
        "\n"
        "Version: %2$s %3$s\n",
        name, gitversion, version_debug);
}

int main (int argc, char * argv[]) {
    srand((unsigned) time(NULL) * getpid());

    int flags = 0;

    // Default: with headers
    flags |= OUTPUT_OPTION_HEADERS;

    FILE * output = stdout;
    const char * format_val = NULL;
    const char * input_name = NULL;
    const char * output_name = NULL;
    char *buffer = NULL;

    int argi = 1;

    while (argi < argc && argv[argi][0] == '-') {
        const char *arg = argv[argi];

        if (strcmp(arg, "-h") == 0 || strcmp(arg, "--help") == 0) {
            printUsage(argv[0], stdout);
            return 0;
        }

        if (strcmp(arg, "-E") == 0 || strcmp(arg, "--explain") == 0) {
            flags |= FLAG_EXPLAIN;
        }
        else if (strcmp(arg, "-H") == 0 || strcmp(arg, "--headers") == 0) {
            flags |= OUTPUT_OPTION_HEADERS;
        }
        else if (strcmp(arg, "-N") == 0 || strcmp(arg, "--no-headers") == 0) {
            flags &= !OUTPUT_OPTION_HEADERS;
        }
        else if (strcmp(arg, "-F") == 0) {
            if (argi + 1 >= argc) {
                fprintf(stderr, "Expected format to be specified after -F\n");
                printUsage(argv[0], stderr);
                exit(-1);
            }

            format_val = argv[++argi];
        }
        else if (strncmp(arg, "--format=", 9) == 0) {
            format_val = arg + 9;
        }
        else if (strcmp(arg, "-i") == 0) {
            if (
                argi + 1 >= argc ||
                (argv[argi+1][0] == '-' && argv[argi+1][1] != '\0')
            ) {
                fprintf(stderr,
                    "Expected input name to be specified after -i\n"
                );
                printUsage(argv[0], stderr);
                exit(-1);
            }

            input_name = argv[++argi];
        }
        else if (strncmp(arg, "--input=", 8) == 0) {
            input_name = arg + 8;
        }
        else if (strcmp(arg, "-o") == 0) {
            if (
                argi + 1 >= argc ||
                (argv[argi+1][0] == '-' && argv[argi+1][1] != '\0')
            ) {
                fprintf(stderr,
                    "Expected output name to be specified after -o\n"
                );
                printUsage(argv[0], stderr);
                exit(-1);
            }

            output_name = argv[++argi];
        }
        else if (strncmp(arg, "--output=", 9) == 0) {
            output_name = arg + 9;
        }
        else if (strcmp(arg, "-v") == 0) {
            flags |= OUTPUT_OPTION_VERBOSE;
            #ifdef DEBUG
            debug_verbosity = 1;
            #endif
        }
        #ifdef DEBUG
        else if (strcmp(arg, "-vv") == 0) {
            flags |= OUTPUT_OPTION_VERBOSE;
            debug_verbosity = 2;
        }
        else if (strcmp(arg, "-vvv") == 0) {
            flags |= OUTPUT_OPTION_VERBOSE;
            debug_verbosity = 3;
        }
        else if (strncmp(arg, "--verbose=", 10) == 0) {
            flags |= OUTPUT_OPTION_VERBOSE;
            debug_verbosity = atoi(arg + 10);
        }
        else if (strcmp(arg, "-A") == 0) {
            flags |= OUTPUT_OPTION_AST;
        }
        #endif
        else if (strcmp(arg, "--stats") == 0) {
            flags |= OUTPUT_OPTION_STATS;
        }
        else if (strcmp(arg, "-f") == 0) {
            if (argi + 1 >= argc) {
                fprintf(stderr, "Expected file to be specified after -f\n");
                printUsage(argv[0], stderr);
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
            printUsage(argv[0], stderr);
            return -1;
        }

        argi++;
    }

    if (input_name != NULL) {
        if(strcmp(input_name, "-") == 0) {
            // Do nothing, it's already stdin
        }
        else {
            // Swap the process's stdin for the file specified on the command
            // line.
            FILE *result = freopen(input_name, "r", stdin);
            if (result == NULL) {
                fprintf(
                    stderr,
                    "Couldn't open file '%s' for reading\n",
                    input_name
                );
                return -1;
            }
        }
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

    if (format_val != NULL) {
        int format_flag = get_format_flag(format_val);
        if (format_flag < 0) {
            fprintf(stderr, "Unrecognised format: %s\n", format_val);
            return -1;
        }
        flags |= format_flag;
    } else if (isatty(fileno(output))) {
        flags |= OUTPUT_FORMAT_BOX;
    } else {
        // Disable next line for some fun just dumping data with no delineation
        flags |= OUTPUT_FORMAT_COMMA;
    }

    if (argc > argi) {
        char *write_ptr;

        if (buffer == NULL) {
            buffer = malloc(1024);
            write_ptr = buffer;
        }
        else {
            write_ptr = buffer + strlen(buffer);
            *(write_ptr++) = ' ';
        }

        while (argi < argc) {
            write_ptr += sprintf(write_ptr, "%s ", argv[argi++]);
        }
    }

    #ifdef PERSISTANT_TEMP
    temp_setMappingDB(temp_openMappingDB("/tmp/csvdb.session.0000.temp.csv"));
    #endif

    if (buffer != NULL) {
        int result = runQueries(buffer, flags, output);
        free(buffer);

        #ifndef PERSISTANT_TEMP
        temp_dropAll();
        #endif

        return result;
    }

    // If we're here it means we don't yet have a query.
    // If stdin is something more than a tty (i.e pipe or redirected file)
    // then we will assume the following query:
    if (!isatty(fileno(stdin))) {
        return query("SELECT * FROM stdin", flags, output, NULL);
    }

    // If verbose option is set but we don't actually have a query, then we
    // treat it like a version flag.
    if (flags & OUTPUT_OPTION_VERBOSE) {
        printf("%s %s\n", gitversion, version_debug);
        return 0;
    }

    repl();

    #ifndef PERSISTANT_TEMP
    temp_dropAll();
    #endif

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

static enum OutputOption get_format_flag (const char *format_val) {

    if(strcmp(format_val, "tsv") == 0) {
        return OUTPUT_FORMAT_TAB;
    }

    if (strcmp(format_val, "csv") == 0) {
        return OUTPUT_FORMAT_COMMA;
    }

    if (strcmp(format_val, "csv:excel") == 0) {
        return OUTPUT_FORMAT_CSV_EXCEL;
    }

    if (strcmp(format_val, "html") == 0) {
        return OUTPUT_FORMAT_HTML;
    }

    if (
        strcmp(format_val, "json") == 0 ||
        strcmp(format_val, "json:object") == 0
    ) {
        return OUTPUT_FORMAT_JSON;
    }

    if (
        strcmp(format_val, "json:array") == 0 ||
        strcmp(format_val, "json_array") == 0 // compat
    ) {
        return OUTPUT_FORMAT_JSON_ARRAY;
    }

    if (strcmp(format_val, "table") == 0) {
        return OUTPUT_FORMAT_TABLE;
    }

    if (strcmp(format_val, "record") == 0) {
        return OUTPUT_FORMAT_INFO_SEP;
    }

    if (strcmp(format_val, "xml") == 0) {
        return OUTPUT_FORMAT_XML;
    }

    if (
        strcmp(format_val, "sql") == 0 ||
        strcmp(format_val, "sql:insert") == 0
    ) {
        return OUTPUT_FORMAT_SQL_INSERT;
    }

    if (
        strcmp(format_val, "sql:values") == 0 ||
        strcmp(format_val, "sql_values") == 0 // compat
    ) {
        return OUTPUT_FORMAT_SQL_VALUES;
    }

    if (
        strcmp(format_val, "sql:create") == 0 ||
        strcmp(format_val, "sql_create") == 0 // compat
    ) {
        return OUTPUT_FORMAT_SQL_CREATE;
    }

    if (strcmp(format_val, "box") == 0)
    {
        return OUTPUT_FORMAT_BOX;
    }

    return -1;
}