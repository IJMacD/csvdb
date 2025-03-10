#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "structs.h"
#include "query/query.h"
#include "query/token.h"

extern char *gitversion;

static void printHelp ();

void repl () {
    enum OutputOption options = OUTPUT_OPTION_HEADERS | OUTPUT_FORMAT_BOX;
    printf("CSVDB REPL\nType \".help\" for help.\n\n");

    int explain = 0;

    while(1) {
        printf("> ");
        fflush(stdout);

        size_t buffer_size = 1024;
        char *line_buffer = malloc(buffer_size);
        size_t size = getline(&line_buffer, &buffer_size, stdin);

        if (size == -1ul) {
            printf("\n");
            break;
        }

        size_t index = 0;
        char token[32];
        getToken(line_buffer, &index, token, 32);

        if (token[0] == '\0') {
            continue;
        }

        if (strcmp(token, ".exit") == 0) {
            break;
        }

        if (strcmp(token, ".help") == 0) {
            printHelp();
            continue;
        }

        if (strcmp(token, ".version") == 0) {
            printf("Version: %s\n", gitversion);
            continue;
        }

        if (strcmp(token, ".explain") == 0) {
            explain = !explain;
            printf("Explain: %s\n", explain ? "ON" : "OFF");
            continue;
        }

        if (strcmp(token, ".format") == 0) {
            getToken(line_buffer, &index, token, 32);

            options &= ~OUTPUT_MASK_FORMAT;

            if(strcmp(token, "tsv") == 0) {
                options |= OUTPUT_FORMAT_TAB;
            } else if (strcmp(token, "csv") == 0) {
                options |= OUTPUT_FORMAT_COMMA;
            } else if (strcmp(token, "html") == 0) {
                options |= OUTPUT_FORMAT_HTML;
            } else if (strcmp(token, "json_array") == 0) {
                options |= OUTPUT_FORMAT_JSON_ARRAY;
            } else if (strcmp(token, "json") == 0) {
                options |= OUTPUT_FORMAT_JSON;
            } else if (strcmp(token, "sql") == 0) {
                options |= OUTPUT_FORMAT_SQL_INSERT;
            } else if (strcmp(token, "table") == 0) {
                options |= OUTPUT_FORMAT_TABLE;
            } else if (strcmp(token, "box") == 0) {
                options |= OUTPUT_FORMAT_BOX;
            } else if (strcmp(token, "record") == 0) {
                options |= OUTPUT_FORMAT_INFO_SEP;
            } else if (strcmp(token, "xml") == 0) {
                options |= OUTPUT_FORMAT_XML;
            } else if (strcmp(token, "sql_values") == 0) {
                options |= OUTPUT_FORMAT_SQL_VALUES;
            } else {
                options |= OUTPUT_FORMAT_BOX;
            }

            continue;
        }

        options &= ~FLAG_EXPLAIN;
        if (explain) {
            options |= FLAG_EXPLAIN;
        }

        const char *end_ptr;

        query(line_buffer, options, stdout, &end_ptr);

        free(line_buffer);

        printf("\n");
    }
}

static void printHelp () {
    printf(
        "Single line query execution supported\n\n"
        "Commands in REPL:\n"
        "\t.help\n"
        "\t.version\n"
        "\t.exit\n"
        "\t.format tsv|csv|html|table|box|json|json_array|sql|sql_values|xml|record\n"
        "\t.explain\n"
    );
}