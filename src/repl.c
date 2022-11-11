#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "structs.h"
#include "query/query.h"
#include "query/token.h"

void repl () {
    enum OutputOption options = OUTPUT_OPTION_HEADERS | OUTPUT_FORMAT_TABLE;

    while(1) {
        printf("> ");

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

        const char *end_ptr;

        query(line_buffer, options, stdout, &end_ptr);

        free(line_buffer);

        printf("\n");
    }
}