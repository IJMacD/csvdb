#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <ctype.h>

#include <unistd.h>

#include "query.h"
#include "output.h"

/*
 * CGI Spec:
 *
 * /cgi-bin/csvdb/extra/path?query=string
 *
 * $PATH_INFO = /extra/path
 * $QUERY_STRING = query=string
 *
 * POST data: stdin
 */

char **global_argv;

void urldecode2(char *dst, const char *src);

int main () {

    char buffer[1024];
    int flags = 0;

    FILE * output = stdout;

    srand((unsigned) time(NULL) * getpid());

    // if (argc > arg && (strcmp(argv[arg], "-E") == 0 || strcmp(argv[arg], "--explain") == 0)) {
    //     flags |= FLAG_EXPLAIN;
    //     arg++;
    // }

    // if (argc > arg && (strcmp(argv[arg], "-H") == 0 || strcmp(argv[arg], "--headers") == 0)) {
        flags |= OUTPUT_OPTION_HEADERS;
    //     arg++;
    // }

    // char * format_val = NULL;

    // if (argc > arg && strcmp(argv[arg], "-F") == 0) {
    //     arg++;

    //     if (argc > arg) {
    //         format_val = argv[arg];
    //         arg++;
    //     }
    // }
    // else if (argc > arg && strncmp(argv[arg], "--format=", 9) == 0) {
    //     format_val = argv[arg] + 9;
    //     arg++;
    // }

    // if (format_val != NULL) {
    //     if(strcmp(format_val, "tsv") == 0) {
    //         flags |= OUTPUT_FORMAT_TAB;
    //     } else if (strcmp(format_val, "csv") == 0) {
    //         flags |= OUTPUT_FORMAT_COMMA;
    //     } else if (strcmp(format_val, "html") == 0) {
    //         flags |= OUTPUT_FORMAT_HTML;
    //     } else if (strcmp(format_val, "json_array") == 0) {
    //         flags |= OUTPUT_FORMAT_JSON_ARRAY;
    //     } else if (strcmp(format_val, "json") == 0) {
    //         flags |= OUTPUT_FORMAT_JSON;
    //     } else {
    //         fprintf(stderr, "Unrecognised format: %s\n", format_val);
    //         return -1;
    //     }
    // }
    flags |= OUTPUT_FORMAT_HTML;

    // char * output_name = NULL;

    // if (argc > arg && strcmp(argv[arg], "-o") == 0) {
    //     arg++;

    //     if (argc > arg) {
    //         output_name = argv[arg];
    //         arg++;
    //     }
    // }
    // else if (argc > arg && strncmp(argv[arg], "--output=", 9) == 0) {
    //     output_name = argv[arg] + 9;
    //     arg++;
    // }

    // if (output_name != NULL) {

    //     if(strcmp(output_name, "-") == 0) {
    //         output = stdout;
    //     }
    //     else {
    //         output = fopen(output_name, "w");

    //         if (!output) {
    //             fprintf(stderr, "Couldn't open file '%s' for writing\n", output_name);
    //             return -1;
    //         }
    //     }
    // }

    dup2(STDOUT_FILENO, STDERR_FILENO);

    size_t count = fread(buffer, 1, 1024, stdin);

    if (count > 0) {
        buffer[count] = '\0';

        int offset = 0;

        if (strncmp(buffer, "query=", 6) == 0) {
            offset = 6;
        }

        // printf("%s\n", buffer + offset);

        urldecode2(buffer, buffer + offset);

        printf("Content-Type: text/html\n\n");

        return query(buffer, flags, output);
    }

    printf("Content-Type: text/plain\n\n");

    printf("Error processing query\n");

    return -1;
}

void urldecode2(char *dst, const char *src)
{
    char a, b;
    while (*src) {
        if ((*src == '%') &&
            ((a = src[1]) && (b = src[2])) &&
            (isxdigit(a) && isxdigit(b))) {
                if (a >= 'a')
                    a -= 'a'-'A';
                if (a >= 'A')
                    a -= ('A' - 10);
                else
                    a -= '0';
                if (b >= 'a')
                    b -= 'a'-'A';
                if (b >= 'A')
                    b -= ('A' - 10);
                else
                    b -= '0';
                *dst++ = 16*a+b;
                src+=3;
        } else if (*src == '+') {
            *dst++ = ' ';
            src++;
        } else {
            *dst++ = *src++;
        }
    }
    *dst++ = '\0';
}