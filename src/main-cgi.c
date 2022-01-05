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
    int flags = OUTPUT_OPTION_HEADERS | OUTPUT_FORMAT_HTML;

    FILE * output = stdout;

    srand((unsigned) time(NULL) * getpid());

    // is this working?
    // dup2(STDOUT_FILENO, STDERR_FILENO);

    size_t count = fread(buffer, 1, 1024, stdin);

    if (count > 0) {
        buffer[count] = '\0';

        int offset = 0;

        if (strncmp(buffer, "query=", 6) == 0) {
            offset = 6;
        }

        // printf("%s\n", buffer + offset);

        urldecode2(buffer, buffer + offset);

        // Explain query still only outputs "unformatted"
        if (strncmp(buffer, "EXPLAIN", 7) == 0) {
            printf("Content-Type: text/plain\n\n");
        } else {
            printf("Content-Type: text/html\n\n");
        }

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