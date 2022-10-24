#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <ctype.h>
#include <errno.h>

#include <unistd.h>
#include <sys/stat.h>

#include "query.h"

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

// Process name for VIEWs/subqueries
// Hardcoded in Dockerfile
char *process_name = "/bin/csvdb";

static void urldecode2(char *dst, const char *src);

static void setDataDir (const char * datadir);

int main () {

    char query_buffer[1024];
    int flags = OUTPUT_OPTION_HEADERS;
    int format = 0;

    FILE * output = stdout;

    srand((unsigned) time(NULL) * getpid());

    // Redirect stderr -> stdout
    dup2(STDOUT_FILENO, STDERR_FILENO);

    // char dirname[255];
    // fprintf(stderr, "debug: cwd %s\n", getcwd(dirname, 255));

    setDataDir("/data"); // Hardcoded in Dockerfile getenv("CSVDB_DATA_DIR");

    char *accept_env = getenv("HTTP_ACCEPT");

    if (strstr(accept_env, "text/html") != NULL) {
        format = OUTPUT_FORMAT_HTML;
    }
    else if (strstr(accept_env, "application/json") != NULL) {
        format = OUTPUT_FORMAT_JSON;
    }
    else if (strstr(accept_env, "text/csv") != NULL) {
        format = OUTPUT_FORMAT_COMMA;
    }
    else if (strstr(accept_env, "application/sql") != NULL) {
        format = OUTPUT_FORMAT_SQL_INSERT;
    }
    else if (strstr(accept_env, "application/xml") != NULL) {
        format = OUTPUT_FORMAT_XML;
    }
    else {
        format = OUTPUT_FORMAT_TABLE;
    }

    // Don't modify char * returned from getenv
    char *query_string = getenv("QUERY_STRING");

    if (query_string == NULL) {
        printf("HTTP/1.1 500 Server Error\n");
        printf("Content-Type: text/plain\n\n");
        printf("No query string was provided\n");
        exit(-1);
    }

    while (*query_string != '\0') {
        char * eq = strchr(query_string, '=');
        if (eq == NULL) {
            break;
        }
        *eq = '\0';
        char * key = query_string;
        query_string = eq + 1;

        char * amp = strchr(query_string, '&');
        char * value = query_string;
        if (amp != NULL) {
            *amp = '\0';
            query_string = amp + 1;
        }

        // printf("Query-Value: %s: %s\n", key, value);

        if (strcmp(key, "format") == 0) {

            if (strcmp(value, "txt") == 0) {
                format = OUTPUT_FORMAT_TABLE;
            } else if (strcmp(value, "csv") == 0) {
                format = OUTPUT_FORMAT_COMMA;
            } else if (strcmp(value, "tsv") == 0) {
                format = OUTPUT_FORMAT_TAB;
            } else if (strcmp(value, "html") == 0) {
                format = OUTPUT_FORMAT_HTML;
            } else if (strcmp(value, "json") == 0) {
                format = OUTPUT_FORMAT_JSON;
            } else if (strcmp(value, "json_array") == 0) {
                format = OUTPUT_FORMAT_JSON_ARRAY;
            } else if (strcmp(value, "sql") == 0) {
                format = OUTPUT_FORMAT_SQL_INSERT;
            } else if (strcmp(value, "xml") == 0) {
                format = OUTPUT_FORMAT_XML;
            } else {
                format = OUTPUT_FORMAT_HTML;
            }

        } else if (strcmp(key, "query") == 0) {
            urldecode2(query_buffer, value);
        }
    }

    if (query_buffer == NULL) {
        printf("HTTP/1.1 500 Server Error\n");
        printf("Content-Type: text/plain\n\n");
        printf("No query was provided in the query string\n");
        exit(-1);
    }

    flags |= format;

    printf("Access-Control-Allow-Origin: *\n");

    // EXPLAIN query still only outputs "unformatted"
    if (strncmp(query_buffer, "EXPLAIN", 7) == 0) {
        printf("Content-Type: text/plain; charset=utf-8\n");
    }
    else if (format == OUTPUT_FORMAT_COMMA) {
        printf("Content-Type: text/csv; charset=utf-8\n");
    }
    else if (format == OUTPUT_FORMAT_TAB) {
        printf("Content-Type: text/tab-separated-values; charset=utf-8\n");
    }
    else if (format == OUTPUT_FORMAT_HTML) {
        printf("Content-Type: text/html; charset=utf-8\n");
    }
    else if (format == OUTPUT_FORMAT_JSON || format == OUTPUT_FORMAT_JSON_ARRAY)
    {
        printf("Content-Type: application/json; charset=utf-8\n");
    }
    else if (format == OUTPUT_FORMAT_SQL_INSERT) {
        printf("Content-Type: application/sql; charset=utf-8\n");
    }
    else if (format == OUTPUT_FORMAT_XML) {
        printf("Content-Type: application/xml; charset=utf-8\n");
    }
    else {
        printf("Content-Type: text/plain; charset=utf-8\n");
    }

    // This will end up as a header
    // fprintf(stderr, "query: %s\n", query_buffer);

    // Double newline to end headers
    printf("\n");

    if (query(query_buffer, flags, output)) {
        printf("Error processing query\n");
        return -1;
    }

    return 0;
}

static void urldecode2(char *dst, const char *src)
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

static void setDataDir (const char * datadir) {
    if (datadir != NULL) {
        if (chdir(datadir)) {
            if (errno == ENOENT) {
                if (mkdir(datadir, S_IRWXU|S_IRGRP|S_IXGRP)) {
                    printf("HTTP/1.1 500 Server Error\n");
                    printf("Content-Type: text/plain\n\n");
                    printf("error: %s\n", strerror(errno));
                    exit(-1);
                }
                // fprintf(stderr, "[DEBUG] created dir: %s\n", datadir);
                if (chdir(datadir)) {
                    printf("HTTP/1.1 500 Server Error\n");
                    printf("Content-Type: text/plain\n\n");
                    printf("error: %s\n", strerror(errno));
                    perror("chdir");
                    exit(-1);
                }
            }
            else  {
                printf("HTTP/1.1 500 Server Error\n");
                printf("Content-Type: text/plain\n\n");
                printf("error: %s\n", strerror(errno));
                exit(-1);
            }
        }
        // fprintf(stderr, "debug: cwd %s\n", getcwd(dirname, 255));
    }
}