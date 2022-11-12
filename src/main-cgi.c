#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <ctype.h>
#include <errno.h>

#include <unistd.h>
#include <sys/stat.h>

#include "structs.h"
#include "query/query.h"
#include "db/temp.h"

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

static void urldecode2(char *dst, const char *src);

static int setDataDir (const char * datadir);

static void printError (int format, FILE *error);

static char *getSession ();

void set_session_temp_db (const char * session_id);

int main () {

    char query_buffer[1024];
    int flags = OUTPUT_OPTION_HEADERS | OUTPUT_OPTION_STATS;
    int format = OUTPUT_FORMAT_TABLE;

    FILE * output = stdout;

    srand((unsigned) time(NULL) * getpid());

    // open tmp file to collect errors
    FILE * error = fopen("/tmp/csvdb_error", "w+");

    // Redirect stderr -> error file
    dup2(fileno(error), STDERR_FILENO);

    // char dirname[255];
    // fprintf(stderr, "debug: cwd %s\n", getcwd(dirname, 255));

    char *data_dir = getenv("CSVDB_DATA_DIR");
    if (data_dir) {
        if (setDataDir(data_dir) < 0) {
            fclose(error);
            return -1;
        }
    }

    char *accept_env = getenv("HTTP_ACCEPT");

    if (accept_env != NULL) {
        if (strstr(accept_env, "text/html") != NULL) {
            format = OUTPUT_FORMAT_HTML;
        }
        else if (strstr(accept_env, "application/json") != NULL) {
            format = OUTPUT_FORMAT_JSON;
        }
        else if (strstr(accept_env, "text/csv") != NULL) {
            format = OUTPUT_FORMAT_COMMA;
        }
        else if (strstr(accept_env, "text/tab-separated-values") != NULL) {
            format = OUTPUT_FORMAT_TAB;
        }
        else if (strstr(accept_env, "application/sql") != NULL) {
            format = OUTPUT_FORMAT_SQL_INSERT;
        }
        else if (strstr(accept_env, "application/xml") != NULL) {
            format = OUTPUT_FORMAT_XML;
        }
    }

    // Don't modify char * returned from getenv
    char *query_string = getenv("QUERY_STRING");

    if (query_string == NULL) {
        printf("HTTP/1.1 500 Server Error\n");
        printf("Content-Type: text/plain\n\n");
        printf("No query string was provided\n");

        fclose(error);
        return -1;
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
            } else if (strcmp(value, "markdown") == 0) {
                format = OUTPUT_FORMAT_TABLE;
            }

        } else if (strcmp(key, "query") == 0) {
            urldecode2(query_buffer, value);
        }
    }

    if (query_buffer[0] == '\0') {
        printf("HTTP/1.1 500 Server Error\n");
        printf("Access-Control-Allow-Origin: *\n");
        printf("Content-Type: text/plain\n\n");
        printf("No query was provided in the query string\n");

        fclose(error);
        return -1;
    }

    char *session = getSession();

    if (session == NULL) {
        return -1;
    }

    set_session_temp_db(session);

    flags |= format;

    printf("Access-Control-Allow-Origin: *\n");

    if (format == OUTPUT_FORMAT_COMMA) {
        printf("Content-Type: text/csv; charset=utf-8\n");
    }
    else if (format == OUTPUT_FORMAT_TAB) {
        printf("Content-Type: text/tab-separated-values; charset=utf-8\n");
    }
    else if (format == OUTPUT_FORMAT_HTML) {
        printf("Content-Type: text/html; charset=utf-8\n");
    }
    else if (
        format == OUTPUT_FORMAT_JSON
        || format == OUTPUT_FORMAT_JSON_ARRAY
    ) {
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

    if (query(query_buffer, flags, output, NULL)) {
        // Write errors to stdout now
        printError(format, error);
        fclose(error);
        return -1;
    }

    fclose(error);
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

static int setDataDir (const char * datadir) {
    if (datadir != NULL) {
        if (chdir(datadir)) {
            if (errno == ENOENT) {
                if (mkdir(datadir, S_IRWXU|S_IRGRP|S_IXGRP)) {
                    printf("HTTP/1.1 500 Server Error\n");
                    printf("Content-Type: text/plain\n\n");
                    printf("%s\n", strerror(errno));
                    return -1;
                }
                // fprintf(stderr, "[DEBUG] created dir: %s\n", datadir);
                if (chdir(datadir)) {
                    printf("HTTP/1.1 500 Server Error\n");
                    printf("Content-Type: text/plain\n\n");
                    printf("%s\n", strerror(errno));
                    perror("chdir");
                    return -1;
                }
            }
            else  {
                printf("HTTP/1.1 500 Server Error\n");
                printf("Content-Type: text/plain\n\n");
                printf("%s\n", strerror(errno));
                return -1;
            }
        }
        // fprintf(stderr, "debug: cwd %s\n", getcwd(dirname, 255));
    }
    return 0;
}

static void printError (int format, FILE *error) {
    if (format == OUTPUT_FORMAT_HTML) {
        printf("<p style=\"font-family: sans-serif;color: red\">");
    }
    else if (format == OUTPUT_FORMAT_JSON) {
        printf("{\"error\": \"");
    }
    else if (format == OUTPUT_FORMAT_XML) {
        printf("<results><error>");
    }

    printf("Error processing query: ");

    fseek(error, 0, SEEK_SET);
    char buffer[4096] = {0};
    int size = fread(buffer, 1, sizeof(buffer), error);

    // JSON doesn't allow newlines
    if (format == OUTPUT_FORMAT_JSON) {
        for (char *c = buffer; *c; c++) {
            if (*c == '\n') *c = ' ';
        }
    }

    fwrite(buffer, 1, size, stdout);

    if (format == OUTPUT_FORMAT_HTML) {
        printf("</p>");
    }
    else if (format == OUTPUT_FORMAT_JSON) {
        printf("\"}");
    }
    else if (format == OUTPUT_FORMAT_XML) {
        printf("</error></results>");
    }
}

/**
 * @brief Get the session identifier
 *
 * @return char* a malloc'd pointer. Caller must free.
 */
static char *getSession () {
    char *session_id = malloc(32);

    char *cookie = getenv("HTTP_COOKIE");

    if (cookie != NULL) {
        char *start_ptr = strstr(cookie, "CSVDB_SESSION=");
        if (start_ptr != NULL) {
            start_ptr += sizeof("CSVDB_SESSION=");

            char *end_ptr = strstr(start_ptr, ";");

            if (end_ptr) {
                int len = end_ptr - start_ptr;
                strncpy(session_id, start_ptr, len);
                session_id[len] = '\0';
            }
            else {
                strcpy(session_id, start_ptr);
            }

            return session_id;
        }
    }

    unsigned int r1 = rand();
    unsigned int r2 = rand();

    sprintf(session_id, "%08x%08x", r1, r2);

    time_t t = time(NULL);
    t += 7 * 24 * 60 * 60;

    struct tm *expires = localtime(&t);

    fprintf(
        stdout,
        // asctime() adds \n at end
        "Set-Cookie: CSVDB_SESSION=%s; path=/; expires=%s",
        session_id,
        asctime(expires)
    );

    return session_id;
}

void set_session_temp_db (const char * session_id) {
    char temp_mapping_filename[MAX_TABLE_LENGTH];

    sprintf(temp_mapping_filename, "/tmp/csvdb.session.%s.temp.csv", session_id);

    struct DB *temp_mapping = temp_openMappingDB(temp_mapping_filename);

    temp_setMappingDB(temp_mapping);
}
