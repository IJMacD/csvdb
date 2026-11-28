#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <ctype.h>
#include <errno.h>

#include <unistd.h>
#include <sys/stat.h>

#include "structs.h"
#include "query/query.h"
#include "db/temp.h"

int debug_verbosity = 0;

/*
 * CGI Spec:
 *
 * GET  /cgi-bin/csvdb?query=<url-encoded-sql>[&format=...]
 * POST /cgi-bin/csvdb  with Content-Type: text/sql  (body = raw SQL)
 *      Optional query-string format=... still applies.
 *
 * $PATH_INFO = /extra/path
 * $QUERY_STRING = query=string
 * $HTTP_ACCEPT = Accept header (also selects output format)
 * $CONTENT_TYPE / $CONTENT_LENGTH for POST bodies
 *
 * POST data: stdin
 */

#define MAX_POST_BODY (1024 * 1024)

static void urldecode2(char *dst, const char *src);

static int setDataDir(const char *datadir);

static void printError(int format, FILE *error);

static char *getSession(void);

void set_session_temp_db(const char *session_id);

static int content_type_is(const char *content_type, const char *media_type);

static int parse_format_name(const char *value);

static int format_from_accept(const char *accept_env);

static void apply_query_params(char *query_string, int *format, char **query_out);

static char *read_post_body(size_t max_size);

static void print_cors_headers(void);

static void print_content_type(int format);

int main(void)
{
    char *query = NULL;
    int flags = OUTPUT_OPTION_HEADERS | OUTPUT_OPTION_STATS;
    int format = OUTPUT_FORMAT_TABLE;

    FILE *output = stdout;

    srand((unsigned)time(NULL) * getpid());

    // open tmp file to collect errors
    FILE *error_log = fopen("/tmp/csvdb_error", "w+");

    // Redirect stderr -> error file
    dup2(fileno(error_log), STDERR_FILENO);

    char *data_dir = getenv("CSVDB_DATA_DIR");
    if (data_dir)
    {
        if (setDataDir(data_dir) < 0)
        {
            fprintf(stderr, "Unable to chdir\n");
            fclose(error_log);
            return -1;
        }
    }

    char *request_method = getenv("REQUEST_METHOD");
    if (request_method == NULL)
    {
        request_method = "GET";
    }

    // CORS preflight for browser clients posting text/sql
    if (strcasecmp(request_method, "OPTIONS") == 0)
    {
        printf("Status: 204 No Content\n");
        print_cors_headers();
        printf("Access-Control-Allow-Methods: GET, POST, OPTIONS\n");
        printf("Access-Control-Allow-Headers: Content-Type, Accept\n");
        printf("Access-Control-Max-Age: 86400\n");
        printf("\n");
        fclose(error_log);
        return 0;
    }

    format = format_from_accept(getenv("HTTP_ACCEPT"));

    // Don't modify char * returned from getenv — copy first
    char *query_string_env = getenv("QUERY_STRING");
    char *query_string = NULL;
    if (query_string_env != NULL && query_string_env[0] != '\0')
    {
        query_string = strdup(query_string_env);
        if (query_string == NULL)
        {
            printf("Status: 500 Internal Server Error\n");
            print_cors_headers();
            printf("Content-Type: text/plain\n\n");
            printf("Out of memory\n");
            fclose(error_log);
            return -1;
        }
        apply_query_params(query_string, &format, &query);
        free(query_string);
    }

    // POST body as raw SQL when Content-Type is text/sql
    if ((query == NULL || query[0] == '\0') &&
        strcasecmp(request_method, "POST") == 0)
    {
        char *content_type = getenv("CONTENT_TYPE");
        if (content_type_is(content_type, "text/sql"))
        {
            free(query);
            query = read_post_body(MAX_POST_BODY);
            if (query == NULL)
            {
                printf("Status: 400 Bad Request\n");
                print_cors_headers();
                printf("Content-Type: text/plain\n\n");
                printf("Unable to read POST body as text/sql\n");
                fclose(error_log);
                return -1;
            }
        }
    }

    if (query == NULL || query[0] == '\0')
    {
        printf("Status: 400 Bad Request\n");
        print_cors_headers();
        printf("Content-Type: text/plain\n\n");
        printf("No query was provided (use ?query=... or POST text/sql)\n");
        free(query);
        fclose(error_log);
        return -1;
    }

    char *session = getSession();

    if (session == NULL)
    {
        fprintf(stderr, "Unable to create session\n");
        free(query);
        fclose(error_log);
        return -1;
    }

    set_session_temp_db(session);

    flags |= format;

    print_cors_headers();
    print_content_type(format);

    // Double newline to end headers
    printf("\n");

    int result = runQueries(query, flags, output);
    if (result < 0)
    {
        // Write errors to stdout now
        printError(format, error_log);
        free(query);
        free(session);
        fclose(error_log);
        return result;
    }

    free(query);
    free(session);
    fclose(error_log);
    return 0;
}

static void print_cors_headers(void)
{
    printf("Access-Control-Allow-Origin: *\n");
}

static void print_content_type(int format)
{
    if (format == OUTPUT_FORMAT_COMMA)
    {
        printf("Content-Type: text/csv; charset=utf-8\n");
    }
    else if (format == OUTPUT_FORMAT_TAB)
    {
        printf("Content-Type: text/tab-separated-values; charset=utf-8\n");
    }
    else if (format == OUTPUT_FORMAT_HTML)
    {
        printf("Content-Type: text/html; charset=utf-8\n");
    }
    else if (
        format == OUTPUT_FORMAT_JSON || format == OUTPUT_FORMAT_JSON_ARRAY)
    {
        printf("Content-Type: application/json; charset=utf-8\n");
    }
    else if (format == OUTPUT_FORMAT_SQL_INSERT)
    {
        printf("Content-Type: application/sql; charset=utf-8\n");
    }
    else if (format == OUTPUT_FORMAT_XML)
    {
        printf("Content-Type: application/xml; charset=utf-8\n");
    }
    else
    {
        printf("Content-Type: text/plain; charset=utf-8\n");
    }
}

/**
 * Returns 1 when content_type's media type matches media_type
 * (ignoring parameters such as "; charset=utf-8").
 */
static int content_type_is(const char *content_type, const char *media_type)
{
    size_t len;

    if (content_type == NULL || media_type == NULL)
    {
        return 0;
    }

    while (*content_type == ' ' || *content_type == '\t')
    {
        content_type++;
    }

    len = strlen(media_type);
    if (strncasecmp(content_type, media_type, len) != 0)
    {
        return 0;
    }

    return content_type[len] == '\0' || content_type[len] == ';' ||
           content_type[len] == ' ' || content_type[len] == '\t';
}

static int parse_format_name(const char *value)
{
    if (strcmp(value, "txt") == 0 || strcmp(value, "markdown") == 0)
    {
        return OUTPUT_FORMAT_TABLE;
    }
    if (strcmp(value, "csv") == 0)
    {
        return OUTPUT_FORMAT_COMMA;
    }
    if (strcmp(value, "tsv") == 0)
    {
        return OUTPUT_FORMAT_TAB;
    }
    if (strcmp(value, "html") == 0)
    {
        return OUTPUT_FORMAT_HTML;
    }
    if (strcmp(value, "json") == 0)
    {
        return OUTPUT_FORMAT_JSON;
    }
    if (strcmp(value, "json_array") == 0)
    {
        return OUTPUT_FORMAT_JSON_ARRAY;
    }
    if (strcmp(value, "sql") == 0)
    {
        return OUTPUT_FORMAT_SQL_INSERT;
    }
    if (strcmp(value, "xml") == 0)
    {
        return OUTPUT_FORMAT_XML;
    }
    return -1;
}

static int format_from_accept(const char *accept_env)
{
    if (accept_env == NULL)
    {
        return OUTPUT_FORMAT_TABLE;
    }

    // Prefer scripting types that browsers do not send by default. Check
    // text/html before application/xml — typical browser Accept lists both.
    if (strstr(accept_env, "text/csv") != NULL)
    {
        return OUTPUT_FORMAT_COMMA;
    }
    if (strstr(accept_env, "text/tab-separated-values") != NULL)
    {
        return OUTPUT_FORMAT_TAB;
    }
    if (strstr(accept_env, "application/json") != NULL)
    {
        return OUTPUT_FORMAT_JSON;
    }
    if (strstr(accept_env, "application/sql") != NULL)
    {
        return OUTPUT_FORMAT_SQL_INSERT;
    }
    if (strstr(accept_env, "text/html") != NULL)
    {
        return OUTPUT_FORMAT_HTML;
    }
    if (strstr(accept_env, "application/xml") != NULL)
    {
        return OUTPUT_FORMAT_XML;
    }

    return OUTPUT_FORMAT_TABLE;
}

/**
 * Parses application/x-www-form-urlencoded query string params into
 * *format and optionally *query_out (malloc'd, caller frees).
 */
static void apply_query_params(char *query_string, int *format, char **query_out)
{
    while (*query_string != '\0')
    {
        char *eq = strchr(query_string, '=');
        if (eq == NULL)
        {
            break;
        }
        *eq = '\0';
        char *key = query_string;
        query_string = eq + 1;

        char *amp = strchr(query_string, '&');
        char *value = query_string;
        if (amp != NULL)
        {
            *amp = '\0';
            query_string = amp + 1;
        }
        else
        {
            query_string += strlen(query_string);
        }

        if (strcmp(key, "format") == 0)
        {
            int parsed = parse_format_name(value);
            if (parsed >= 0)
            {
                *format = parsed;
            }
        }
        else if (strcmp(key, "query") == 0)
        {
            char *decoded = malloc(strlen(value) + 1);
            if (decoded == NULL)
            {
                continue;
            }
            urldecode2(decoded, value);
            free(*query_out);
            *query_out = decoded;
        }
    }
}

/**
 * Reads up to max_size bytes from stdin based on CONTENT_LENGTH.
 * Returns a malloc'd NUL-terminated string, or NULL on failure.
 */
static char *read_post_body(size_t max_size)
{
    char *len_env = getenv("CONTENT_LENGTH");
    size_t length;
    char *end = NULL;
    char *buffer;
    size_t got;

    if (len_env == NULL || len_env[0] == '\0')
    {
        return NULL;
    }

    errno = 0;
    length = strtoul(len_env, &end, 10);
    if (errno != 0 || end == len_env || *end != '\0')
    {
        return NULL;
    }

    if (length == 0 || length > max_size)
    {
        return NULL;
    }

    buffer = malloc(length + 1);
    if (buffer == NULL)
    {
        return NULL;
    }

    got = fread(buffer, 1, length, stdin);
    if (got != length)
    {
        free(buffer);
        return NULL;
    }

    buffer[length] = '\0';
    return buffer;
}

static void urldecode2(char *dst, const char *src)
{
    char a, b;
    while (*src)
    {
        if ((*src == '%') &&
            ((a = src[1]) && (b = src[2])) &&
            (isxdigit(a) && isxdigit(b)))
        {
            if (a >= 'a')
                a -= 'a' - 'A';
            if (a >= 'A')
                a -= ('A' - 10);
            else
                a -= '0';
            if (b >= 'a')
                b -= 'a' - 'A';
            if (b >= 'A')
                b -= ('A' - 10);
            else
                b -= '0';
            *dst++ = 16 * a + b;
            src += 3;
        }
        else if (*src == '+')
        {
            *dst++ = ' ';
            src++;
        }
        else
        {
            *dst++ = *src++;
        }
    }
    *dst++ = '\0';
}

static int setDataDir(const char *datadir)
{
    if (datadir != NULL)
    {
        if (chdir(datadir))
        {
            if (errno == ENOENT)
            {
                if (mkdir(datadir, S_IRWXU | S_IRGRP | S_IXGRP))
                {
                    printf("Status: 500 Internal Server Error\n");
                    printf("Content-Type: text/plain\n\n");
                    printf("%s\n", strerror(errno));
                    return -1;
                }
                if (chdir(datadir))
                {
                    printf("Status: 500 Internal Server Error\n");
                    printf("Content-Type: text/plain\n\n");
                    printf("%s\n", strerror(errno));
                    perror("chdir");
                    return -1;
                }
            }
            else
            {
                printf("Status: 500 Internal Server Error\n");
                printf("Content-Type: text/plain\n\n");
                printf("%s\n", strerror(errno));
                return -1;
            }
        }
    }
    return 0;
}

/**
 * Copies accumulated errors from `error_file` (set to /tmp/csvdb_error) to
 * stdout.
 */
static void printError(int format, FILE *error_file)
{
    if (format == OUTPUT_FORMAT_HTML)
    {
        printf("<p style=\"font-family: sans-serif;color: red\">");
    }
    else if (format == OUTPUT_FORMAT_JSON)
    {
        printf("{\"error\": \"");
    }
    else if (format == OUTPUT_FORMAT_XML)
    {
        printf("<results><error>");
    }

    printf("Error processing query: ");

    fseek(error_file, 0, SEEK_SET);
    char buffer[4096] = {0};
    int size = fread(buffer, 1, sizeof(buffer), error_file);

    // JSON doesn't allow newlines
    if (format == OUTPUT_FORMAT_JSON)
    {
        for (char *c = buffer; *c; c++)
        {
            if (*c == '\n')
                *c = ' ';
        }
    }

    fwrite(buffer, 1, size, stdout);

    if (format == OUTPUT_FORMAT_HTML)
    {
        printf("</p>");
    }
    else if (format == OUTPUT_FORMAT_JSON)
    {
        printf("\"}");
    }
    else if (format == OUTPUT_FORMAT_XML)
    {
        printf("</error></results>");
    }
}

/**
 * @brief Get the session identifier
 *
 * @return char* a malloc'd pointer. Caller must free.
 */
static char *getSession(void)
{
    char *session_id = malloc(32);

    if (session_id == NULL)
    {
        return NULL;
    }

    char *cookie = getenv("HTTP_COOKIE");

    if (cookie != NULL)
    {
        char *start_ptr = strstr(cookie, "CSVDB_SESSION=");
        if (start_ptr != NULL)
        {
            start_ptr += sizeof("CSVDB_SESSION=") - 1;

            char *end_ptr = strstr(start_ptr, ";");

            if (end_ptr)
            {
                int len = end_ptr - start_ptr;
                if (len > 31)
                {
                    len = 31;
                }
                strncpy(session_id, start_ptr, len);
                session_id[len] = '\0';
            }
            else
            {
                strncpy(session_id, start_ptr, 31);
                session_id[31] = '\0';
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
        asctime(expires));

    return session_id;
}

void set_session_temp_db(const char *session_id)
{
    char temp_mapping_filename[MAX_TABLE_LENGTH];

    sprintf(temp_mapping_filename, "/tmp/csvdb.session.%s.temp.csv", session_id);

    struct DB *temp_mapping = temp_openMappingDB(temp_mapping_filename);

    temp_setMappingDB(temp_mapping);
}
