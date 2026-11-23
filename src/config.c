#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "config.h"

static struct CsvdbConfig g_config;

enum ConfigSection {
    SECTION_NONE = 0,
    SECTION_OUTPUT,
    SECTION_PATHS,
    SECTION_DEFAULTS
};

static void skip_ws(const char **p)
{
    while (**p != '\0' && isspace((unsigned char)**p))
    {
        (*p)++;
    }
}

static void rtrim(char *s)
{
    size_t n = strlen(s);
    while (n > 0 && isspace((unsigned char)s[n - 1]))
    {
        s[--n] = '\0';
    }
}

/**
 * Strip a trailing `#` comment that is not inside quotes.
 */
static void strip_comment(char *line)
{
    int in_quote = 0;
    for (char *p = line; *p != '\0'; p++)
    {
        if (*p == '"' && (p == line || p[-1] != '\\'))
        {
            in_quote = !in_quote;
        }
        else if (*p == '#' && !in_quote)
        {
            *p = '\0';
            break;
        }
    }
}

/**
 * Copy a TOML-ish string or bare token into out.
 * Accepts "quoted", 'quoted', or bare values.
 * Returns 0 on success, -1 on failure.
 */
static int parse_value_token(const char **pp, char *out, size_t out_sz)
{
    const char *p = *pp;
    skip_ws(&p);

    if (*p == '\0')
    {
        return -1;
    }

    char quote = 0;
    if (*p == '"' || *p == '\'')
    {
        quote = *p++;
    }

    size_t i = 0;
    while (*p != '\0')
    {
        if (quote)
        {
            if (*p == quote)
            {
                p++;
                break;
            }
        }
        else
        {
            if (isspace((unsigned char)*p) || *p == ',' || *p == ']' || *p == '#')
            {
                break;
            }
        }

        if (i + 1 >= out_sz)
        {
            return -1;
        }

        out[i++] = *p++;
    }

    out[i] = '\0';
    *pp = p;
    return i > 0 || quote ? 0 : -1;
}

/**
 * Expand a leading ~/ or bare ~ to $HOME. Writes into out.
 * Returns 0 on success.
 */
static int expand_path(const char *in, char *out, size_t out_sz)
{
    if (in[0] != '~')
    {
        if (strlen(in) >= out_sz)
        {
            return -1;
        }
        strcpy(out, in);
        return 0;
    }

    const char *home = getenv("HOME");
    if (home == NULL || home[0] == '\0')
    {
        return -1;
    }

    const char *rest = in + 1;
    if (*rest == '/')
    {
        rest++;
    }
    else if (*rest != '\0')
    {
        /* ~user is not supported */
        return -1;
    }

    if (*rest == '\0')
    {
        if (strlen(home) >= out_sz)
        {
            return -1;
        }
        strcpy(out, home);
        return 0;
    }

    int n = snprintf(out, out_sz, "%s/%s", home, rest);
    if (n < 0 || (size_t)n >= out_sz)
    {
        return -1;
    }

    return 0;
}

static int parse_bool(const char *val, int *out)
{
    if (
        strcmp(val, "true") == 0 ||
        strcmp(val, "yes") == 0 ||
        strcmp(val, "1") == 0 ||
        strcmp(val, "on") == 0)
    {
        *out = 1;
        return 0;
    }

    if (
        strcmp(val, "false") == 0 ||
        strcmp(val, "no") == 0 ||
        strcmp(val, "0") == 0 ||
        strcmp(val, "off") == 0)
    {
        *out = 0;
        return 0;
    }

    return -1;
}

static void trim_trailing_slashes(char *path)
{
    size_t n = strlen(path);
    while (n > 1 && path[n - 1] == '/')
    {
        path[--n] = '\0';
    }
}

static int add_path_entry(
    char dest[][CSVDB_MAX_PATH_LEN],
    int *count,
    int max_count,
    const char *raw,
    const char *what)
{
    if (*count >= max_count)
    {
        fprintf(
            stderr,
            "csvdb config: ignoring %s entry (max %d)\n",
            what,
            max_count);
        return -1;
    }

    char expanded[CSVDB_MAX_PATH_LEN];
    if (expand_path(raw, expanded, sizeof(expanded)) != 0)
    {
        fprintf(stderr, "csvdb config: bad %s path: %s\n", what, raw);
        return -1;
    }

    trim_trailing_slashes(expanded);
    strcpy(dest[*count], expanded);
    (*count)++;
    return 0;
}

/**
 * Parse key = [ "a", "b" ] starting at the '[' (or value after =).
 * Supports values continuing on following lines until ']'.
 */
static int parse_path_array(
    char dest[][CSVDB_MAX_PATH_LEN],
    int *count,
    int max_count,
    const char *what,
    const char *after_eq,
    FILE *f,
    char *line_buf,
    size_t line_buf_sz)
{
    const char *p = after_eq;
    skip_ws(&p);

    if (*p != '[')
    {
        fprintf(stderr, "csvdb config: %s expects an array\n", what);
        return -1;
    }
    p++;

    while (1)
    {
        skip_ws(&p);
        while (*p == '\0')
        {
            if (fgets(line_buf, (int)line_buf_sz, f) == NULL)
            {
                fprintf(stderr, "csvdb config: unclosed %s array\n", what);
                return -1;
            }
            strip_comment(line_buf);
            rtrim(line_buf);
            p = line_buf;
            skip_ws(&p);
        }

        if (*p == ']')
        {
            return 0;
        }

        char token[CSVDB_MAX_PATH_LEN];
        if (parse_value_token(&p, token, sizeof(token)) != 0)
        {
            fprintf(stderr, "csvdb config: bad %s value\n", what);
            return -1;
        }

        add_path_entry(dest, count, max_count, token, what);

        skip_ws(&p);
        if (*p == ',')
        {
            p++;
            continue;
        }
        if (*p == ']')
        {
            return 0;
        }
        if (*p == '\0')
        {
            continue;
        }

        fprintf(stderr, "csvdb config: bad %s list syntax\n", what);
        return -1;
    }
}

static void parse_file(struct CsvdbConfig *cfg, FILE *f)
{
    char line[1024];
    enum ConfigSection section = SECTION_NONE;

    while (fgets(line, sizeof(line), f) != NULL)
    {
        strip_comment(line);
        rtrim(line);

        const char *p = line;
        skip_ws(&p);
        if (*p == '\0')
        {
            continue;
        }

        if (*p == '[')
        {
            const char *end = strchr(p, ']');
            if (end == NULL)
            {
                fprintf(stderr, "csvdb config: bad section header\n");
                continue;
            }

            char name[64];
            size_t n = (size_t)(end - p - 1);
            if (n >= sizeof(name))
            {
                n = sizeof(name) - 1;
            }
            memcpy(name, p + 1, n);
            name[n] = '\0';

            if (strcmp(name, "output") == 0)
            {
                section = SECTION_OUTPUT;
            }
            else if (strcmp(name, "paths") == 0)
            {
                section = SECTION_PATHS;
            }
            else if (strcmp(name, "defaults") == 0)
            {
                section = SECTION_DEFAULTS;
            }
            else
            {
                section = SECTION_NONE;
            }
            continue;
        }

        const char *eq = strchr(p, '=');
        if (eq == NULL)
        {
            continue;
        }

        char key[64];
        size_t key_len = (size_t)(eq - p);
        while (key_len > 0 && isspace((unsigned char)p[key_len - 1]))
        {
            key_len--;
        }
        if (key_len == 0 || key_len >= sizeof(key))
        {
            continue;
        }
        memcpy(key, p, key_len);
        key[key_len] = '\0';

        const char *val_start = eq + 1;

        if (section == SECTION_OUTPUT && strcmp(key, "format") == 0)
        {
            char token[64];
            const char *vp = val_start;
            if (parse_value_token(&vp, token, sizeof(token)) != 0)
            {
                fprintf(stderr, "csvdb config: bad format value\n");
                continue;
            }
            int fmt = csvdb_parse_format(token);
            if (fmt < 0)
            {
                fprintf(stderr, "csvdb config: unrecognised format: %s\n", token);
                continue;
            }
            cfg->has_format = 1;
            cfg->format = (enum OutputOption)fmt;
        }
        else if (section == SECTION_OUTPUT && strcmp(key, "headers") == 0)
        {
            char token[64];
            const char *vp = val_start;
            if (parse_value_token(&vp, token, sizeof(token)) != 0)
            {
                fprintf(stderr, "csvdb config: bad headers value\n");
                continue;
            }
            int headers = 0;
            if (parse_bool(token, &headers) != 0)
            {
                fprintf(stderr, "csvdb config: bad headers value: %s\n", token);
                continue;
            }
            cfg->has_headers = 1;
            cfg->headers = headers;
        }
        else if (section == SECTION_DEFAULTS && strcmp(key, "stats") == 0)
        {
            char token[64];
            const char *vp = val_start;
            if (parse_value_token(&vp, token, sizeof(token)) != 0)
            {
                fprintf(stderr, "csvdb config: bad stats value\n");
                continue;
            }
            int stats = 0;
            if (parse_bool(token, &stats) != 0)
            {
                fprintf(stderr, "csvdb config: bad stats value: %s\n", token);
                continue;
            }
            cfg->has_stats = 1;
            cfg->stats = stats;
        }
        else if (section == SECTION_PATHS && strcmp(key, "index_dirs") == 0)
        {
            parse_path_array(
                cfg->index_dirs,
                &cfg->index_dir_count,
                CSVDB_MAX_INDEX_DIRS,
                "index_dirs",
                val_start,
                f,
                line,
                sizeof(line));
        }
        else if (section == SECTION_PATHS && strcmp(key, "data_dirs") == 0)
        {
            parse_path_array(
                cfg->data_dirs,
                &cfg->data_dir_count,
                CSVDB_MAX_DATA_DIRS,
                "data_dirs",
                val_start,
                f,
                line,
                sizeof(line));
        }
        else if (section == SECTION_PATHS && strcmp(key, "index_write_dir") == 0)
        {
            char token[CSVDB_MAX_PATH_LEN];
            const char *vp = val_start;
            if (parse_value_token(&vp, token, sizeof(token)) != 0)
            {
                fprintf(stderr, "csvdb config: bad index_write_dir value\n");
                continue;
            }
            char expanded[CSVDB_MAX_PATH_LEN];
            if (expand_path(token, expanded, sizeof(expanded)) != 0)
            {
                fprintf(
                    stderr,
                    "csvdb config: bad index_write_dir path: %s\n",
                    token);
                continue;
            }
            trim_trailing_slashes(expanded);
            strcpy(cfg->index_write_dir, expanded);
            cfg->has_index_write_dir = 1;
        }
    }
}

static int resolve_config_path(char *out, size_t out_sz)
{
    const char *env = getenv("CSVDB_CONFIG");
    if (env != NULL)
    {
        if (env[0] == '\0')
        {
            return -1; /* explicitly disabled */
        }
        if (strlen(env) >= out_sz)
        {
            return -1;
        }
        strcpy(out, env);
        return 0;
    }

    const char *xdg = getenv("XDG_CONFIG_HOME");
    if (xdg != NULL && xdg[0] != '\0')
    {
        int n = snprintf(out, out_sz, "%s/csvdb/config", xdg);
        if (n < 0 || (size_t)n >= out_sz)
        {
            return -1;
        }
        return 0;
    }

    const char *home = getenv("HOME");
    if (home == NULL || home[0] == '\0')
    {
        return -1;
    }

    int n = snprintf(out, out_sz, "%s/.config/csvdb/config", home);
    if (n < 0 || (size_t)n >= out_sz)
    {
        return -1;
    }
    return 0;
}

void csvdb_config_load(void)
{
    if (g_config.loaded)
    {
        return;
    }

    memset(&g_config, 0, sizeof(g_config));
    g_config.loaded = 1;

    char path[CSVDB_MAX_PATH_LEN];
    if (resolve_config_path(path, sizeof(path)) != 0)
    {
        return;
    }

    FILE *f = fopen(path, "r");
    if (f == NULL)
    {
        return;
    }

    parse_file(&g_config, f);
    fclose(f);
}

const struct CsvdbConfig *csvdb_config(void)
{
    csvdb_config_load();
    return &g_config;
}

const char *csvdb_path_basename(const char *path)
{
    const char *base = path;
    if (path == NULL)
    {
        return "";
    }

    for (const char *p = path; *p != '\0'; p++)
    {
        if (*p == '/')
        {
            base = p + 1;
        }
    }

    return base;
}

int csvdb_parse_format(const char *format_val)
{
    if (format_val == NULL)
    {
        return -1;
    }

    if (strcmp(format_val, "tsv") == 0)
    {
        return OUTPUT_FORMAT_TAB;
    }

    if (strcmp(format_val, "csv") == 0)
    {
        return OUTPUT_FORMAT_COMMA;
    }

    if (strcmp(format_val, "csv:excel") == 0)
    {
        return OUTPUT_FORMAT_CSV_EXCEL;
    }

    if (strcmp(format_val, "html") == 0)
    {
        return OUTPUT_FORMAT_HTML;
    }

    if (
        strcmp(format_val, "json") == 0 ||
        strcmp(format_val, "json:object") == 0)
    {
        return OUTPUT_FORMAT_JSON;
    }

    if (
        strcmp(format_val, "json:array") == 0 ||
        strcmp(format_val, "json_array") == 0)
    {
        return OUTPUT_FORMAT_JSON_ARRAY;
    }

    if (strcmp(format_val, "table") == 0)
    {
        return OUTPUT_FORMAT_TABLE;
    }

    if (strcmp(format_val, "record") == 0)
    {
        return OUTPUT_FORMAT_INFO_SEP;
    }

    if (strcmp(format_val, "xml") == 0)
    {
        return OUTPUT_FORMAT_XML;
    }

    if (
        strcmp(format_val, "sql") == 0 ||
        strcmp(format_val, "sql:insert") == 0)
    {
        return OUTPUT_FORMAT_SQL_INSERT;
    }

    if (
        strcmp(format_val, "sql:values") == 0 ||
        strcmp(format_val, "sql_values") == 0)
    {
        return OUTPUT_FORMAT_SQL_VALUES;
    }

    if (
        strcmp(format_val, "sql:create") == 0 ||
        strcmp(format_val, "sql_create") == 0)
    {
        return OUTPUT_FORMAT_SQL_CREATE;
    }

    if (strcmp(format_val, "box") == 0)
    {
        return OUTPUT_FORMAT_BOX;
    }

    return -1;
}
