#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>
#include <termios.h>
#include <errno.h>

#include "structs.h"
#include "query/query.h"
#include "query/token.h"

extern char *gitversion;

#define REPL_HISTORY_MAX 64
#define REPL_LINE_CAP 4096

static void printHelp(void);
static char *readEditedLine(const char *prompt, int *interrupted);
static void historyAdd(const char *entry);
static void historyFree(void);
static int statementComplete(const char *buf, int require_semicolon);
static int isDotCommand(const char *buf);
static void trimTrailingNewline(char *s);

static char *history[REPL_HISTORY_MAX];
static int history_count = 0;

static struct termios saved_termios;
static int raw_mode_active = 0;

static void disableRawMode(void)
{
    if (raw_mode_active)
    {
        tcsetattr(STDIN_FILENO, TCSAFLUSH, &saved_termios);
        raw_mode_active = 0;
    }
}

static int enableRawMode(void)
{
    if (!isatty(STDIN_FILENO))
    {
        return -1;
    }

    if (tcgetattr(STDIN_FILENO, &saved_termios) == -1)
    {
        return -1;
    }

    struct termios raw = saved_termios;
    raw.c_lflag &= ~(ECHO | ICANON | IEXTEN);
    raw.c_iflag &= ~(IXON | ICRNL);
    raw.c_cc[VMIN] = 1;
    raw.c_cc[VTIME] = 0;

    if (tcsetattr(STDIN_FILENO, TCSAFLUSH, &raw) == -1)
    {
        return -1;
    }

    raw_mode_active = 1;
    return 0;
}

static void redrawLine(
    const char *prompt,
    const char *buf,
    size_t len,
    size_t cursor)
{
    /* Move to start of line, print prompt+buffer, clear tail, place cursor. */
    printf("\r%s", prompt);
    fwrite(buf, 1, len, stdout);
    printf("\033[K");
    printf("\r%s", prompt);
    if (cursor > 0)
    {
        fwrite(buf, 1, cursor, stdout);
    }
    fflush(stdout);
}

/**
 * Read one physical line with left/right editing and up/down history.
 * Returns a malloc'd string (without trailing newline), or NULL on EOF.
 * Sets *interrupted when the user hits Ctrl-C.
 * Falls back to getline when stdin is not a TTY.
 */
static char *readEditedLine(const char *prompt, int *interrupted)
{
    if (interrupted)
    {
        *interrupted = 0;
    }

    if (!isatty(STDIN_FILENO) || enableRawMode() != 0)
    {
        printf("%s", prompt);
        fflush(stdout);

        size_t buffer_size = 1024;
        char *line_buffer = malloc(buffer_size);
        if (line_buffer == NULL)
        {
            return NULL;
        }

        ssize_t size = getline(&line_buffer, &buffer_size, stdin);
        if (size < 0)
        {
            free(line_buffer);
            return NULL;
        }

        trimTrailingNewline(line_buffer);
        return line_buffer;
    }

    printf("%s", prompt);
    fflush(stdout);

    char buf[REPL_LINE_CAP];
    size_t len = 0;
    size_t cursor = 0;
    /* history_count means "new line"; history_count-1 .. 0 are past entries */
    int history_index = history_count;
    char *stash = NULL;

    while (1)
    {
        unsigned char c;
        ssize_t n = read(STDIN_FILENO, &c, 1);
        if (n <= 0)
        {
            disableRawMode();
            free(stash);
            if (len == 0)
            {
                printf("\n");
                return NULL;
            }
            buf[len] = '\0';
            char *out = strdup(buf);
            return out;
        }

        if (c == 4) /* Ctrl-D */
        {
            if (len == 0)
            {
                disableRawMode();
                free(stash);
                printf("\n");
                return NULL;
            }
            continue;
        }

        if (c == 3) /* Ctrl-C */
        {
            disableRawMode();
            free(stash);
            printf("^C\n");
            if (interrupted)
            {
                *interrupted = 1;
            }
            return NULL;
        }

        if (c == '\r' || c == '\n')
        {
            disableRawMode();
            free(stash);
            printf("\n");
            buf[len] = '\0';
            return strdup(buf);
        }

        if (c == 127 || c == 8) /* Backspace */
        {
            if (cursor > 0)
            {
                memmove(buf + cursor - 1, buf + cursor, len - cursor);
                cursor--;
                len--;
                redrawLine(prompt, buf, len, cursor);
            }
            continue;
        }

        if (c == 1) /* Ctrl-A */
        {
            cursor = 0;
            redrawLine(prompt, buf, len, cursor);
            continue;
        }

        if (c == 5) /* Ctrl-E */
        {
            cursor = len;
            redrawLine(prompt, buf, len, cursor);
            continue;
        }

        if (c == 27) /* Escape sequence */
        {
            unsigned char seq[2];
            if (read(STDIN_FILENO, &seq[0], 1) != 1)
            {
                continue;
            }
            if (read(STDIN_FILENO, &seq[1], 1) != 1)
            {
                continue;
            }

            if (seq[0] == '[')
            {
                if (seq[1] == 'D') /* Left */
                {
                    if (cursor > 0)
                    {
                        cursor--;
                        redrawLine(prompt, buf, len, cursor);
                    }
                }
                else if (seq[1] == 'C') /* Right */
                {
                    if (cursor < len)
                    {
                        cursor++;
                        redrawLine(prompt, buf, len, cursor);
                    }
                }
                else if (seq[1] == 'A') /* Up */
                {
                    if (history_index > 0)
                    {
                        if (history_index == history_count)
                        {
                            free(stash);
                            buf[len] = '\0';
                            stash = strdup(buf);
                        }
                        history_index--;
                        const char *h = history[history_index];
                        size_t hlen = strlen(h);
                        if (hlen >= REPL_LINE_CAP)
                        {
                            hlen = REPL_LINE_CAP - 1;
                        }
                        memcpy(buf, h, hlen);
                        len = hlen;
                        cursor = len;
                        redrawLine(prompt, buf, len, cursor);
                    }
                }
                else if (seq[1] == 'B') /* Down */
                {
                    if (history_index < history_count)
                    {
                        history_index++;
                        const char *h;
                        if (history_index == history_count)
                        {
                            h = stash ? stash : "";
                        }
                        else
                        {
                            h = history[history_index];
                        }
                        size_t hlen = strlen(h);
                        if (hlen >= REPL_LINE_CAP)
                        {
                            hlen = REPL_LINE_CAP - 1;
                        }
                        memcpy(buf, h, hlen);
                        len = hlen;
                        cursor = len;
                        redrawLine(prompt, buf, len, cursor);
                    }
                }
                else if (seq[1] == '3') /* Delete: ESC [ 3 ~ */
                {
                    unsigned char tilde;
                    if (read(STDIN_FILENO, &tilde, 1) == 1 && tilde == '~')
                    {
                        if (cursor < len)
                        {
                            memmove(
                                buf + cursor,
                                buf + cursor + 1,
                                len - cursor - 1);
                            len--;
                            redrawLine(prompt, buf, len, cursor);
                        }
                    }
                }
                else if (seq[1] == 'H') /* Home */
                {
                    cursor = 0;
                    redrawLine(prompt, buf, len, cursor);
                }
                else if (seq[1] == 'F') /* End */
                {
                    cursor = len;
                    redrawLine(prompt, buf, len, cursor);
                }
            }
            continue;
        }

        /* Ignore other control characters */
        if (c < 32)
        {
            continue;
        }

        if (len + 1 >= REPL_LINE_CAP)
        {
            continue;
        }

        if (cursor == len)
        {
            /* Fast path: append at end — no full redraw needed */
            buf[len++] = (char)c;
            cursor++;
            fwrite(&c, 1, 1, stdout);
            fflush(stdout);
        }
        else
        {
            memmove(buf + cursor + 1, buf + cursor, len - cursor);
            buf[cursor] = (char)c;
            cursor++;
            len++;
            redrawLine(prompt, buf, len, cursor);
        }
    }
}

static void historyAdd(const char *entry)
{
    if (entry == NULL || entry[0] == '\0')
    {
        return;
    }

    /* Don't store duplicates of the most recent entry */
    if (history_count > 0 && strcmp(history[history_count - 1], entry) == 0)
    {
        return;
    }

    if (history_count == REPL_HISTORY_MAX)
    {
        free(history[0]);
        memmove(history, history + 1, (REPL_HISTORY_MAX - 1) * sizeof(char *));
        history_count--;
    }

    history[history_count++] = strdup(entry);
}

static void historyFree(void)
{
    for (int i = 0; i < history_count; i++)
    {
        free(history[i]);
        history[i] = NULL;
    }
    history_count = 0;
}

static void trimTrailingNewline(char *s)
{
    size_t n = strlen(s);
    while (n > 0 && (s[n - 1] == '\n' || s[n - 1] == '\r'))
    {
        s[--n] = '\0';
    }
}

/**
 * True when buf is a dot-command (.help, .exit, ...) — no semicolon needed.
 */
static int isDotCommand(const char *buf)
{
    while (*buf && isspace((unsigned char)*buf))
    {
        buf++;
    }
    return *buf == '.';
}

/**
 * True when the buffer holds a complete SQL statement (semicolon outside
 * quotes / line comments) or a complete dot-command.
 * When require_semicolon is 0 (non-TTY), any non-empty buffer is complete.
 */
static int statementComplete(const char *buf, int require_semicolon)
{
    if (isDotCommand(buf))
    {
        return 1;
    }

    if (!require_semicolon)
    {
        return buf[0] != '\0';
    }

    int in_single = 0;
    int in_double = 0;

    for (const char *p = buf; *p; p++)
    {
        if (!in_double && *p == '\'')
        {
            /* SQL '' escape inside a string */
            if (in_single && *(p + 1) == '\'')
            {
                p++;
                continue;
            }
            in_single = !in_single;
            continue;
        }

        if (!in_single && *p == '"')
        {
            in_double = !in_double;
            continue;
        }

        if (in_single || in_double)
        {
            continue;
        }

        /* Line comment */
        if (*p == '-' && *(p + 1) == '-')
        {
            while (*p && *p != '\n')
            {
                p++;
            }
            if (*p == '\0')
            {
                break;
            }
            continue;
        }

        if (*p == ';')
        {
            return 1;
        }
    }

    return 0;
}

void repl(int options)
{
    /* Ensure a usable interactive format if caller left format unset */
    if ((options & OUTPUT_MASK_FORMAT) == 0)
    {
        options |= OUTPUT_FORMAT_BOX;
    }

    printf("CSVDB REPL\nType \".help\" for help.\n\n");

    int explain = 0;
    int require_semicolon = isatty(STDIN_FILENO);
    atexit(historyFree);
    atexit(disableRawMode);

    while (1)
    {
        char *statement = NULL;
        size_t statement_len = 0;
        int first_line = 1;
        int cancelled = 0;

        while (1)
        {
            const char *prompt = first_line ? "> " : "... ";
            int interrupted = 0;
            char *line = readEditedLine(prompt, &interrupted);

            if (interrupted)
            {
                free(line);
                free(statement);
                statement = NULL;
                cancelled = 1;
                break;
            }

            if (line == NULL)
            {
                /* EOF: run any buffered statement, then leave */
                if (statement != NULL && statement[0] != '\0')
                {
                    break;
                }
                free(statement);
                return;
            }

            if (line[0] == '\0' && first_line)
            {
                free(line);
                cancelled = 1;
                break;
            }

            if (first_line)
            {
                statement = line;
                statement_len = strlen(statement);
                first_line = 0;
            }
            else
            {
                size_t line_len = strlen(line);
                size_t need = statement_len + 1 + line_len + 1;
                char *grown = realloc(statement, need);
                if (grown == NULL)
                {
                    free(line);
                    free(statement);
                    fprintf(stderr, "Out of memory\n");
                    return;
                }
                statement = grown;
                statement[statement_len++] = '\n';
                memcpy(statement + statement_len, line, line_len + 1);
                statement_len += line_len;
                free(line);
            }

            if (statementComplete(statement, require_semicolon))
            {
                break;
            }
        }

        if (cancelled)
        {
            continue;
        }

        if (statement == NULL || statement[0] == '\0')
        {
            free(statement);
            continue;
        }

        historyAdd(statement);

        size_t index = 0;
        char token[32];
        getToken(statement, &index, token, 32);

        if (token[0] == '\0')
        {
            free(statement);
            continue;
        }

        if (strcmp(token, ".exit") == 0)
        {
            free(statement);
            break;
        }

        if (strcmp(token, ".help") == 0)
        {
            printHelp();
            free(statement);
            continue;
        }

        if (strcmp(token, ".version") == 0)
        {
            printf("Version: %s\n", gitversion);
            free(statement);
            continue;
        }

        if (strcmp(token, ".explain") == 0)
        {
            explain = !explain;
            printf("Explain: %s\n", explain ? "ON" : "OFF");
            free(statement);
            continue;
        }

        if (strcmp(token, ".format") == 0)
        {
            getToken(statement, &index, token, 32);

            options &= ~OUTPUT_MASK_FORMAT;

            if (strcmp(token, "tsv") == 0)
            {
                options |= OUTPUT_FORMAT_TAB;
            }
            else if (strcmp(token, "csv") == 0)
            {
                options |= OUTPUT_FORMAT_COMMA;
            }
            else if (strcmp(token, "html") == 0)
            {
                options |= OUTPUT_FORMAT_HTML;
            }
            else if (strcmp(token, "json_array") == 0)
            {
                options |= OUTPUT_FORMAT_JSON_ARRAY;
            }
            else if (strcmp(token, "json") == 0)
            {
                options |= OUTPUT_FORMAT_JSON;
            }
            else if (strcmp(token, "sql") == 0)
            {
                options |= OUTPUT_FORMAT_SQL_INSERT;
            }
            else if (strcmp(token, "table") == 0)
            {
                options |= OUTPUT_FORMAT_TABLE;
            }
            else if (strcmp(token, "box") == 0)
            {
                options |= OUTPUT_FORMAT_BOX;
            }
            else if (strcmp(token, "record") == 0)
            {
                options |= OUTPUT_FORMAT_INFO_SEP;
            }
            else if (strcmp(token, "xml") == 0)
            {
                options |= OUTPUT_FORMAT_XML;
            }
            else if (strcmp(token, "sql_values") == 0)
            {
                options |= OUTPUT_FORMAT_SQL_VALUES;
            }
            else
            {
                options |= OUTPUT_FORMAT_BOX;
            }

            free(statement);
            continue;
        }

        options &= ~FLAG_EXPLAIN;
        if (explain)
        {
            options |= FLAG_EXPLAIN;
        }

        const char *end_ptr;
        query(statement, options, stdout, &end_ptr);

        free(statement);
        printf("\n");
    }
}

static void printHelp(void)
{
    printf(
        "Enter SQL statements ending with ';'. Dot-commands need no semicolon.\n"
        "Up/down arrows recall history; left/right move the cursor.\n\n"
        "Commands in REPL:\n"
        "\t.help\n"
        "\t.version\n"
        "\t.exit\n"
        "\t.format tsv|csv|html|table|box|json|json_array|sql|sql_values|xml|record\n"
        "\t.explain\n");
}
