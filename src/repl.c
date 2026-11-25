#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>
#include <termios.h>
#include <errno.h>

#include "structs.h"
#include "config.h"
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
    /*
     * OCRNL maps output CR to NL. If left on, redraws that use '\r' advance
     * to a new line instead of returning to column 0 — so each edit appears
     * to reprint the line above. Keep ONLCR so printf("\n") still yields CR+LF.
     */
    raw.c_oflag &= ~(OCRNL);
    raw.c_cc[VMIN] = 1;
    raw.c_cc[VTIME] = 0;

    if (tcsetattr(STDIN_FILENO, TCSAFLUSH, &raw) == -1)
    {
        return -1;
    }

    raw_mode_active = 1;
    return 0;
}

/**
 * Move to column 1 of the current row. Prefer CSI CHA over bare '\r' so a
 * mis-set OCRNL cannot turn the home movement into a line feed.
 */
static void moveToLineStart(void)
{
    printf("\033[1G");
}

static void redrawLine(
    const char *prompt,
    const char *buf,
    size_t len,
    size_t cursor)
{
    /* Move to start of line, print prompt+buffer, clear tail, place cursor. */
    moveToLineStart();
    printf("%s", prompt);
    if (len > 0)
    {
        fwrite(buf, 1, len, stdout);
    }
    printf("\033[K");
    /* Reposition by reprinting up to the cursor (avoids CUB wrap quirks). */
    moveToLineStart();
    printf("%s", prompt);
    if (cursor > 0)
    {
        fwrite(buf, 1, cursor, stdout);
    }
    fflush(stdout);
}

/**
 * After a mid-line insert or delete, rewrite the suffix from the cursor
 * without a full-line home+reprint (so the row above is never touched).
 */
static void redrawSuffix(
    const char *buf,
    size_t len,
    size_t cursor)
{
    if (cursor < len)
    {
        fwrite(buf + cursor, 1, len - cursor, stdout);
    }
    printf("\033[K");
    if (cursor < len)
    {
        printf("\033[%zuD", len - cursor);
    }
    fflush(stdout);
}

static void moveCursorLeft(void)
{
    printf("\033[D");
    fflush(stdout);
}

static void moveCursorRight(void)
{
    printf("\033[C");
    fflush(stdout);
}

/**
 * Read a CSI / SS3 escape sequence after the initial ESC byte.
 * Sets *final to the terminating byte (e.g. 'D' for left) and *param to the
 * leading numeric CSI parameter (0 if none). Returns 0 on failure, 1 on success.
 */
static int readEscapeSequence(int *final, int *param)
{
    unsigned char c;
    int value = 0;
    int have_digit = 0;

    *final = 0;
    *param = 0;

    if (read(STDIN_FILENO, &c, 1) != 1)
    {
        return 0;
    }

    /* SS3: ESC O A/B/C/D (application cursor keys) */
    if (c == 'O')
    {
        if (read(STDIN_FILENO, &c, 1) != 1)
        {
            return 0;
        }
        *final = (int)c;
        return 1;
    }

    /* CSI: ESC [ ... final */
    if (c != '[')
    {
        return 0;
    }

    while (1)
    {
        if (read(STDIN_FILENO, &c, 1) != 1)
        {
            return 0;
        }

        if (c >= '0' && c <= '9')
        {
            /* Accumulate only until the first parameter is committed. */
            if (*param == 0 && !have_digit)
            {
                value = c - '0';
                have_digit = 1;
            }
            else if (*param == 0 && have_digit)
            {
                value = value * 10 + (c - '0');
            }
            continue;
        }

        if (c == ';')
        {
            /* Commit first parameter; ignore modifier parameters after. */
            if (*param == 0 && have_digit)
            {
                *param = value;
            }
            have_digit = 0;
            value = 0;
            continue;
        }

        if (c >= 0x40 && c <= 0x7E)
        {
            if (*param == 0 && have_digit)
            {
                *param = value;
            }
            *final = (int)c;
            return 1;
        }

        if (c < 0x20 || c > 0x3F)
        {
            return 0;
        }
        /* Other intermediate bytes — ignore */
    }
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
                if (cursor == len)
                {
                    /* At end: erase the last cell in place. */
                    printf("\b \b");
                    fflush(stdout);
                }
                else
                {
                    /* In the middle: move left, then rewrite the tail. */
                    printf("\b");
                    redrawSuffix(buf, len, cursor);
                }
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
            int final = 0;
            int param = 0;
            if (!readEscapeSequence(&final, &param))
            {
                continue;
            }

            if (final == 'D') /* Left */
            {
                if (cursor > 0)
                {
                    cursor--;
                    moveCursorLeft();
                }
            }
            else if (final == 'C') /* Right */
            {
                if (cursor < len)
                {
                    cursor++;
                    moveCursorRight();
                }
            }
            else if (final == 'A') /* Up */
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
            else if (final == 'B') /* Down */
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
            else if (final == 'H') /* Home */
            {
                if (cursor > 0)
                {
                    cursor = 0;
                    redrawLine(prompt, buf, len, cursor);
                }
            }
            else if (final == 'F') /* End */
            {
                if (cursor < len)
                {
                    cursor = len;
                    redrawLine(prompt, buf, len, cursor);
                }
            }
            else if (final == '~')
            {
                /* CSI n~ : 1/7 Home, 4/8 End, 3 Delete */
                if (param == 1 || param == 7)
                {
                    if (cursor > 0)
                    {
                        cursor = 0;
                        redrawLine(prompt, buf, len, cursor);
                    }
                }
                else if (param == 4 || param == 8)
                {
                    if (cursor < len)
                    {
                        cursor = len;
                        redrawLine(prompt, buf, len, cursor);
                    }
                }
                else if (param == 3)
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
            /* Write new char + remainder from the current cursor cell. */
            fwrite(buf + cursor - 1, 1, len - (cursor - 1), stdout);
            printf("\033[K");
            if (cursor < len)
            {
                printf("\033[%zuD", len - cursor);
            }
            fflush(stdout);
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

        if (strcmp(token, ".headers") == 0)
        {
            getToken(statement, &index, token, 32);

            if (token[0] == '\0')
            {
                options ^= OUTPUT_OPTION_HEADERS;
            }
            else if (
                strcmp(token, "on") == 0 ||
                strcmp(token, "1") == 0 ||
                strcmp(token, "true") == 0)
            {
                options |= OUTPUT_OPTION_HEADERS;
            }
            else if (
                strcmp(token, "off") == 0 ||
                strcmp(token, "0") == 0 ||
                strcmp(token, "false") == 0)
            {
                options &= ~OUTPUT_OPTION_HEADERS;
            }
            else
            {
                fprintf(stderr, "Usage: .headers [on|off]\n");
                free(statement);
                continue;
            }

            printf(
                "Headers: %s\n",
                (options & OUTPUT_OPTION_HEADERS) ? "ON" : "OFF");
            free(statement);
            continue;
        }

        if (strcmp(token, ".format") == 0)
        {
            getToken(statement, &index, token, 32);

            int format = csvdb_parse_format(token);
            if (format < 0)
            {
                fprintf(
                    stderr,
                    "Unknown format '%s'. "
                    "See .help for supported formats.\n",
                    token);
                free(statement);
                continue;
            }

            options = (options & ~OUTPUT_MASK_FORMAT) | format;

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
        "\t.format tsv|csv|html|table|box|json|json:array|sql|sql:values|xml|record\n"
        "\t.headers [on|off]\n"
        "\t.explain\n");
}
