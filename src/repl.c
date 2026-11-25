#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>
#include <termios.h>
#include <errno.h>
#include <sys/ioctl.h>
#include <setjmp.h>

#include "structs.h"
#include "config.h"
#include "fatal.h"
#include "query/query.h"
#include "query/token.h"

extern char *gitversion;

#define REPL_HISTORY_MAX 64
#define REPL_LINE_CAP 4096

static void printHelp(void);
static char *readEditedLine(const char *prompt, int *interrupted);
static char *readEditedStatement(int *interrupted, int require_semicolon);
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

static int termCols(void)
{
    struct winsize ws;
    if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &ws) == 0 && ws.ws_col > 0)
    {
        return (int)ws.ws_col;
    }
    return 80;
}

static const char *linePrompt(int line_index)
{
    return line_index == 0 ? "> " : "... ";
}

static int linePromptLen(int line_index)
{
    return line_index == 0 ? 2 : 4;
}

/**
 * Map a buffer index to a visual (row, col) within the edit block.
 * Soft-wraps at the terminal width. Sets *total_rows to rows used by buf.
 */
static void layoutIndex(
    const char *buf,
    size_t len,
    size_t index,
    int cols,
    int *out_row,
    int *out_col,
    int *total_rows)
{
    int row = 0;
    int col = linePromptLen(0);
    int line_index = 0;
    int max_row = 0;

    if (index == 0)
    {
        *out_row = 0;
        *out_col = col;
    }

    for (size_t i = 0; i < len; i++)
    {
        if (buf[i] == '\n')
        {
            row++;
            line_index++;
            col = linePromptLen(line_index);
        }
        else
        {
            if (col >= cols)
            {
                row++;
                col = 0;
            }
            col++;
        }

        if (row > max_row)
        {
            max_row = row;
        }

        if (i + 1 == index)
        {
            *out_row = row;
            *out_col = col;
        }
    }

    if (len == 0 || index == len)
    {
        *out_row = row;
        *out_col = col;
    }

    *total_rows = max_row + 1;
}

/**
 * Full redraw of a (possibly multi-line) edit buffer.
 * *drawn_rows / *drawn_cursor_row describe the previous draw so we can move
 * back to the block start even when the cursor was not on the last row.
 * Prefer CSI CHA over bare CR (OCRNL-safe).
 */
static void redrawBuffer(
    const char *buf,
    size_t len,
    size_t cursor,
    int *drawn_rows,
    int *drawn_cursor_row)
{
    int cols = termCols();
    int cur_row = 0;
    int cur_col = 0;
    int total_rows = 1;
    layoutIndex(buf, len, cursor, cols, &cur_row, &cur_col, &total_rows);

    int end_row = 0;
    int end_col = 0;
    int end_total = 1;
    layoutIndex(buf, len, len, cols, &end_row, &end_col, &end_total);

    int clear_rows = total_rows > *drawn_rows ? total_rows : *drawn_rows;
    if (clear_rows < 1)
    {
        clear_rows = 1;
    }

    /* Move to the first row of the previous draw from wherever the cursor is. */
    if (*drawn_cursor_row > 0)
    {
        printf("\033[%dA", *drawn_cursor_row);
    }
    printf("\033[1G");

    /* Clear the old block. */
    for (int r = 0; r < clear_rows; r++)
    {
        printf("\033[K");
        if (r + 1 < clear_rows)
        {
            printf("\n");
        }
    }
    if (clear_rows > 1)
    {
        printf("\033[%dA", clear_rows - 1);
    }
    printf("\033[1G");

    /* Print prompts + content. */
    int line_index = 0;
    printf("%s", linePrompt(0));
    for (size_t i = 0; i < len; i++)
    {
        if (buf[i] == '\n')
        {
            printf("\033[K\n%s", linePrompt(++line_index));
        }
        else
        {
            fputc((unsigned char)buf[i], stdout);
        }
    }
    printf("\033[K");

    /* Move from end of content to the edit cursor. */
    if (end_row > cur_row)
    {
        printf("\033[%dA", end_row - cur_row);
    }
    else if (cur_row > end_row)
    {
        printf("\033[%dB", cur_row - end_row);
    }
    printf("\033[%dG", cur_col + 1);

    *drawn_rows = total_rows;
    *drawn_cursor_row = cur_row;
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

/** True when buf has no newline — single visual line (CSI left/right is safe). */
static int isSingleLine(const char *buf, size_t len)
{
    for (size_t i = 0; i < len; i++)
    {
        if (buf[i] == '\n')
        {
            return 0;
        }
    }
    return 1;
}

/** Index at the start of the logical line containing `cursor`. */
static size_t lineStart(const char *buf, size_t cursor)
{
    while (cursor > 0 && buf[cursor - 1] != '\n')
    {
        cursor--;
    }
    return cursor;
}

/** Index just after the end of the logical line containing `cursor`. */
static size_t lineEnd(const char *buf, size_t len, size_t cursor)
{
    while (cursor < len && buf[cursor] != '\n')
    {
        cursor++;
    }
    return cursor;
}

/** Column within the logical line (0 = first char after that line's prompt). */
static size_t columnInLine(const char *buf, size_t cursor)
{
    return cursor - lineStart(buf, cursor);
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
 * Read one physical line (non-TTY / fallback). Returns malloc'd string without
 * trailing newline, or NULL on EOF.
 */
static char *readEditedLine(const char *prompt, int *interrupted)
{
    if (interrupted)
    {
        *interrupted = 0;
    }

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

/**
 * Interactive multi-line statement editor.
 * Enter inserts a newline while the statement is incomplete; Enter submits when
 * complete (semicolon / dot-command). Up/down move between logical lines in the
 * current buffer, or through history when already on the first/last line.
 */
static char *readEditedStatement(int *interrupted, int require_semicolon)
{
    if (interrupted)
    {
        *interrupted = 0;
    }

    if (enableRawMode() != 0)
    {
        return readEditedLine("> ", interrupted);
    }

    char buf[REPL_LINE_CAP];
    size_t len = 0;
    size_t cursor = 0;
    int history_index = history_count;
    char *stash = NULL;
    int drawn_rows = 1;
    int drawn_cursor_row = 0;

    printf("%s", linePrompt(0));
    fflush(stdout);

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
            printf("\n");
            return strdup(buf);
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
            /* Delete forward when buffer non-empty */
            if (cursor < len)
            {
                memmove(buf + cursor, buf + cursor + 1, len - cursor - 1);
                len--;
                redrawBuffer(buf, len, cursor, &drawn_rows, &drawn_cursor_row);
            }
            continue;
        }

        if (c == 3) /* Ctrl-C */
        {
            disableRawMode();
            free(stash);
            /* Advance past the drawn block so ^C lands on a fresh row. */
            if (drawn_rows - 1 > drawn_cursor_row)
            {
                printf("\033[%dB", drawn_rows - 1 - drawn_cursor_row);
            }
            printf("^C\n");
            if (interrupted)
            {
                *interrupted = 1;
            }
            return NULL;
        }

        if (c == '\r' || c == '\n')
        {
            buf[len] = '\0';

            /* Empty Enter cancels, matching the old single-line REPL. */
            if (len == 0)
            {
                disableRawMode();
                free(stash);
                printf("\n");
                return strdup("");
            }

            if (statementComplete(buf, require_semicolon))
            {
                disableRawMode();
                free(stash);
                if (drawn_rows - 1 > drawn_cursor_row)
                {
                    printf("\033[%dB", drawn_rows - 1 - drawn_cursor_row);
                }
                printf("\n");
                return strdup(buf);
            }

            /* Incomplete SQL: insert a newline at the cursor and keep editing. */
            if (len + 1 >= REPL_LINE_CAP)
            {
                continue;
            }
            memmove(buf + cursor + 1, buf + cursor, len - cursor);
            buf[cursor] = '\n';
            cursor++;
            len++;
            redrawBuffer(buf, len, cursor, &drawn_rows, &drawn_cursor_row);
            continue;
        }

        if (c == 127 || c == 8) /* Backspace */
        {
            if (cursor > 0)
            {
                memmove(buf + cursor - 1, buf + cursor, len - cursor);
                cursor--;
                len--;
                redrawBuffer(buf, len, cursor, &drawn_rows, &drawn_cursor_row);
            }
            continue;
        }

        if (c == 1) /* Ctrl-A: start of logical line */
        {
            size_t start = lineStart(buf, cursor);
            if (cursor != start)
            {
                cursor = start;
                redrawBuffer(buf, len, cursor, &drawn_rows, &drawn_cursor_row);
            }
            continue;
        }

        if (c == 5) /* Ctrl-E: end of logical line */
        {
            size_t end = lineEnd(buf, len, cursor);
            if (cursor != end)
            {
                cursor = end;
                redrawBuffer(buf, len, cursor, &drawn_rows, &drawn_cursor_row);
            }
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
                    if (isSingleLine(buf, len) && buf[cursor] != '\n')
                    {
                        moveCursorLeft();
                    }
                    else
                    {
                        redrawBuffer(buf, len, cursor, &drawn_rows, &drawn_cursor_row);
                    }
                }
            }
            else if (final == 'C') /* Right */
            {
                if (cursor < len)
                {
                    int crossed_nl = (buf[cursor] == '\n');
                    cursor++;
                    if (isSingleLine(buf, len) && !crossed_nl)
                    {
                        moveCursorRight();
                    }
                    else
                    {
                        redrawBuffer(buf, len, cursor, &drawn_rows, &drawn_cursor_row);
                    }
                }
            }
            else if (final == 'A') /* Up */
            {
                size_t start = lineStart(buf, cursor);
                if (start > 0)
                {
                    /* Move to the previous logical line, preserving column. */
                    size_t col = columnInLine(buf, cursor);
                    size_t prev_end = start - 1; /* points at the '\n' */
                    size_t prev_start = lineStart(buf, prev_end);
                    size_t prev_len = prev_end - prev_start;
                    cursor = prev_start + (col < prev_len ? col : prev_len);
                    redrawBuffer(buf, len, cursor, &drawn_rows, &drawn_cursor_row);
                }
                else if (history_index > 0)
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
                    redrawBuffer(buf, len, cursor, &drawn_rows, &drawn_cursor_row);
                }
            }
            else if (final == 'B') /* Down */
            {
                size_t end = lineEnd(buf, len, cursor);
                if (end < len && buf[end] == '\n')
                {
                    size_t col = columnInLine(buf, cursor);
                    size_t next_start = end + 1;
                    size_t next_end = lineEnd(buf, len, next_start);
                    size_t next_len = next_end - next_start;
                    cursor = next_start + (col < next_len ? col : next_len);
                    redrawBuffer(buf, len, cursor, &drawn_rows, &drawn_cursor_row);
                }
                else if (history_index < history_count)
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
                    redrawBuffer(buf, len, cursor, &drawn_rows, &drawn_cursor_row);
                }
            }
            else if (final == 'H') /* Home */
            {
                size_t start = lineStart(buf, cursor);
                if (cursor != start)
                {
                    cursor = start;
                    redrawBuffer(buf, len, cursor, &drawn_rows, &drawn_cursor_row);
                }
            }
            else if (final == 'F') /* End */
            {
                size_t end = lineEnd(buf, len, cursor);
                if (cursor != end)
                {
                    cursor = end;
                    redrawBuffer(buf, len, cursor, &drawn_rows, &drawn_cursor_row);
                }
            }
            else if (final == '~')
            {
                if (param == 1 || param == 7)
                {
                    size_t start = lineStart(buf, cursor);
                    if (cursor != start)
                    {
                        cursor = start;
                        redrawBuffer(buf, len, cursor, &drawn_rows, &drawn_cursor_row);
                    }
                }
                else if (param == 4 || param == 8)
                {
                    size_t end = lineEnd(buf, len, cursor);
                    if (cursor != end)
                    {
                        cursor = end;
                        redrawBuffer(buf, len, cursor, &drawn_rows, &drawn_cursor_row);
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
                        redrawBuffer(buf, len, cursor, &drawn_rows, &drawn_cursor_row);
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

        if (cursor == len && isSingleLine(buf, len))
        {
            /* Fast path: append on a single line — no full redraw. */
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
            redrawBuffer(buf, len, cursor, &drawn_rows, &drawn_cursor_row);
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

void repl(int options_in)
{
    /*
     * options / explain must be volatile: they are modified across the loop and
     * a query-time longjmp (csvdb_fatal) must not clobber them under -O3.
     */
    volatile int options = options_in;
    volatile int explain = 0;

    /* Ensure a usable interactive format if caller left format unset */
    if ((options & OUTPUT_MASK_FORMAT) == 0)
    {
        options |= OUTPUT_FORMAT_BOX;
    }

    printf("CSVDB REPL\nType \".help\" for help.\n\n");

    int require_semicolon = isatty(STDIN_FILENO);
    atexit(historyFree);
    atexit(disableRawMode);

    while (1)
    {
        char *statement = NULL;
        int cancelled = 0;

        if (require_semicolon)
        {
            /* TTY: one multi-line editable buffer for the whole statement. */
            int interrupted = 0;
            statement = readEditedStatement(&interrupted, require_semicolon);
            if (interrupted)
            {
                free(statement);
                continue;
            }
            if (statement == NULL)
            {
                return;
            }
            if (statement[0] == '\0')
            {
                free(statement);
                continue;
            }
        }
        else
        {
            /* Non-TTY: assemble physical lines until the statement is complete. */
            size_t statement_len = 0;
            int first_line = 1;

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

        int run_flags = options;
        run_flags &= ~FLAG_EXPLAIN;
        if (explain)
        {
            run_flags |= FLAG_EXPLAIN;
        }

        /*
         * Recover from query-time csvdb_fatal() so the session continues.
         * setjmp returns non-zero after a fatal; both paths share cleanup.
         */
        jmp_buf fatal_jmp;
        csvdb_fatal_catch(&fatal_jmp);
        if (setjmp(fatal_jmp) == 0)
        {
            const char *end_ptr;
            query(statement, run_flags, stdout, &end_ptr);
        }
        csvdb_fatal_catch(NULL);

        free(statement);
        printf("\n");
    }
}

static void printHelp(void)
{
    printf(
        "Enter SQL statements ending with ';'. Dot-commands need no semicolon.\n"
        "Enter adds a continuation line while a statement is incomplete.\n"
        "Up/down move between those lines (or history on the first line);\n"
        "left/right move the cursor. Home/End are per-line.\n\n"
        "Commands in REPL:\n"
        "\t.help\n"
        "\t.version\n"
        "\t.exit\n"
        "\t.format tsv|csv|html|table|box|json|json:array|sql|sql:values|xml|record\n"
        "\t.headers [on|off]\n"
        "\t.explain\n");
}
