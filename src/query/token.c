#include <string.h>
#include <ctype.h>
#include <stdlib.h>

#include "../structs.h"

static void skipToken (const char *string, size_t *index);

static void skipLine (const char * string, size_t *index);

static void skipLinePtr (const char **string);

static int isTokenChar (char c);

static int isOperatorChar (char c);

/**
 * @brief Skips spaces, newlines, tabs and comment lines
 *
 * @param string
 * @param index
 */
void skipWhitespace (const char *string, size_t *index) {
    while (string[*index] != '\0') {
        while(isspace(string[*index])) { (*index)++; }

        if (strncmp(string + *index, "--", 2) == 0) {
            skipLine(string, index);
        } else {
            break;
        }
    }
}

/**
 * @brief Skips spaces, newlines, tabs and comment lines
 *
 * @param string
 * @param index
 */
void skipWhitespacePtr (const char **string) {
    while (**string != '\0') {
        while(isspace(**string)) { (*string)++; }

        if (strncmp(*string, "--", 2) == 0) {
            skipLinePtr(string);
        } else {
            break;
        }
    }
}

static void skipToken (const char *string, size_t *index) {
    if (string[*index] == '\'') {
        // Skip open quote
        (*index)++;

        while (string[*index] != '\0') {
            if (string[*index] == '\\') {
                (*index) += 2;
            }
            if (string[*index] == '\'') {
                // Allow double single quote escaping
                if (string[*index + 1] != '\'') {
                    break;
                }
                (*index)++;
            }
            (*index)++;
        }

        // Skip close quote
        if (string[*index] != '\0') {
            (*index)++;
        }
    }
    else if (string[*index] == '"') {
        // Skip open quote
        (*index)++;

        while (string[*index] != '\0' && string[*index] != '"') {
            (*index)++;
        }

        // Skip close quote
        if (string[*index] != '\0') {
            (*index)++;
        }
    }
    else {
        while (isTokenChar(string[*index])) {
            (*index)++;
        }
    }
}

static void skipLine (const char *string, size_t *index) {
    while (string[*index] != '\n' && string[*index] != '\0') {
        (*index)++;
    }
    if (string[*index] == '\n') (*index)++;
}


static void skipLinePtr (const char **string) {
    while (**string != '\n' && **string != '\0') {
        (*string)++;
    }
    if (**string == '\n') (*string)++;
}

/**
 * @brief Get the next token in the stream
 *
 * @param string
 * @param index
 * @param token
 * @param token_max_length
 * @return int Length of token
 */
int getToken (
    const char *string,
    size_t *index,
    char *token,
    int token_max_length
) {
    skipWhitespace(string, index);

    // Check for end of string
    if (string[*index] == '\0') {
        // clear output value
        token[0] = '\0';
        return 0;
    }

    // Check for simple operators
    if (!isTokenChar(string[*index])) {
        // operators are not allowed as tokens
        token[0] = '\0';
        return -1;
    }

    int start_index = *index;

    skipToken(string, index);

    int token_length = *index - start_index;

    if (token_length > token_max_length) {
        return -1;
    }

    memcpy(token, string + start_index, token_length);

    token[token_length] = '\0';

    return token_length;
}

/**
 * @brief Get the next token in the stream, optionally quoted
 *
 * @param string
 * @param index
 * @param token
 * @param token_max_length
 * @return int 0 means no quotes, 2 means double quotes
 */
int getQuotedToken (
    const char *string,
    size_t *index,
    char *token,
    int token_max_length
) {
    skipWhitespace(string, index);

    if (string[*index] == '\0') {
        return -1;
    }

    int quoted_flag = (string[*index] == '"' ? 2 : 0);

    int start_index = *index;

    if (quoted_flag) {
        start_index++;
    }

    skipToken(string, index);

    int token_length = *index - start_index;

    if (quoted_flag) {
        token_length--;
    }

    if (token_length > token_max_length) {
        return -1;
    }

    memcpy(token, string + start_index, token_length);

    token[token_length] = '\0';

    return quoted_flag;
}

int getNumericToken (const char *string, size_t *index) {
    char val[10];
    getToken(string, index, val, 10);
    return atol(val);
}

/**
 * @brief Get an operator token from the stream
 *
 * @param string
 * @param index
 * @param token
 * @param token_max_length
 * @return int Length of token
 */
int getOperatorToken (
    const char *string,
    size_t *index,
    char *token,
    int token_max_length
) {
    skipWhitespace(string, index);

    // End of string
    if (string[*index] == '\0') {
        // clear output value
        token[0] = '\0';
        return -1;
    }
    int start_index = *index;

    while (isOperatorChar(string[*index])) {
        (*index)++;
    }

    int token_length = *index - start_index;

    if (token_length == 0) {
        // We didn't get a pure operator but perhaps we'll have a wordy one
        // e.g. BETWEEN, AND, OR, IN

        skipToken(string, index);

        token_length = *index - start_index;
    }

    if (token_length > token_max_length) {
        return -1;
    }

    memcpy(token, string + start_index, token_length);

    token[token_length] = '\0';

    return token_length;
}

static int isTokenChar (char c) {
    return !iscntrl(c)
        && c != ' '
        && c != ','
        && c != '('
        && c != ')'
        && c != '|'
        && c != '='
        && c != '!'
        && c != '>'
        && c != '<'
        && c != '+'
        && c != '-'
        && c != '*'
        && c != '/'
        && c != '%'
        && c != ';';
}

/**
 * @brief Operator could be =, !=, >, <, >=, IS, LIKE etc.
 *
 * @param c
 * @return int
 */
static int isOperatorChar (char c) {
    return c == '|'
        || c == '='
        || c == '!'
        || c == '>'
        || c == '<'
        || c == '+'
        || c == '-'
        || c == '*'
        || c == '/'
        || c == '%';
}