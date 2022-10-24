#include <string.h>
#include <ctype.h>

#include "token.h"

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

void skipToken (const char *string, size_t *index) {
    if (string[*index] == '\'') {
        // Skip open quote
        (*index)++;

        while (string[*index] != '\0' && string[*index] != '\'') {
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
        while (!iscntrl(string[*index]) && string[*index] != ' ' && string[*index] != ',' && string[*index] != '|') {
            (*index)++;
        }
    }
}

void skipLine (const char *string, size_t *index) {
    while (string[*index] != '\n' && string[*index] != '\0') {
        (*index)++;
    }
    if (string[*index] == '\n') (*index)++;
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
int getToken (const char *string, size_t *index, char *token, int token_max_length) {
    skipWhitespace(string, index);

    // End of string
    if (string[*index] == '\0') {
        // clear output value
        token[0] = '\0';
        return -1;
    }
    int start_index = *index;

    // printf("Token starts at %d\n", start_index);

    skipToken(string, index);

    int token_length = *index - start_index;

    if (token_length > token_max_length) {
        return -1;
    }

    // printf("Token is %d characters\n", token_length);

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
 * @return int 0 means no quotes, ~~1 means single quotes~~, 2 means double quotes
 */
int getQuotedToken (const char *string, size_t *index, char *token, int token_max_length) {
    skipWhitespace(string, index);

    if (string[*index] == '\0') {
        return -1;
    }

    int quoted_flag = (string[*index] == '"' ? 2 : 0);

    int start_index = *index;

    if (quoted_flag) {
        start_index++;
    }

    // printf("Token starts at %d\n", start_index);

    skipToken(string, index);

    int token_length = *index - start_index;

    if (quoted_flag) {
        token_length--;
    }

    if (token_length > token_max_length) {
        return -1;
    }

    // printf("Token is %d characters\n", token_length);

    memcpy(token, string + start_index, token_length);

    token[token_length] = '\0';

    return quoted_flag;
}

int getNumericToken (const char *string, size_t *index) {
    char val[10];
    getToken(string, index, val, 10);
    return atol(val);
}
