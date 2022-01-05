#include <string.h>
#include <ctype.h>

#include "token.h"

void skipWhitespace2 (const char **string) {
    while (**string != '\0') {
        while(isspace(**string)) { (*string)++; }

        if (strncmp(*string, "--", 2) == 0) {
            skipLine2(string);
        } else {
            break;
        }
    }
}
void skipWhitespace (const char *string, size_t *index) {
    const char *ptr = string + *index;
    const char *start = ptr;
    skipWhitespace2(&ptr);

    *index += ptr - start;
}

void skipToken2 (const char **string) {
    if (**string == '\'') {
        // Skip open quote
        (*string)++;

        while (**string != '\0' && **string != '\'') {
            (*string)++;
        }

        // Skip close quote
        if (**string != '\0') {
            (*string)++;
        }
    }
    else if (**string == '"') {
        // Skip open quote
        (*string)++;

        while (**string != '\0' && **string != '"') {
            (*string)++;
        }

        // Skip close quote
        if (**string != '\0') {
            (*string)++;
        }
    }
    else {
        while (!iscntrl(**string) && **string != ' ' && **string != ',') {
            (*string)++;
        }
    }
}

void skipToken (const char *string, size_t *index) {
    const char *ptr = string + *index;
    const char *start = ptr;
    skipToken2(&ptr);

    *index += ptr - start;
}

void skipLine2 (const char **string) {
    while (**string != '\n' && **string != '\0') {
        (*string)++;
    }
    if (**string == '\n') (*string)++;
}

void skipLine (const char *string, size_t *index) {
    const char *ptr = string + *index;
    const char *start = ptr;
    skipLine2(&ptr);

    *index += ptr - start;
}

/**
 * @brief Get the next token in the stream
 *
 * @param string Pointer to input string
 * @param token Output string
 * @param token_max_length Storage size of output string
 * @return int Length of token
 */
int getToken2 (const char **string, char *token, int token_max_length) {

    skipWhitespace2(string);

    if (**string == '\0') {
        return -1;
    }
    const char *start = *string;

    // printf("Token starts at %d\n", start_index);

    skipToken2(string);

    int token_length = *string - start;

    if (token_length > token_max_length) {
        return -1;
    }

    // printf("Token is %d characters\n", token_length);

    memcpy(token, *string, token_length);

    token[token_length] = '\0';

    return token_length;
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
    const char *ptr = string + *index;
    int result = getToken2(&ptr, token, token_max_length);
    if (result >= 0) {
        *index += result;
    }
    return result;
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
int getQuotedToken2 (const char **string, char *token, int token_max_length) {

    skipWhitespace2(string);

    if (**string == '\0') {
        return -1;
    }

    int quoted_flag = (**string == '"' ? 2 : 0);

    const char * start = *string;

    if (quoted_flag) {
        start++;
    }

    // printf("Token starts at %d\n", start_index);

    skipToken2(string);

    int token_length = *string - start;

    if (quoted_flag) {
        token_length--;
    }

    if (token_length > token_max_length) {
        return -1;
    }

    // printf("Token is %d characters\n", token_length);

    memcpy(token, start, token_length);

    token[token_length] = '\0';

    return quoted_flag;
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
    const char *ptr = string + *index;
    int result = getQuotedToken2(&ptr, token, token_max_length);
    if (result >= 0) {
        *index += result;
    }
    return result;
}

int getNumericToken (const char *string, size_t *index) {
    char val[10];
    getToken(string, index, val, 10);
    return atol(val);
}