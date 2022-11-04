#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>

int is_numeric (const char *string) {
    if (*string == '\0') return 0;

    const char *ptr = string;

    // Allow spaces at start
    while (isspace(*ptr)) ptr++;

    // First character can be a negative sign
    if (*ptr == '-') ptr++;

    int decimal = 0;
    while (*ptr != '\0') {
        if (ptr > string && *ptr == '.') {
            if (decimal) return 0;
            decimal = 1;
        }
        else if (!isdigit(*ptr)) return 0;
        ptr++;
    }
    return 1;
}

void reverse_array (int * array, int size) {
    for (int i = 0; i < size / 2; i++) {
        int i1 = size - i - 1;
        int temp = array[i1];
        array[i1] = array[i];
        array[i] = temp;
    }
}

int str_find_index (const char * string, char chr) {
    int i = 0;
    while (string[i] != '\0') {
        if (string[i] == chr) return i;
        i++;
    }
    return -1;
}

int writeUTF8(char * output, int codepoint) {
    if (codepoint < 0x80) {
      output[0] = (codepoint >>0  & 0x7F) | 0x00;
      output[1] = '\0';
      return 1;
    }

    if (codepoint < 0x0800) {
      output[0] = (codepoint >>6  & 0x1F) | 0xC0;
      output[1] = (codepoint >>0  & 0x3F) | 0x80;
      output[2] = '\0';

      return 2;
    }

    if (codepoint < 0x010000) {
      output[0] = (codepoint >>12 & 0x0F) | 0xE0;
      output[1] = (codepoint >>6  & 0x3F) | 0x80;
      output[2] = (codepoint >>0  & 0x3F) | 0x80;
      output[3] = '\0';

      return 3;
    }

    if (codepoint < 0x110000) {
      output[0] = (codepoint >>18 & 0x07) | 0xF0;
      output[1] = (codepoint >>12 & 0x3F) | 0x80;
      output[2] = (codepoint >>6  & 0x3F) | 0x80;
      output[3] = (codepoint >>0  & 0x3F) | 0x80;
      output[4] = '\0';

      return 4;
    }

    return 0;
}

/**
 * @brief Given a string *starting with an open parenthesis*, search for a
 * matching closing parenthesis and return the total length of string
 * (including the open and closing parentheses). This function allows nested
 * parentheses.
 * First character must be '('.
 *
 * @param string
 * @return int Length including opening and closing parentheses
 */
int find_matching_parenthesis (const char *string) {
    int offset = 0;
    int depth = 1;

    if (string[offset] != '(') {
        fprintf(stderr, "find_matching_parenthesis was expecting '(' but actually got '%c'\n", string[offset]);
        exit(-1);
    }

    offset++;

    while(string[offset] != '\0') {
        char c = string[offset++];
        if (c == '(') {
            depth++;
        }
        else if(c == ')') {
            depth--;
        }

        if (depth == 0) {
            return offset;
        }
    }

    return -1;
}

/**
 * @brief Copy from source string to output, collapsing consecutive whitespace
 * characters in to a single space. Newlines and tabs are converted to spaces.
 * Will add null terminator.
 *
 * @param output Result will be written here
 * @param source Copied from here
 * @param length length of *input* string to copy
 * @return int length written to output
 */
int whitespaceCollapse (char *output, const char *source, int length) {
    int whitespace_flag = 0;
    int written = 0;
    for (int i = 0; i < length; i++) {
        if (source[i] == '\0') break;
        if (source[i] == ' ' || source[i] == '\n' || source[i] == '\t') {
            if (!whitespace_flag) {
                output[written++] = ' ';
            }
            whitespace_flag = 1;
        }
        else {
            output[written++] = source[i];
            whitespace_flag = 0;
        }
    }
    output[written] = '\0';
    return written;
}

/**
 * @brief strcpy that can cope with overlapping strings as long as the dest is
 * after the src.
 *
 * @param dest
 * @param src
 */
void strcpy_overlap (char *dest, const char *src) {
    if (src == dest) return;

    char c;
    while((c = *(src++)) != '\0') {
        *(dest++) = c;
    }

    *dest = '\0';
}