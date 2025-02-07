#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <ctype.h>

int is_numeric(const char *string)
{
    if (*string == '\0')
        return 0;

    const char *ptr = string;

    // Allow spaces at start
    while (isspace(*ptr))
        ptr++;

    // First character can be a negative sign
    if (*ptr == '-')
        ptr++;

    int decimal = 0;
    while (*ptr != '\0')
    {
        if (ptr > string && *ptr == '.')
        {
            if (decimal)
                return 0;
            decimal = 1;
        }
        else if (!isdigit(*ptr))
            return 0;
        ptr++;
    }
    return 1;
}

void reverse_array(int *array, int size)
{
    for (int i = 0; i < size / 2; i++)
    {
        int i1 = size - i - 1;
        int temp = array[i1];
        array[i1] = array[i];
        array[i] = temp;
    }
}

int str_find_index(const char *string, char chr)
{
    int i = 0;
    while (string[i] != '\0')
    {
        if (string[i] == chr)
            return i;
        i++;
    }
    return -1;
}

int writeUTF8(char *output, int codepoint)
{
    if (codepoint < 0x80)
    {
        output[0] = (codepoint >> 0 & 0x7F) | 0x00;
        output[1] = '\0';
        return 1;
    }

    if (codepoint < 0x0800)
    {
        output[0] = (codepoint >> 6 & 0x1F) | 0xC0;
        output[1] = (codepoint >> 0 & 0x3F) | 0x80;
        output[2] = '\0';

        return 2;
    }

    if (codepoint < 0x010000)
    {
        output[0] = (codepoint >> 12 & 0x0F) | 0xE0;
        output[1] = (codepoint >> 6 & 0x3F) | 0x80;
        output[2] = (codepoint >> 0 & 0x3F) | 0x80;
        output[3] = '\0';

        return 3;
    }

    if (codepoint < 0x110000)
    {
        output[0] = (codepoint >> 18 & 0x07) | 0xF0;
        output[1] = (codepoint >> 12 & 0x3F) | 0x80;
        output[2] = (codepoint >> 6 & 0x3F) | 0x80;
        output[3] = (codepoint >> 0 & 0x3F) | 0x80;
        output[4] = '\0';

        return 4;
    }

    return 0;
}

int readUTF8(uint8_t *input, uint8_t **end_ptr)
{
    if (input[0] < 0x80)
    {
        if (end_ptr != NULL)
        {
            *end_ptr = input + 1;
        }
        return input[0];
    }

    if ((input[0] & 0xE0) == 0xC0)
    {
        if (end_ptr != NULL)
        {
            *end_ptr = input + 2;
        }

        // Catch (some) malformed UTF-8
        // Note: end_ptr has already moved on expected number of bytes
        if ((input[1] & 0xC0) != 0x80)
            return 0;

        return ((input[0] & 0x1F) << 6) | (input[1] & 0x3F);
    }

    if ((input[0] & 0xF0) == 0xE0)
    {
        if (end_ptr != NULL)
        {
            *end_ptr = input + 3;
        }

        // Catch (some) malformed UTF-8
        // Note: end_ptr has already moved on expected number of bytes
        if ((input[1] & 0xC0) != 0x80 || (input[2] & 0xC0) != 0x80)
            return 0;

        return ((input[0] & 0x0F) << 12) | ((input[1] & 0x3F) << 6) | (input[2] & 0x3F);
    }

    if ((input[0] & 0xF8) == 0xF0)
    {
        if (end_ptr != NULL)
        {
            *end_ptr = input + 4;
        }

        // Catch (some) malformed UTF-8
        // Note: end_ptr has already moved on expected number of bytes
        if ((input[1] & 0xC0) != 0x80 || (input[2] & 0xC0) != 0x80 || (input[3] & 0xC0) != 0x80)
            return 0;

        return ((input[0] & 0x0F) << 18) | ((input[1] & 0x3F) << 12) | ((input[2] & 0x3F) << 6) | (input[3] & 0x3F);
    }

    if (end_ptr != NULL)
    {
        *end_ptr = input;
    }

    return -1;
}

/**
 * @brief Given a string *starting with an open parenthesis*, search for a
 * matching closing parenthesis and return the total length of string
 * (including the open and closing parentheses). This function allows nested
 * parentheses.
 * First character must be '('.
 * Ignores anything in single quotes.
 *
 * @param string
 * @return int Length including opening and closing parentheses
 */
int find_matching_parenthesis(const char *string)
{
    int offset = 0;
    int depth = 1;
    int quoted = 0;

    if (string[offset] != '(')
    {
        fprintf(
            stderr,
            "find_matching_parenthesis was expecting '(' but actually got '%c'"
            "\n",
            string[offset]);
        exit(-1);
    }

    offset++;

    while (string[offset] != '\0')
    {
        char c = string[offset++];

        if (c == '\'')
        {
            quoted = ~quoted;
        }
        else if (!quoted)
        {
            if (c == '(')
            {
                depth++;
            }
            else if (c == ')')
            {
                depth--;
            }
        }

        if (depth == 0)
        {
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
int whitespaceCollapse(char *output, const char *source, int length)
{
    int whitespace_flag = 0;
    int written = 0;
    for (int i = 0; i < length; i++)
    {
        if (source[i] == '\0')
            break;
        if (source[i] == ' ' || source[i] == '\n' || source[i] == '\t')
        {
            if (!whitespace_flag)
            {
                output[written++] = ' ';
            }
            whitespace_flag = 1;
        }
        else
        {
            output[written++] = source[i];
            whitespace_flag = 0;
        }
    }

    // Trim trailing whitespace
    if (output[written - 1] == ' ')
    {
        written--;
    }

    output[written] = '\0';
    return written;
}

void trimTrailingWhitespace(char *string)
{
    size_t length = strlen(string);
    char *ptr = string + length;
    while (ptr > string && isspace(*ptr))
    {
        *(ptr--) = '\0';
    }
}

/**
 * @brief strcpy that can cope with overlapping strings as long as the dest is
 * after the src.
 *
 * @param dest
 * @param src
 */
void strcpy_overlap(char *dest, const char *src)
{
    if (src == dest)
        return;

    char c;
    while ((c = *(src++)) != '\0')
    {
        *(dest++) = c;
    }

    *dest = '\0';
}

/**
 * returns which *single* bit is 1
 * returns -1 if no *single* bit is 1
 */
int whichBit(int bit_map)
{
    int count = 0;

    while (bit_map)
    {
        int bit = bit_map & 1;

        bit_map >>= 1;

        if (bit)
        {
            return bit_map ? -1 : count;
        }

        count++;
    }

    return -1;
}

int w1252Map(int w1252Char)
{
    if (w1252Char < 0x80)
    {
        return w1252Char;
    }

    int codepoints[] = {
        8364, 129, 8218, 402, 8222, 8230, 8224, 8225, 710, 8240, 352, 8249, 338, 141, 381, 143,
        144, 8216, 8217, 8220, 8221, 8226, 8211, 8212, 732, 8482, 353, 8250, 339, 157, 382,
        376, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176,
        177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194,
        195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212,
        213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230,
        231, 232, 233, 234, 235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247, 248,
        249, 250, 251, 252, 253, 254, 255};

    for (int i = 0; i < 128; i++)
    {
        if (codepoints[i] == w1252Char)
        {
            return 0x80 + i;
        }
    }

    return -1;
}

void replace(char *output, const char *input, char search, char *replace)
{
    size_t r_len = strlen(replace);
    while (*input)
    {
        if (*input == search)
        {
            for (size_t i = 0; i < r_len; i++)
            {
                *(output++) = replace[i];
            }
        }
        else
        {
            *(output++) = *input;
        }
        input++;
    }
    *output = '\0';
}

int countCodePoints(char *string)
{
    int count = 0;
    while (*string++)
    {
        char c = *string;
        if ((c & 0b11000000) != 0b10000000)
        {
            count++;
        }
    }
    return count;
}