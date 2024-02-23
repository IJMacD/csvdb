#include <stdint.h>

int is_numeric(const char *string);

void reverse_array(int *array, int size);

int str_find_index(const char *string, char chr);

int writeUTF8(char *output, int codepoint);

int readUTF8(uint8_t *input, uint8_t **end_ptr);

int find_matching_parenthesis(const char *string);

int whitespaceCollapse(char *output, const char *source, int length);

void strcpy_overlap(char *dest, const char *src);

void trimTrailingWhitespace(char *string);

int whichBit(int bit_map);

int w1252Map(int w1252Char);

void replace(char *out, const char *input, char search, char *replace);
