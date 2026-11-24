#include <stdint.h>

int is_numeric(const char *string);

/**
 * @brief True if string is a complex form ending in i/I (not a plain real).
 */
int is_complex(const char *string);

/**
 * @brief Parse a real or complex string into re/im. Accepts plain numerics
 *        (im=0) and forms like i, -i, 4i, 3+4i, 3-i.
 * @return 1 on success, 0 on failure
 */
int parse_complex(const char *string, double *re, double *im);

/**
 * @brief Compact formatting for a double (integers stay without a fraction).
 * @return Number of bytes written
 */
int sprint_number(char *output, double value);

/**
 * @brief Canonical complex formatting (a+bi). Pure reals use sprint_number.
 * @return Number of bytes written
 */
int sprint_complex(char *output, double re, double im);

/**
 * @brief Compare two numeric strings as doubles.
 * @return <0 if a < b, 0 if equal, >0 if a > b
 */
int compare_numeric(const char *a, const char *b);

/**
 * @brief Compare complex (or real) strings by real, then imag.
 * @return <0 if a < b, 0 if equal, >0 if a > b; 0 if either fails to parse
 */
int compare_complex(const char *a, const char *b);

/**
 * @brief Write an order-preserving fixed-width key for a numeric string.
 * @return Number of bytes written
 */
int format_sortable_number(char *output, const char *value);

/**
 * @brief Order-preserving key for a complex (or real) string: re then im.
 * @return Number of bytes written
 */
int format_sortable_complex(char *output, const char *value);

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

int countCodePoints(char *string);