#include <stdlib.h>

void skipWhitespace (const char *string, size_t *index);

void skipToken (const char *string, size_t *index);

void skipLine (const char * string, size_t *index);

int getToken (const char *string, size_t *index, char *token, int token_max_length);

int getQuotedToken (const char *string, size_t *index, char *token, int token_max_length);

int getNumericToken (const char *string, size_t *index);
