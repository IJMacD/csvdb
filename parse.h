#include <stdlib.h>

#include "query.h"

int parseQuery (struct Query *q, const char *query);

int destroyQuery (struct Query *q);

void skipWhitespace (const char *string, size_t *index);

void skipToken (const char *string, size_t *index);

void skipLine (const char * string, size_t *index);

int getToken (const char *string, size_t *index, char *token, int token_max_length);

int getNumericToken (const char *string, size_t *index);
