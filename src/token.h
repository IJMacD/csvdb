void skipWhitespace (const char *string, size_t *index);

void skipWhitespacePtr (const char **string);

int getToken (
    const char *string,
    size_t *index,
    char *token,
    int token_max_length
);

int getQuotedToken (
    const char *string,
    size_t *index,
    char *token,
    int token_max_length
);

int getNumericToken (const char *string, size_t *index);

int getOperatorToken (
    const char *string,
    size_t *index,
    char *token,
    int token_max_length
);
