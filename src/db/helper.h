#include <stdio.h>

#include "../structs.h"

void consumeStream (struct DB *db, FILE *stream);

int indexLines (struct DB *db, int max_lines, char quote_char);

int ends_with (const char *string, const char *search);

/**
 * Open path only if it is a regular file.
 * On some systems fopen(path, "r") succeeds for directories; those must not be
 * treated as empty CSV tables.
 */
FILE *fopen_regular (const char *path, const char *mode);
