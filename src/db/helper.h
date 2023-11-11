#include <stdio.h>

#include "../structs.h"

void consumeStream (struct DB *db, FILE *stream);

int indexLines (struct DB *db, int max_lines, char quote_char);
