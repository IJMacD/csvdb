#include <stdio.h>
#include "db.h"

#define OUTPUT_OPTION_TAB   1
#define OUTPUT_OPTION_COMMA 2

void printHeaderLine (FILE *f, struct DB *db, int *field_indices, int field_count, int flags);

void printResultLine (FILE *f, struct DB *db, int *field_indices, int field_count, int record_index, int result_count, int flags);
