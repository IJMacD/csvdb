#include <stdio.h>
#include "db.h"
#include "query.h"

// xxxx aaa h
//          h - headers
//      aaa   - format

#define OUTPUT_OPTION_HEADERS   1

#define OUTPUT_FORMAT_TAB       (1 << 1)
#define OUTPUT_FORMAT_COMMA     (2 << 1)
#define OUTPUT_FORMAT_JSON      (3 << 1)
#define OUTPUT_FORMAT_HTML      (4 << 1)

void printHeaderLine (FILE *f, struct DB *db, struct ResultColumn columns[], int column_count, int flags);

void printResultLine (FILE *f, struct DB *db, struct ResultColumn columns[], int column_count, int record_index, int * result_ids, int result_count, int flags);

void printPreamble (FILE *f, struct DB *db, struct ResultColumn columns[], int column_count, int flags);

void printPostamble (FILE *f, struct DB *db, struct ResultColumn columns[], int column_count, int result_count, int flags);
