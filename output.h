#include <stdio.h>
#include "db.h"
#include "query.h"
#include "result.h"

// xxxx aaa h
//          h - headers
//      aaa   - format

#define OUTPUT_OPTION_HEADERS   1

#define OUTPUT_MASK_FORMAT      0x0E

#define OUTPUT_FORMAT_TAB       (1 << 1)
#define OUTPUT_FORMAT_COMMA     (2 << 1)
#define OUTPUT_FORMAT_JSON      (3 << 1)
#define OUTPUT_FORMAT_HTML      (4 << 1)
#define OUTPUT_FORMAT_JSON_ARRAY      (5 << 1)

void printHeaderLine (FILE *f, struct DB *tables, int table_count, struct ResultColumn columns[], int column_count, int flags);

void printResultLine (FILE *f, struct DB *tables, int table_count, struct ResultColumn columns[], int column_count, int result_index, struct RowList * row_list, int flags);

void printPreamble (FILE *f, struct DB *db, struct ResultColumn columns[], int column_count, int flags);

void printPostamble (FILE *f, struct DB *db, struct ResultColumn columns[], int column_count, int result_count, int flags);
