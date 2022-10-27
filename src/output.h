#include <stdio.h>
#include "query.h"
#include "result.h"

// hxxx aaaa
// h         - headers
//      aaaa - format

#define OUTPUT_OPTION_HEADERS       1 << 7

#define OUTPUT_MASK_FORMAT          0x0F

#define OUTPUT_FORMAT_TAB           1
#define OUTPUT_FORMAT_COMMA         2
#define OUTPUT_FORMAT_JSON          3
#define OUTPUT_FORMAT_HTML          4
#define OUTPUT_FORMAT_JSON_ARRAY    5
#define OUTPUT_FORMAT_SQL_INSERT    6
#define OUTPUT_FORMAT_TABLE         7
#define OUTPUT_FORMAT_INFO_SEP      8
#define OUTPUT_FORMAT_XML           9
#define OUTPUT_FORMAT_SQL_VALUES   10

void printHeaderLine (FILE *f, struct Table *tables, int table_count, struct ColumnNode columns[], int column_count, int flags);

void printResultLine (FILE *f, struct Table *tables, int table_count, struct ColumnNode columns[], int column_count, int result_index, struct RowList * row_list, int flags);

void printPreamble (FILE *f, struct Table *table, struct ColumnNode columns[], int column_count, int flags);

void printPostamble (FILE *f, struct Table *table, struct ColumnNode columns[], int column_count, int result_count, int flags);
