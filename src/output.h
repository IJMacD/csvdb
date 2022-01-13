#include <stdio.h>
#include "query.h"
#include "result.h"

void printHeaderLine (FILE *f, struct Table *tables, int table_count, struct ColumnNode columns[], int column_count, int flags);

void printResultLine (FILE *f, struct Table *tables, int table_count, struct ColumnNode columns[], int column_count, int result_index, struct RowList * row_list, int flags);

void printPreamble (FILE *f, struct Table *table, struct ColumnNode columns[], int column_count, int flags);

void printPostamble (FILE *f, struct Table *table, struct ColumnNode columns[], int column_count, int result_count, int flags);
