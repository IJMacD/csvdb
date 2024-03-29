#include "../structs.h"

void printHeaderLine (
    FILE *f,
    struct Table *tables,
    int table_count,
    struct Node columns[],
    int column_count,
    enum OutputOption flags
);

void printResultLine (
    FILE *f,
    struct Table *tables,
    int table_count,
    struct Node columns[],
    int column_count,
    int result_index,
    RowListIndex row_list,
    enum OutputOption flags
);

void printPreamble (
    FILE *f,
    struct Table *tables,
    int table_count,
    struct Node columns[],
    int column_count,
    enum OutputOption flags
);

void printPostamble (
    FILE *f,
    struct Table *tables,
    int table_count,
    struct Node columns[],
    int column_count,
    int result_count,
    enum OutputOption flags
);
