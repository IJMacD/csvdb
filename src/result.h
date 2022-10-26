#pragma once

#include "limits.h"

struct Field {
    char text[MAX_FIELD_LENGTH];
    int table_id;
    int index;
};

struct ColumnNode {
    char alias[MAX_FIELD_LENGTH];
    int function;
    int concat;
    struct Field fields[2];
};

struct RowList {
    int row_count;
    int join_count;
    int * row_ids;
};

struct ResultSet {
    int list_count;
    struct RowList * row_lists;
    char * list_values;
};

int getRowID (struct RowList * row_list, int join_id, int index);

void writeRowID (struct RowList * row_list, int join_id, int index, int value);

void appendRowID (struct RowList * row_list, int value);

void appendRowID2 (struct RowList * row_list, int value1, int value2);

void appendRowID3 (struct RowList * row_list, int value1, int value2, int value3);

void appendJoinedRowID (struct RowList * dest_list, struct RowList * src_list, int src_index, int value);

void copyResultRow (struct RowList * dest_list, struct RowList * src_list, int src_index);

void makeRowList (struct RowList * list, int join_count, int max_rows);

void destroyRowList (struct RowList * list);

void overwriteRowList (struct RowList * dest, struct RowList * src);

void reverseRowList (struct RowList * row_list);
