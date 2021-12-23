#pragma once

#include "limits.h"

struct ResultColumn {
    int field;
    int function;
    char text[FIELD_MAX_LENGTH];
    char alias[FIELD_MAX_LENGTH];
    int table_id;
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