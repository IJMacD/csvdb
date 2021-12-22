#include <stdio.h>
#include "result.h"

int getRowID (struct RowList * row_list, int join_id, int index) {
    return row_list->row_ids[index * row_list->join_count + join_id];
}

void writeRowID (struct RowList * row_list, int join_id, int index, int value) {
    row_list->row_ids[index * row_list->join_count + join_id] = value;
}

void appendRowID (struct RowList * row_list, int value) {
    if (row_list->join_count != 1) {
        fprintf(stderr, "Cannot append 1 row ID to list with %d join\n", row_list->join_count);
    }
    row_list->row_ids[row_list->row_count * row_list->join_count] = value;
    row_list->row_count++;
}

void appendRowID2 (struct RowList * row_list, int value1, int value2) {
    if (row_list->join_count != 1) {
        fprintf(stderr, "Cannot append 2 row IDs to list with %d join(s)\n", row_list->join_count);
    }
    row_list->row_ids[row_list->row_count * row_list->join_count + 0] = value1;
    row_list->row_ids[row_list->row_count * row_list->join_count + 1] = value2;
    row_list->row_count++;
}

void appendRowID3 (struct RowList * row_list, int value1, int value2, int value3) {
    if (row_list->join_count != 1) {
        fprintf(stderr, "Cannot append 3 row IDs to list with %d join(s)\n", row_list->join_count);
    }
    row_list->row_ids[row_list->row_count * row_list->join_count + 0] = value1;
    row_list->row_ids[row_list->row_count * row_list->join_count + 1] = value2;
    row_list->row_ids[row_list->row_count * row_list->join_count + 2] = value3;
    row_list->row_count++;
}