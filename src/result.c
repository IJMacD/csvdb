#include <stdio.h>
#include <stdlib.h>
#include "result.h"

int getRowID (struct RowList * row_list, int join_id, int index) {
    return row_list->row_ids[index * row_list->join_count + join_id];
}

void writeRowID (struct RowList * row_list, int join_id, int index, int value) {
    row_list->row_ids[index * row_list->join_count + join_id] = value;
}

void appendRowID (struct RowList * row_list, int value) {
    if (row_list->join_count != 1) {
        fprintf(stderr, "Cannot append 1 row ID to list with %d joins\n", row_list->join_count);
        exit(-1);
    }
    row_list->row_ids[row_list->row_count * row_list->join_count] = value;
    row_list->row_count++;
}

void appendRowID2 (struct RowList * row_list, int value1, int value2) {
    if (row_list->join_count != 2) {
        fprintf(stderr, "Cannot append 2 row IDs to list with %d join(s)\n", row_list->join_count);
        exit(-1);
    }
    row_list->row_ids[row_list->row_count * row_list->join_count + 0] = value1;
    row_list->row_ids[row_list->row_count * row_list->join_count + 1] = value2;
    row_list->row_count++;
}

void appendRowID3 (struct RowList * row_list, int value1, int value2, int value3) {
    if (row_list->join_count != 3) {
        fprintf(stderr, "Cannot append 3 row IDs to list with %d join(s)\n", row_list->join_count);
        exit(-1);
    }
    row_list->row_ids[row_list->row_count * row_list->join_count + 0] = value1;
    row_list->row_ids[row_list->row_count * row_list->join_count + 1] = value2;
    row_list->row_ids[row_list->row_count * row_list->join_count + 2] = value3;
    row_list->row_count++;
}

void appendJoinedRowID (struct RowList * dest_list, struct RowList * src_list, int src_index, int value) {
    if (dest_list->join_count != src_list->join_count + 1) {
        fprintf(stderr, "Cannot append joined rowid from list with size %d to size %d\n", src_list->join_count, dest_list->join_count);
        exit(-1);;
    }

    int dest_index = dest_list->row_count;
    int i = 0;
    for(; i < src_list->join_count; i++) {
        writeRowID(dest_list, i, dest_index, getRowID(src_list, i, src_index));
    }

    writeRowID(dest_list, i, dest_index, value);

    dest_list->row_count++;
}

void copyResultRow (struct RowList * dest_list, struct RowList * src_list, int src_index) {
    if (dest_list->join_count != src_list->join_count) {
        fprintf(stderr, "Cannot copy source result row to destination with differnce size (%d vs %d)\n", src_list->join_count, dest_list->join_count);
        exit(-1);
    }

    for(int i = 0; i < src_list->join_count; i++) {
        writeRowID(dest_list, i, dest_list->row_count, getRowID(src_list, i, src_index));
    }

    dest_list->row_count++;
}

void makeRowList (struct RowList * list, int join_count, int max_rows) {
    list->join_count = join_count;
    list->row_count = 0;

    if (join_count == 0) {
        // Special case for constant-only table-less query
        join_count = 1;
        // OK it wasn't that special...
    }

    int size = (sizeof (int *)) * join_count * max_rows;

    if (size < 0) {
        fprintf(stderr, "Not trying to allocate space for %d rows\n", max_rows);
        exit(-1);
    }

    list->row_ids = malloc(size);

    if (list->row_ids == NULL) {
        fprintf(stderr, "Cannot allocate space for %d rows\n", max_rows);
        exit(-1);
    }
}

void destroyRowList (struct RowList * list) {
    if (list->row_ids != NULL) {
        free(list->row_ids);
        list->row_ids = NULL;
    }
}

/**
 * @brief Copy contents of one row list to another
 *
 * Will destroy destination list first
 *
 * @param dest
 * @param src
 */
void copyRowList (struct RowList * dest, struct RowList * src) {
    destroyRowList(dest);

    dest->join_count = src->join_count;
    dest->row_count = src->row_count;
    dest->row_ids = src->row_ids;
}