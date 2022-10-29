#include <stdio.h>
#include <stdlib.h>

#include "structs.h"
#include "util.h"

static void prepareRowList (struct RowList * list, int join_count, int max_rows);

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
    // fprintf(stderr, "Append row %d @ idx %d\n", row_list->row_count, row_list->row_count * row_list->join_count);
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

/**
 * src and dest can be same RowList
 */
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

static void prepareRowList (struct RowList * list, int join_count, int max_rows) {
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

    // fprintf(stderr, "Allocated %d bytes for list->row_ids @ %p\n", size, list->row_ids);
}

void destroyRowList (struct RowList * list) {
    if (list->row_ids != NULL) {
        free(list->row_ids);
        list->row_ids = NULL;
    }
}

void reverseRowList (struct RowList * row_list) {
    if (row_list->join_count == 1) {
        // quick dirty implementation
        reverse_array(row_list->row_ids, row_list->row_count);
    }
    else {
        for (int i = 0; i < row_list->row_count / 2; i++) {
            int i1 = row_list->row_count - i - 1;

            for (int j = 0; j < row_list->join_count; j++) {
                int temp = getRowID(row_list, j, i1);
                writeRowID(row_list, j, i1, getRowID(row_list, j, i));
                writeRowID(row_list, j, i, temp);
            }
        }
    }
}

struct RowList *makeRowList (int join_count, int max_rows) {
    // Limits number of groups/working space
    static int max_size = 100;
    static int count = 0;
    static struct RowList *row_list_pool = NULL;

    if (row_list_pool == NULL) {
        row_list_pool = malloc(sizeof(*row_list_pool) * max_size);
    }

    if (count == max_size) {
        // We cannot realloc() row_list_pool to a new location because it would
        // invalidate all row_list pointers in use out in the wild.
        // We'll try to reallocate and check what location we're given. If our
        // previous allocation was enlarged then we can continue. Otherwise we
        // must terminate.

        max_size *= 2;
        int size = sizeof(*row_list_pool) * max_size;
        void *ptr = realloc(row_list_pool, size);
        if (ptr == NULL) {
            fprintf(stderr, "Unable to allocate %d bytes for a RowList\n", size);
            exit(-1);
        }

        // Were we given a larger allocation at the same location?
        if (ptr != row_list_pool) {
            fprintf(stderr, "Exhausted size of row_list pool: %d\n", max_size);
            exit(-1);
        }

        row_list_pool = ptr;
    }

    struct RowList *row_list = &row_list_pool[count++];

    prepareRowList(row_list, join_count, max_rows);

    return row_list;
}

void pushRowList(struct ResultSet *result_set, struct RowList *row_list) {
    // fprintf(stderr, "pushRowList()\n");
    // fprintf(stderr, "result_set: { row_lists @ %p, count = %d }\n", result_set->row_lists, result_set->count);
    result_set->row_lists[result_set->count++] = row_list;
}

struct RowList *popRowList(struct ResultSet *result_set) {
    if (result_set->count == 0) {
        return NULL;
    }

    return result_set->row_lists[--result_set->count];
}