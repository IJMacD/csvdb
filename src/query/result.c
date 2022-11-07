#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../structs.h"
#include "../functions/util.h"

int getRowID (struct RowList * row_list, int join_id, int index) {
    return row_list->row_ids[index * row_list->join_count + join_id];
}

void writeRowID (struct RowList * row_list, int join_id, int index, int value) {
    row_list->row_ids[index * row_list->join_count + join_id] = value;
}

void appendRowID (struct RowList * row_list, int value) {
    if (row_list->join_count != 1) {
        fprintf(
            stderr,
            "Cannot append 1 row ID to list with %d joins\n",
            row_list->join_count
        );
        exit(-1);
    }
    row_list->row_ids[row_list->row_count * row_list->join_count] = value;
    row_list->row_count++;
    // fprintf(
    //     stderr,
    //     "Append row %d @ idx %d\n",
    //     row_list->row_count,
    //     row_list->row_count * row_list->join_count
    // );
}

void appendRowID2 (struct RowList * row_list, int value1, int value2) {
    if (row_list->join_count != 2) {
        fprintf(
            stderr,
            "Cannot append 2 row IDs to list with %d join(s)\n",
            row_list->join_count
        );
        exit(-1);
    }
    row_list->row_ids[row_list->row_count * row_list->join_count + 0] = value1;
    row_list->row_ids[row_list->row_count * row_list->join_count + 1] = value2;
    row_list->row_count++;
}

void appendRowID3 (struct RowList * row_list, int value1, int value2, int value3) {
    if (row_list->join_count != 3) {
        fprintf(
            stderr,
            "Cannot append 3 row IDs to list with %d join(s)\n",
            row_list->join_count
        );
        exit(-1);
    }
    row_list->row_ids[row_list->row_count * row_list->join_count + 0] = value1;
    row_list->row_ids[row_list->row_count * row_list->join_count + 1] = value2;
    row_list->row_ids[row_list->row_count * row_list->join_count + 2] = value3;
    row_list->row_count++;
}

/**
 * @brief Copy N rowids from source list and 1 new rowid, and append to
 * dest_list which must have a width of N+1
 *
 * @param dest_list
 * @param src_list
 * @param src_index
 * @param value
 */
void appendJoinedRowID (
    struct RowList * dest_list,
    struct RowList * src_list,
    int src_index,
    int value
) {
    if (dest_list->join_count != src_list->join_count + 1) {
        fprintf(
            stderr,
            "Cannot append joined rowid from list with size %d to size %d\n",
            src_list->join_count,
            dest_list->join_count
        );
        exit(-1);
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
 * @brief Copy all N rowids in a result row from src_list and append to
 * dest_list
 *
 * @param dest_list
 * @param src_list
 * @param src_index
 */
void copyResultRow (
    struct RowList * dest_list,
    struct RowList * src_list,
    int src_index
) {
    if (dest_list->join_count != src_list->join_count) {
        fprintf(
            stderr,
            "Cannot copy source result row to destination with different size "
            "(%d vs %d)\n",
            src_list->join_count,
            dest_list->join_count
        );
        exit(-1);
    }

    for(int i = 0; i < src_list->join_count; i++) {
        writeRowID(
            dest_list,
            i,
            dest_list->row_count,
            getRowID(src_list, i, src_index)
        );
    }

    dest_list->row_count++;
}

/**
 * @brief Flip all ros in a RowList. First row becomes last and vice-versa.
 *
 * @param row_list
 * @param limit -1 for no limit
 */
void reverseRowList (struct RowList * row_list, int limit) {
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

            if (limit > -1 && i >= limit) {
                break;
            }
        }
    }

    if (limit > -1 && limit < row_list->row_count) {
        row_list->row_count = limit;
    }
}

/**
 * @brief Copy all rows from one RowList to another RowList
 *
 * @param dest_list
 * @param src_list
 */
void copyRowList (struct RowList *dest_list, struct RowList *src_list) {
    if (src_list->join_count != dest_list->join_count) {
        fprintf(
            stderr,
            "copyRowList src_list and dest_list have different join counts (%d "
            "vs %d)\n",
            src_list->join_count,
            dest_list->join_count
        );
        exit(-1);
    }

    if (src_list->join_count == 1) {
        // quick dirty implementation
        memcpy(dest_list->row_ids, src_list->row_ids, src_list->row_count);
    }
    else {
        for (int i = 0; i < src_list->row_count; i++) {

            for (int j = 0; j < src_list->join_count; j++) {
                writeRowID(dest_list, j, i, getRowID(src_list, j, i));
            }
        }
    }
}

/**
 * @brief Swap a row at index_a with the row at index_b within the same RowList
 *
 * @param row_list
 * @param index_a
 * @param index_b
 */
void swapRows (struct RowList *row_list, int index_a, int index_b) {
    for (int i = 0; i < row_list->join_count; i++) {
        int tmp = getRowID(row_list, i, index_b);
        writeRowID(row_list, i, index_b, getRowID(row_list, i, index_a));
        writeRowID(row_list, i, index_a, tmp);
    }
}

static struct RowList *row_list_pool = NULL;

static int pool_count = 0;

static unsigned long pool_map = 0;

/**
 * @brief Get the RowList object from the pool by index.
 * Important: DO NOT hold on to this pointer for long.
 * More specifically do not hold on to it after a createRowList() call.
 *
 * @param index
 * @return struct RowList*
 */
struct RowList *getRowList (RowListIndex index) {
    if (index < 0) return NULL;
    return row_list_pool + index;
}

/**
 * @brief Create a Row List object in the pool on the heap and return an index
 *
 * @param join_count
 * @param max_rows
 * @return int index in pool
 */
RowListIndex createRowList (int join_count, int max_rows) {
    // Initial pool size
    static int max_size = 10;

    if (row_list_pool == NULL) {
        row_list_pool = malloc(sizeof(*row_list_pool) * max_size);
    }

    if (pool_count == max_size) {
        max_size *= 2;
        int size = sizeof(*row_list_pool) * max_size;
        void *ptr = realloc(row_list_pool, size);
        if (ptr == NULL) {
            fprintf(
                stderr,
                "Unable to allocate %d bytes for a RowList\n",
                size
            );
            exit(-1);
        }

        row_list_pool = ptr;

        // #ifdef DEBUG
        // fprintf(stderr, "Expanded RowList Pool: %d\n", max_size);
        // #endif
    }

    if (pool_count > (int)sizeof(pool_map) * 8) {
        // There's no coming back.
        // Don't bother tracking the pool allocations in a map any
        pool_map = -1;
    } else {
        pool_map |= (1ul << pool_count);
    }

    struct RowList *row_list = &row_list_pool[pool_count++];

    // Prepare RowList for use.
    row_list->join_count = join_count;
    row_list->row_count = 0;

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

    row_list->row_ids = malloc(size);

    if (row_list->row_ids == NULL) {
        fprintf(stderr, "Cannot allocate space for %d rows\n", max_rows);
        exit(-1);
    }

    #ifdef DEBUG
    fprintf(
        stderr,
        "Created RowList %d (max: %dj x %dr). Pool use: %d/%d\n",
        pool_count - 1,
        join_count,
        max_rows,
        pool_count,
        max_size
    );
    #endif

    return pool_count - 1;
}

void destroyRowList (RowListIndex row_list) {
    struct RowList *list = getRowList(row_list);

    if (list->row_ids != NULL) {
        free(list->row_ids);
        list->row_ids = NULL;
    }

    pool_map &= ~(1ul << row_list);

    // If every allocated row_list has been destroyed then we can reset the pool
    // to the start.
    if (pool_map == 0) {
        pool_count = 0;
    }
    // Otherwise, if we're destroying the most recent row_list in the pool we
    // can "return" it to the pool;
    else if (row_list == pool_count - 1) {
        pool_count--;
    }


    #ifdef DEBUG
    fprintf(
        stderr,
        "Destroyed RowList %d. Pool use: %d\n",
        row_list,
        pool_count
    );
    #endif
}

void pushRowList(struct ResultSet *result_set, RowListIndex row_list_index) {
    if (result_set->count == result_set->size) {
        int size = result_set->size * 2;
        void *ptr = realloc(result_set->row_list_indices, sizeof(int) * size);
        if (ptr == NULL) {
            fprintf(
                stderr,
                "Unable to reallocate memory for RowList. New size: %d\n",
                size
            );
            exit(-1);
        }
        result_set->row_list_indices = ptr;
        result_set->size = size;
    }

    // fprintf(stderr, "pushRowList()\n");
    // fprintf(
    //     stderr,
    //     "result_set: { row_lists @ %p, count = %d }\n",
    //     result_set->row_lists,
    //     result_set->count
    // );
    result_set->row_list_indices[result_set->count++] = row_list_index;
}

RowListIndex popRowList(struct ResultSet *result_set) {
    if (result_set->count == 0) {
        return -1;
    }

    return result_set->row_list_indices[--result_set->count];
}

/**
 * @brief Create a Result Set object
 *
 * @return struct ResultSet*
 */
struct ResultSet *createResultSet () {
    int default_size = 10;
    struct ResultSet *results = malloc(sizeof(*results));
    results->count = 0;
    results->size = default_size;
    results->row_list_indices = malloc(sizeof(int) * default_size);
    return results;
}

/**
 * @brief
 *
 * @param results
 */
void destroyResultSet (struct ResultSet *results) {
    free(results);
}