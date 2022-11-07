#include "../structs.h"

int getRowID (struct RowList * row_list, int join_id, int index);

void writeRowID (struct RowList * row_list, int join_id, int index, int value);

void appendRowID (struct RowList * row_list, int value);

void appendRowID2 (struct RowList * row_list, int value1, int value2);

void appendRowID3 (
    struct RowList * row_list,
    int value1,
    int value2,
    int value3
);

void appendJoinedRowID (
    struct RowList * dest_list,
    struct RowList * src_list,
    int src_index,
    int value
);

void copyResultRow (
    struct RowList * dest_list,
    struct RowList * src_list,
    int src_index
);

void reverseRowList (struct RowList * row_list, int limit);

void copyRowList (struct RowList *dest_list, struct RowList *src_list);

void swapRows (struct RowList *row_list, int index_a, int index_b);

RowListIndex createRowList (int join_count, int max_rows);

void destroyRowList (RowListIndex list);

void pushRowList (struct ResultSet *result_set, RowListIndex row_list);

RowListIndex popRowList (struct ResultSet *result_set);

struct RowList *getRowList (RowListIndex index);

struct ResultSet *createResultSet ();

void destroyResultSet (struct ResultSet *results);
