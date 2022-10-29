#include "structs.h"

int getRowID (struct RowList * row_list, int join_id, int index);

void writeRowID (struct RowList * row_list, int join_id, int index, int value);

void appendRowID (struct RowList * row_list, int value);

void appendRowID2 (struct RowList * row_list, int value1, int value2);

void appendRowID3 (struct RowList * row_list, int value1, int value2, int value3);

void appendJoinedRowID (struct RowList * dest_list, struct RowList * src_list, int src_index, int value);

void copyResultRow (struct RowList * dest_list, struct RowList * src_list, int src_index);

void destroyRowList (struct RowList * list);

void reverseRowList (struct RowList * row_list);

struct RowList *makeRowList (int join_count, int max_rows);

void pushRowList(struct ResultSet *result_set, struct RowList *row_list);

struct RowList *popRowList(struct ResultSet *result_set);