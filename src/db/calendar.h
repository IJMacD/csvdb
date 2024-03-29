#include "../structs.h"

int calendar_openDB (struct DB *db, const char *filename, char **resolved);

void calendar_closeDB (struct DB *db);

/**
 * Max header length 1024 characters
 */
int calendar_getFieldIndex (struct DB *db, const char *field);

char *calendar_getFieldName (struct DB *db, int field_index);

int calendar_getRecordCount (struct DB *db);

int calendar_getRecordValue (
    struct DB *db,
    int record_index,
    int field_index,
    char *value,
    size_t value_max_length
);

enum IndexSearchType calendar_findIndex(
    struct DB *db,
    const char *table_name,
    struct Node *node,
    int index_type_flags,
    char **resolved
);

int calendar_fullTableAccess (
    struct DB *db,
    RowListIndex list_id,
    struct Node *nodes,
    int node_count,
    int limit_value
);

int calendar_indexSearch(
    struct DB *db,
    const char *value,
    int mode,
    int * output_flag
);