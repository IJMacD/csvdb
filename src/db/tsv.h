#include "../structs.h"

int tsv_openDB (struct DB *db, const char *filename, char **resolved);

void tsv_closeDB (struct DB *db);

/**
 * Max header length 1024 characters
 */
int tsv_getFieldIndex (struct DB *db, const char *field);

char *tsv_getFieldName (struct DB *db, int field_index);

int tsv_getRecordCount (struct DB *db);

int tsv_getRecordValue (
    struct DB *db,
    int record_index,
    int field_index,
    char *value,
    size_t value_max_length
);

enum IndexSearchType tsv_findIndex(
    struct DB *db,
    const char *table_name,
    struct Node *node,
    int index_type_flags,
    char **resolved
);

int tsv_fromHeaders (
    struct DB *db,
    const char *name,
    const char *headers
);

int tsv_fromQuery (
    struct DB *db,
    const char *name,
    const char *query,
    const char **end_ptr
);

int tsv_insertRow (struct DB *db, const char *row);

int tsv_insertFromQuery (
    struct DB *db,
    const char *query,
    const char **end_ptr
);
