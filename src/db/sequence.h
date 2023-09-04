#include "../structs.h"

int sequence_openDB (struct DB *db, const char *filename, char **resolved);

void sequence_closeDB (struct DB *db);

/**
 * Max header length 1024 characters
 */
int sequence_getFieldIndex (struct DB *db, const char *field);

char *sequence_getFieldName (struct DB *db, int field_index);

int sequence_getRecordCount (struct DB *db);

int sequence_getRecordValue (
    struct DB *db,
    int record_index,
    int field_index,
    char *value,
    size_t value_max_length
);

enum IndexSearchType sequence_findIndex(
    struct DB *db,
    const char *table_name,
    struct Node *node,
    int index_type_flags,
    char **resolved
);

int sequence_fullTableAccess (
    struct DB *db,
    RowListIndex list_id,
    struct Node *predicates,
    int predicate_count,
    int limit_value
);
