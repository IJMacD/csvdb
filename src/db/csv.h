#include "../structs.h"

int csv_openDB (struct DB *db, const char *filename, char **resolved);

void csv_closeDB (struct DB *db);

/**
 * Max header length 1024 characters
 */
int csv_getFieldIndex (struct DB *db, const char *field);

char *csv_getFieldName (struct DB *db, int field_index);

int csv_getRecordCount (struct DB *db);

int csv_getRecordValue (
    struct DB *db,
    int record_index,
    int field_index,
    char *value,
    size_t value_max_length
);

enum IndexSearchType csv_findIndex(
    struct DB *db,
    const char *table_name,
    const char *index_name,
    int index_type_flags
);

int csv_fromHeaders (
    struct DB *db,
    const char *name,
    const char *headers
);

int csv_fromQuery (
    struct DB *db,
    const char *name,
    const char *query,
    const char **end_ptr
);

int csv_insertRow (struct DB *db, const char *row);

int csv_insertFromQuery (
    struct DB *db,
    const char *query,
    const char **end_ptr
);
