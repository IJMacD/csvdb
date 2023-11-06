#include "../structs.h"

int openDB (struct DB *db, const char *filename, char **resolved);

void closeDB (struct DB *db);

int getFieldIndex (struct DB *db, const char *field);

char * getFieldName (struct DB *db, int field_index);

int getRecordCount (struct DB *db);

int getRecordValue (
    struct DB *db,
    int record_index,
    int field_index,
    char *value,
    size_t value_max_length
);

enum IndexSearchType findIndex(
    struct DB *db,
    const char *table_name,
    struct Node *node,
    int index_type_flags,
    char **resolved
);

int pkSearch(struct DB *db, const char *value);

int indexSearch (
    struct DB *db,
    const char *value,
    enum IndexScanMode mode,
    enum IndexSearchResult *output_flag
);

int uniqueIndexSearch(struct DB *db, const char *value, int * output_flag);

int fullTableAccess (
    struct DB *db,
    int row_list,
    struct Node *nodes,
    int node_count,
    int limit_value
);

int fullTableScan (
    struct DB *db,
    int row_list,
    int rowid_start,
    int limit
);

int insertRow (
    struct DB *db,
    const char *row
);

int insertFromQuery (
    struct DB *db,
    const char *query,
    const char **end_ptr
);