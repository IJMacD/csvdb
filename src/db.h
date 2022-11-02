#include "structs.h"

int openDB (struct DB *db, const char *filename);

void closeDB (struct DB *db);

/**
 * Max header length 1024 characters
 */
int getFieldIndex (struct DB *db, const char *field);

char * getFieldName (struct DB *db, int field_index);

int getRecordCount (struct DB *db);

int getRecordValue (struct DB *db, int record_index, int field_index, char *value, size_t value_max_length);

enum IndexSearchType findIndex(struct DB *db, const char *table_name, const char *index_name, int index_type_flags);

int pkSearch(struct DB *db, const char *value);

int indexSearch(struct DB *db, const char *value, int rowid_field, int mode, int * output_flag);

int uniqueIndexSearch(struct DB *db, const char *value, int rowid_field, int * output_flag);

int fullTableScan (struct DB *db, struct RowList * row_list, struct Predicate *predicates, int predicate_count, int limit_value);

int fullTableAccess (struct DB *db, struct RowList * row_list, int limit);
