#include "structs.h"

int sequence_openDB (struct DB *db, const char *filename);

void sequence_closeDB (struct DB *db);

/**
 * Max header length 1024 characters
 */
int sequence_getFieldIndex (struct DB *db, const char *field);

char *sequence_getFieldName (struct DB *db, int field_index);

int sequence_getRecordValue (struct DB *db, int record_index, int field_index, char *value, size_t value_max_length);

int sequence_findIndex(struct DB *db, const char *table_name, const char *index_name, int index_type_flags);

