#include "db.h"
#include "db-csv.h"

int openDB (struct DB *db, const char *filename) {
    int result = csv_openDB(db, filename);

    if (result == 0) {
        return 0;
    }

    return result;
}

void closeDB (struct DB *db) {
    if (db->vfs == VFS_CSV) {
        csv_closeDB(db);
    }
}

int getFieldIndex (struct DB *db, const char *field) {
    if (db->vfs == VFS_CSV) {
        return csv_getFieldIndex(db, field);
    }

    return -1;
}

char *getFieldName (struct DB *db, int field_index) {
    if (db->vfs == VFS_CSV) {
        return csv_getFieldName(db, field_index);
    }

    return NULL;
}

int getRecordValue (struct DB *db, int record_index, int field_index, char *value, size_t value_max_length) {
    if (db->vfs == VFS_CSV) {
        return csv_getRecordValue(db, record_index, field_index, value, value_max_length);
    }

    return -1;
}
