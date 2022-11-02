#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>

#include "structs.h"
#include "date.h"

static int max_count = 1000;

static char *field_names[] = {
    "inode",
    "name",
    "type",
    "path",
    "size",
    "created",
    "modified",
};

static int makeDB(struct DB *db, const char * path);

static struct dirent * getDirectoryEntry (struct DB *db, int record_index);

static int unixToJulian (long time);

int dir_openDB (struct DB *db, const char *filename) {
    if (strncmp(filename, "DIR(", 4) != 0) {
        return -1;
    }

    db->vfs = VFS_DIR;
    db->field_count = sizeof(field_names) / sizeof(field_names[0]);

    char path[MAX_TABLE_LENGTH];

    int len = strlen(filename) - 4;
    strncpy(path, filename + 4, MAX_TABLE_LENGTH - 1);
    path[len - 1] = '\0';

    makeDB(db, path);

    return 0;
}

void dir_closeDB (struct DB *db) {
    free(db->data);
}

int dir_getFieldIndex (struct DB *db, const char *field) {
    for (int i = 0; i < db->field_count; i++) {
        if (strcmp(field, field_names[i]) == 0) {
            return i;
        }
    }

    return -1;
}

char *dir_getFieldName (__attribute__((unused)) struct DB *db, int field_index) {
    return field_names[field_index];
}

int dir_getRecordCount (struct DB *db) {
    return db->_record_count;
}

int dir_getRecordValue (struct DB *db, int record_index, int field_index, char *value, __attribute__((unused)) size_t value_max_length) {

    struct dirent *dp = getDirectoryEntry(db, record_index);
    char path[MAX_TABLE_LENGTH * 2];

    // inode
    if (field_index == 0) {
        return sprintf(value, "%ld", dp->d_ino);
    }

    // name
    if (field_index == 1) {
        return sprintf(value, "%s", dp->d_name);
    }

    // type
    if (field_index == 2) {
        if (dp->d_type == DT_DIR)
            return sprintf(value, "%c", 'd');
        if (dp->d_type == DT_REG)
            return sprintf(value, "%c", 'f');
        if (dp->d_type == DT_LNK)
            return sprintf(value, "%c", 's');
        return 0;
    }

    // The rest of the fields require the path
    strcpy(path, db->data);
    int len = strlen(path);
    strcpy(path + len, dp->d_name);

    // path
    if (field_index == 3) {
        return sprintf(value, "%s", path);
    }

    // The rest of the fields require stat
    struct stat s;
    if(stat(path, &s)) { return 0; }

    // size
    if (field_index == 4) {
        if (dp->d_type != DT_REG) {
            return 0;
        }

        return sprintf(value, "%ld", s.st_size);
    }

    // created
    if (field_index == 5) {
        struct DateTime dt;
        datetimeFromJulian(&dt, unixToJulian(s.st_ctime));
        return sprintf(value, "%04d-%02d-%02d", dt.year, dt.month, dt.day);
    }

    // modified
    if (field_index == 6) {
        struct DateTime dt;
        datetimeFromJulian(&dt, unixToJulian(s.st_mtime));
        return sprintf(value, "%04d-%02d-%02d", dt.year, dt.month, dt.day);
    }

    return -1;
}

// All queries go through fullTableScan
int dir_findIndex(__attribute__((unused)) struct DB *db, __attribute__((unused)) const char *table_name, __attribute__((unused)) const char *index_name, __attribute__((unused)) int index_type_flags) {
    return 0;
}

static int makeDB(struct DB *db, const char * path) {
    db->_record_count = 0;
    db->data = NULL;

    DIR *dfd = opendir(path);
    if (dfd == NULL) {
        return -1;
    }

    struct dirent *dp;

    // man7.org says sizeof(*dp) is unreliable
    db->data = malloc(MAX_TABLE_LENGTH + max_count * sizeof(*dp));

    // Put directory path in data
    strcpy(db->data, path);
    int len = strlen(path);
    // Add directory separator
    db->data[len] = '/';
    db->data[len + 1] = '\0';

    while((dp = readdir(dfd)) != NULL) {
        memcpy(getDirectoryEntry(db, db->_record_count), dp, sizeof(*dp));

        db->_record_count++;

        if (db->_record_count >= max_count) {
            break;
        }
    }

    closedir(dfd);

    return 0;
}

static struct dirent * getDirectoryEntry (struct DB *db, int record_index) {
    return (struct dirent *)&db->data[MAX_TABLE_LENGTH + record_index * sizeof(struct dirent)];
}

static int unixToJulian (long time) {
    return 2440587 + time / 86400;
}