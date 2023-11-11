#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include <sys/mman.h>

#include "helper.h"
#include "../structs.h"
#include "../functions/util.h"
#include "../functions/csv.h"

static void prepareHeaders (struct DB *db);

static int makeDB (struct DB *db, FILE *f);

/**
 * @brief Opens, consumes, and closes file specified by filename
 *
 * @param db
 * @param filename
 * @param resolved if not NULL, then will write resolved path to buffer pointed
 * to by this pointer. If this pointer points to NULL then a buffer will be
 * malloc'd for it.
 * @returns int 0 on success; -1 on failure
 */
int csvMmap_openDB (struct DB *db, const char *filename, char **resolved) {
    FILE *f = fopen(filename, "r");

    if (f != NULL && resolved != NULL) {
        *resolved = realpath(filename, *resolved);
    }

    if (!f) {
        char buffer[FILENAME_MAX];
        sprintf(buffer, "%s.csv", filename);
        f = fopen(buffer, "r");

        if (!f) {
            return -1;
        }

        if (resolved != NULL) {
            *resolved = realpath(buffer, *resolved);
        }
    }

    int result = makeDB(db, f);

    fclose(f);

    return result;
}

void csvMmap_closeDB (struct DB *db) {
    if (db->line_indices != NULL) {
        // Rough size, and rough start location.
        // Exact values aren't required.
        // We'll do our best, especially if we've only done a partial index
        int final_index = db->_record_count < 0
            ? -db->_record_count - 1 : db->_record_count;
        int file_size = db->line_indices[final_index];
        munmap(db->data, file_size);

        // max_size of allocation is stored at start of real block
        void *ptr = db->line_indices;
        free(ptr - sizeof(int));
        db->line_indices = NULL;
    }

    if (db->fields != NULL) {
        free(db->fields);
        db->fields = NULL;
    }
}

int csvMmap_getFieldIndex (struct DB *db, const char *field) {
    char *curr_field = db->fields;

    for (int i = 0; i < db->field_count; i++) {
        if (strcmp(field, curr_field) == 0) {
            return i;
        }

        curr_field += strlen(curr_field) + 1;
    }

    return -1;
}

char *csvMmap_getFieldName (struct DB *db, int field_index) {
    char *curr_field = db->fields;

    for (int i = 0; i < db->field_count; i++) {
        if (i == field_index) {
            return curr_field;
        }

        curr_field += strlen(curr_field) + 1;
    }

    return "\0";
}


int csvMmap_getRecordCount (struct DB *db) {
    if (db->_record_count < 0) {
        indexLines(db, -1, '"');
    }

    return db->_record_count;
}

/**
 * Returns the number of bytes read, or -1 on error
 */
int csvMmap_getRecordValue (
    struct DB *db,
    int rowid,
    int field_index,
    char *value,
    size_t value_max_length
) {
    if (rowid < 0) {
        return -1;
    }

    if (field_index < 0 || field_index >= db->field_count) {
        return -1;
    }

    if (db->_record_count < 0) {
        // Just index as many rows as we need
        if (indexLines(db, rowid + 1, '"') < 0) {
            return -1;
        }
    }
    else {
        if (rowid >= db->_record_count) {
            return -1;
        }
    }

    long file_offset = db->line_indices[rowid];

    return csv_get_record_from_line(db->data + file_offset, field_index, value, value_max_length);
}

static int makeDB (struct DB *db, FILE *f) {
    db->vfs = VFS_CSV_MMAP;
    db->file = NULL;
    db->line_indices = NULL;

    if (fseek(f, 0, SEEK_END)) {
        // Can only mmap a seekable file
        return -1;
    }
    size_t size = ftell(f);

    db->data = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fileno(f), 0);

    if (db->data == MAP_FAILED) {
        return -1;
    }

    prepareHeaders(db);

    db->_record_count = -1;

    return 0;
}

static void prepareHeaders (struct DB *db) {
    db->field_count = 1;

    db->fields = malloc(MAX_TABLE_LENGTH);

    char *read_ptr, *write_ptr;

    read_ptr = db->data;
    write_ptr = db->fields;

    // Suppoprt Excel CSV UTF-8.
    // Check for BOM
    if (
        read_ptr[0] == '\xef' &&
        read_ptr[1] == '\xbb' &&
        read_ptr[2] == '\xbf'
    ) {
        read_ptr += 3;
    }

    // Overwrite fields buffer with itself skipping quotes and inserting nulls
    while (*read_ptr && *read_ptr != '\n')
    {
        if(*read_ptr == ',') {
            *(write_ptr++) = '\0';
            db->field_count++;
        }
        else if (*read_ptr != '"' && *read_ptr != '\r') {
            *(write_ptr++) = *read_ptr;
        }

        read_ptr++;
    }

    *(write_ptr++) = '\0';

    db->data = read_ptr + 1;
}
