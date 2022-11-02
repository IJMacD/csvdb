#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include <sys/mman.h>

#include "structs.h"
#include "util.h"

static void prepareHeaders (struct DB *db);

static int indexLines (struct DB *db);

int csvMmap_makeDB (struct DB *db, FILE *f) {
    db->vfs = VFS_CSV_MMAP;
    db->file = NULL;

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

    indexLines(db);

    return 0;
}

/**
 * @brief Opens, consumes, and closes file specified by filename
 *
 * @param db
 * @param filename can also be "stdin"
 * @returns int 0 on success; -1 on failure
 */
int csvMmap_openDB (struct DB *db, const char *filename) {
    FILE *f = fopen(filename, "r");

    if (!f) {
        char buffer[FILENAME_MAX];
        sprintf(buffer, "%s.csv", filename);
        f = fopen(buffer, "r");

        if (!f) {
            return -1;
        }
    }

    int result = csvMmap_makeDB(db, f);

    fclose(f);

    return result;
}

void csvMmap_closeDB (struct DB *db) {
    if (db->line_indices != NULL) {
        // Rough size, and rough start location
        // Exact values aren't required
        int file_size = db->line_indices[db->_record_count];
        munmap(db->data, file_size);

        free(db->line_indices);
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
    return db->_record_count;
}

/**
 * Returns the number of bytes read, or -1 on error
 */
int csvMmap_getRecordValue (struct DB *db, int record_index, int field_index, char *value, size_t value_max_length) {
    if (record_index < 0 || record_index >= db->_record_count) {
        return -1;
    }

    if (field_index < 0 || field_index >= db->field_count) {
        return -1;
    }

    long file_offset = db->line_indices[record_index];

    int current_field_index = 0;
    size_t char_index = 0;
    int quoted_flag = 0;
    size_t i = file_offset;

    while (db->data[i] != '\0') {
        if (char_index == 0 && db->data[i] == '"') {
            quoted_flag = !quoted_flag;
            i++;
            continue;
        }

        // Are we currently in the correct field?
        if (current_field_index == field_index) {

            // Have we found the end of a quoted value?
            // We've found the end of a record
            if (db->data[i] == '"' || (!quoted_flag && (db->data[i] == ',' || db->data[i] == '\n' || db->data[i] == '\r'))) {

                // There might be quotes in the middle of a values, who cares?
                // Let's just ignore that and pretend it won't happen

                // finish off the string and return the length
                value[char_index] = '\0';
                return char_index;
            }

            // Copy the current byte
            value[char_index++] = db->data[i];

            // If we've run out of storage space
            if (char_index > value_max_length) {
                return -1;
            }
        } else {
            // If we've found a comma we're moving on to the next field
            if (!quoted_flag && db->data[i] == ',') {
                current_field_index++;
            }

            // If we got to a newline and we're not in the correct field then the field was not found
            if (db->data[i] == '\n') {
                return -1;
            }
        }

        i++;
    }

    // Getting the very last record from the file
    if (current_field_index == field_index) {
        value[char_index] = '\0';
        return char_index;
    }

    // Ran out of file
    return -1;
}

static void prepareHeaders (struct DB *db) {
    db->field_count = 1;

    db->fields = malloc(MAX_TABLE_LENGTH);

    int i = 0;

    while (db->data[i] != '\n') {
        if (db->data[i] == ',') {
            db->fields[i] = '\0';
            db->field_count++;
        }
        else {
            db->fields[i] = db->data[i];
        }

        i++;

        if (i >= MAX_TABLE_LENGTH) {
            fprintf(stderr, "Unable to process header longer than %d bytes\n", i);
            exit(-1);
        }
    }
    db->fields[i] = '\0';

    db->data += i + 1;
}

static int indexLines (struct DB *db) {
    int count = 0;
    size_t i = 0;

    int max_size = 32;

    db->line_indices = malloc(sizeof(*db->line_indices) * max_size);

    db->line_indices[count++] = i;

    while (db->data[i] != '\0') {
        if (db->data[i] == '\n'){
            db->line_indices[count++] = i + 1;
        }

        if (count == max_size) {
            max_size *= 2;
            void *ptr = realloc(db->line_indices, sizeof(*db->line_indices) * max_size);
            if (ptr == NULL) {
                fprintf(stderr, "Unable to allocate memory for %d line_indices\n", max_size);
                exit(-1);
            }
            db->line_indices = ptr;
        }

        i++;
    }

    if (db->data[i-1] == '\n') {
        count--;
    }

    // Add final count to track file size
    db->line_indices[count] = i;

    db->_record_count = count;

    return count;
}
