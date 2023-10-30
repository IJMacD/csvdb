#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include <sys/mman.h>

#include "../structs.h"
#include "../functions/util.h"

static void prepareHeaders (struct DB *db);

static int indexLines (struct DB *db, int max_rows);

int csvMmap_makeDB (struct DB *db, FILE *f) {
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

    int result = csvMmap_makeDB(db, f);

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
    if (db->_record_count == -1) {
        indexLines(db, -1);
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
        if (indexLines(db, rowid + 1) < 0) {
            return -1;
        }
    }
    else {
        if (rowid >= db->_record_count) {
            return -1;
        }
    }

    long file_offset = db->line_indices[rowid];

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
            if (
                db->data[i] == '"'
                || (
                    !quoted_flag
                    && (
                        db->data[i] == ','
                        || db->data[i] == '\n'
                        || db->data[i] == '\r'
                    )
                )
            ) {

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

            // If we got to a newline and we're not in the correct field then
            // the field was not found
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

    char *read_ptr, *write_ptr;

    read_ptr = db->data;
    write_ptr = db->fields;

    // Overwrite fields buffer with itself skipping quotes and inserting nulls
    while (*read_ptr && *read_ptr != '\n')
    {
        if(
            *read_ptr == ','
            || *read_ptr == '\r'
        ) {
            *(write_ptr++) = '\0';
            db->field_count++;
        }
        else if (*read_ptr != '"') {
            *(write_ptr++) = *read_ptr;
        }

        read_ptr++;
    }

    *(write_ptr++) = '\0';

    db->data = read_ptr + 1;
}

/**
 * @brief Index all, or some lines in the file
 *
 * @param db
 * @param max_rows -1 for all rows
 * @return int return number of lines found
 */
static int indexLines (struct DB *db, int max_rows) {
    int count = 0;
    size_t i = 0;

    // max_size is current size of allocation
    // max_size is stored at start of allocation
    int *max_size = NULL;

    if (db->line_indices == NULL) {
        max_size = malloc(sizeof(*db->line_indices) * (32 + 1));
        // Save current size at start of allocation
        *max_size = 32;
        // False start
        db->line_indices = (void *)max_size + sizeof(*max_size);
    }
    else {
        void *ptr = db->line_indices;
        max_size = ptr - sizeof(*max_size);
    }

    // Check if we've already done a partial index
    if (db->_record_count < -1) {
        count = -db->_record_count - 1;

        if (max_rows <= count) {
            // We've already indexed as much as we need to
            return count;
        }

        // Jump ahead in file to avoid unnecessary repetition
        i = db->line_indices[count];
    }

    db->line_indices[count++] = i;

    int quoted = 0;

    while (db->data[i] != '\0') {
        if (db->data[i] == '\n' && !quoted){
            if (count == *max_size) {
                *max_size *= 2;
                // max_size is the real location of the allocation

                void *ptr = realloc(
                    max_size,
                    sizeof(*db->line_indices) * *max_size + sizeof(*max_size)
                );

                if (ptr == NULL) {
                    fprintf(
                        stderr,
                        "Unable to allocate memory for %d line_indices\n",
                        *max_size
                    );
                    exit(-1);
                }
                // save new memory addresses
                max_size = ptr;
                db->line_indices = ptr + sizeof(*max_size);
            }

            db->line_indices[count++] = i + 1;

            if (max_rows > -1 && count >= max_rows) {
                count--;

                // Save count as negative number to indicate full scan has not
                // yet taken place.
                db->_record_count = -count - 1;

                return count;
            }
        }
        else if (db->data[i] == '"') {
            quoted = ~quoted;
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
