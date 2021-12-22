#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "db.h"
#include "indices.h"
#include "limits.h"

static int countFields (struct DB *db);

static int countLines (struct DB *db);

static int measureLine (struct DB *db, size_t byte_offset);

static void prepareHeaders (struct DB *db);

/**
 * Indices must point to enough memory to contain all the indices
 */
static int indexLines (struct DB *db, long *indices);

static void consumeStream (struct DB *db);

int csvMem_makeDB (struct DB *db, FILE *f) {
    db->vfs = VFS_CSV_MEM;
    db->file = f;

    // It would be nice to have a streaming solution but I don't think it's
    // realistically possible. We will just read the entire stream into memory.
    consumeStream(db);

    prepareHeaders(db);

    int line_count = countLines(db);

    db->line_indices = malloc((sizeof db->line_indices[0]) * line_count);

    indexLines(db, db->line_indices);

    db->record_count = line_count;

    return 0;
}

/**
 * Returns 0 on success; -1 on failure
 */
int csvMem_openDB (struct DB *db, const char *filename) {
    FILE *f;

    if (strcmp(filename, "stdin") == 0) {
        f = stdin;
    }
    else {
        f = fopen(filename, "r");
    }

    if (!f) {
        char buffer[255];
        sprintf(buffer, "%s.csv", filename);
        f = fopen(buffer, "r");

        if (!f) {
            return -1;
        }
    }

    return csvMem_makeDB(db, f);
}

void csvMem_closeDB (struct DB *db) {
    free(db->line_indices);
    db->line_indices = NULL;

    // db->data is in the same block as db->fields
    free(db->fields);
    db->fields = NULL;

    fclose(db->file);
    db->file = NULL;
}

static int countLines (struct DB *db) {
    int count = 0;
    size_t i = 0;

    while (db->data[i] != '\0') {
        if (db->data[i] == '\n'){
            count++;
        }
        i++;
    }

    // Check the last byte.
    // If the file ends in a new line, then fine
    // If it doesn't then we have one more line to count
    if (db->data[i-1] != '\n') count++;

    return count;
}

static int countFields (struct DB *db) {
    int count = 1;
    size_t i = 0;

    // Note: abritrary line limit
    while (db->data[i] != '\0') {
        if (db->data[i] == '\n'){
            return count;
        }

        if (db->data[i] == ','){
            count++;
        }

        i++;
    }

    return count;
}

/**
 * Including \n
 */
static int measureLine (struct DB *db, size_t byte_offset) {
    size_t i = byte_offset;

    while (db->data[i] != '\0') {
        if (db->data[i] == '\n'){
            return i;
        }

        i++;
    }

    return i;
}

/**
 * Indices must point to enough memory to contain all the indices
 */
static int indexLines (struct DB *db, long *indices) {
    int count = 0;
    size_t i = 0;

    indices[count] = i;

    while (db->data[i] != '\0') {
        if (db->data[i] == '\n'){
            indices[++count] = i + 1;
        }

        i++;
    }

    if (db->data[i] != '\n') count++;

    return count;
}

int csvMem_getFieldIndex (struct DB *db, const char *field) {
    char *curr_field = db->fields;

    for (int i = 0; i < db->field_count; i++) {
        if (strcmp(field, curr_field) == 0) {
            return i;
        }

        curr_field += strlen(curr_field) + 1;
    }

    return -1;
}

char *csvMem_getFieldName (struct DB *db, int field_index) {
    char *curr_field = db->fields;

    for (int i = 0; i < db->field_count; i++) {
        if (i == field_index) {
            return curr_field;
        }

        curr_field += strlen(curr_field) + 1;
    }

    return "\0";
}

/**
 * Returns the number of bytes read, or -1 on error
 */
int csvMem_getRecordValue (struct DB *db, int record_index, int field_index, char *value, size_t value_max_length) {
    if (record_index < 0 || record_index >= db->record_count) {
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

    db->field_count = countFields(db);

    int header_length = measureLine(db, 0);

    db->fields = db->data;

    db->data += header_length + 1;

    for (int i = 0; i < header_length; i++) {
        if(db->fields[i] == ',' || db->fields[i] == '\n' || db->fields[i] == '\r') {
            db->fields[i] = '\0';
        }
    }

    db->fields[header_length] = '\0';
}

// No Indexes on memory table
int csvMem_findIndex(__attribute__((unused)) struct DB *db, __attribute__((unused)) const char *table_name, __attribute__((unused)) const char *index_name, __attribute__((unused)) int index_type_flags) {
    return -1;
}

static void consumeStream (struct DB *db) {
    // 4 KB blocks
    int block_size = 4 * 1024;

    db->data = NULL;

    int read_size = -1;
    int block_count = 0;

    do {
        int offset = block_count * block_size;

        block_count++;

        if (db->data == NULL) {
            db->data = malloc(block_size);
        } else {
            void * ptr = realloc(db->data, block_size * block_count);

            if (ptr == NULL) {
                fprintf(stderr, "Unable to assign memory");
                exit(-1);
            }

            db->data = ptr;
        }

        read_size = fread(db->data + offset, block_size, 1, db->file);
    } while (read_size > 0);
}