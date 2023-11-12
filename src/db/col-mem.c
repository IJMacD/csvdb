#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "helper.h"
#include "../structs.h"
#include "../functions/util.h"
#include "../query/query.h"

static int prepareHeaders (struct DB *db);

static int makeDB (struct DB *db, FILE *f);

#define VFS_COL_MAX_FIELD_COUNT 32

struct Fields {
    int fieldStart[VFS_COL_MAX_FIELD_COUNT];
    char fieldNames[VFS_COL_MAX_FIELD_COUNT * MAX_FIELD_LENGTH];
};

/**
 * @brief Opens, consumes, and closes file specified by filename
 * Whitespace Seperated Values i.e. output from standard commands
 *
 * @param db
 * @param filename can also be "stdin.col"
 * @param resolved if not NULL, then will write resolved path to buffer pointed
 * to by this pointer. If this pointer points to NULL then a buffer will be
 * malloc'd for it.
 */
int colMem_openDB (struct DB *db, const char *filename, char **resolved) {
    FILE *f = NULL;

    if (strcmp(filename, "stdin.col") == 0) {
        f = stdin;
    }
    else if (ends_with(filename, ".col")) {
        f = fopen(filename, "r");

        if (resolved != NULL) {
            *resolved = realpath(filename, *resolved);
        }
    }

    if (!f) {
        return -1;
    }

    int result = makeDB(db, f);

    fclose(f);

    return result;
}

void colMem_closeDB (struct DB *db) {
    if (db->line_indices != NULL) {
        // max_size of allocation is stored at start of real block
        void *ptr = db->line_indices;
        free(ptr - sizeof(int));
        db->line_indices = NULL;
    }

    if (db->data != NULL) {
        free(db->data);
        db->data = NULL;
    }

    if (db->fields != NULL) {
        free(db->fields);
        db->fields = NULL;
    }
}

int colMem_getFieldIndex (struct DB *db, const char *field) {
    struct Fields *fields = (struct Fields *)db->fields;
    char *curr_field = fields->fieldNames;

    for (int i = 0; i < db->field_count; i++) {
        if (strcmp(field, curr_field) == 0) {
            return i;
        }

        curr_field += strlen(curr_field) + 1;
    }

    return -1;
}

char *colMem_getFieldName (struct DB *db, int field_index) {
    struct Fields *fields = (struct Fields *)db->fields;
    char *curr_field = fields->fieldNames;

    for (int i = 0; i < db->field_count; i++) {
        if (i == field_index) {
            return curr_field;
        }

        curr_field += strlen(curr_field) + 1;
    }

    return "\0";
}

int colMem_getRecordCount (struct DB *db) {
    if (db->_record_count < 0) {
        indexLines(db, -1, '\0');
    }

    return db->_record_count - 1;
}

/**
 * Records can have embedded spaces.
 * Trailing spaces will be trimmed.
 * Returns the number of bytes read, or -1 on error
 */
int colMem_getRecordValue (
    struct DB *db,
    int record_index,
    int field_index,
    char *value,
    size_t value_max_length
) {
    if (db->_record_count < 0) {
        // Just index as many rows as we need
        if (indexLines(db, record_index + 1, '"') < 0) {
            return -1;
        }
    }
    else {
        if (record_index >= colMem_getRecordCount(db)) {
            return -1;
        }
    }

    if (field_index < 0 || field_index >= db->field_count) {
        return -1;
    }

    struct Fields *fields = (struct Fields *)db->fields;

    long file_offset = db->line_indices[record_index + 1] + fields->fieldStart[field_index];
    // Find maximum extent of field. Trailing whitespace will be trimmed later.
    // End of last field in line can be found as start of next line.
    long file_offset_end = field_index == db->field_count - 1 ?
        db->line_indices[record_index + 2] :
        db->line_indices[record_index + 1] + fields->fieldStart[field_index + 1];

    char *in_ptr = db->data + file_offset;
    char *in_ptr_end = db->data + file_offset_end;
    char *out_ptr = value;
    char *out_start_ptr = out_ptr;
    char *out_end_ptr = out_ptr + value_max_length - 1;

    char *last_non_space = out_ptr;

    // Consume whole field
    while (in_ptr != in_ptr_end && *in_ptr != '\n') {
        *out_ptr = *(in_ptr++);

        // Have we run out of space?
        if (out_ptr == out_end_ptr) {
            break;
        }

        if (!isspace(*out_ptr)) {
            last_non_space = out_ptr;
        }

        out_ptr++;
    }

    // Go back to last non-space character
    out_ptr = last_non_space + 1;

    *(out_ptr++) = '\0';

    return out_ptr - out_start_ptr;
}

static int makeDB (struct DB *db, FILE *f) {
    db->vfs = VFS_COL_MEM;

    // It would be nice to have a streaming solution but I don't think it's
    // realistically possible. We will just read the entire stream into memory.
    consumeStream(db, f);

    int result = prepareHeaders(db);
    if (result < 0) {
        return -1;
    }

    db->_record_count = -1;

    return 0;
}

/**
 * Mallocs buffer to hold field data.
 * Copies field names into field buffer.
 * Records field widths (as series of field offsets) in field buffer.
 * Field widths are determined by whitespace in header line. Headers names
 * must be separated by at least two spaces.
 */
static int prepareHeaders (struct DB *db) {
    db->field_count = 1;

    struct Fields *fields = malloc(sizeof *fields);

    db->fields = (void *)fields;

    if (db->data[0] == '\0') {
        fprintf(stderr, "Empty file\n");
        return -1;
    }

    char *read_ptr, *write_ptr;

    read_ptr = db->data;
    write_ptr = fields->fieldNames;

    fields->fieldStart[0] = 0;

    while (*read_ptr && *read_ptr != '\n')
    {
        if(isspace(*read_ptr)) {
            read_ptr++;

            // Found a space, check for double space
            if (!isspace(*read_ptr)) {
                // We didn't have double space. Write the space we just consumed
                // as well as the next char and continue with the loop.
                *(write_ptr++) = ' ';
                *(write_ptr++) = *(read_ptr++);
                continue;
            }

            // Record end of field in fields struct
            *(write_ptr++) = '\0';

            // Consume all following whitespace except newline
            while (*read_ptr != '\0' && *read_ptr != '\n' && isspace(*read_ptr)) {
                read_ptr++;
            }

            // Start of next field
            fields->fieldStart[db->field_count] = (read_ptr - db->data);

            if (*read_ptr != '\0' && *read_ptr != '\n') {
                db->field_count++;

                if (db->field_count >= VFS_COL_MAX_FIELD_COUNT) {
                    fprintf(stderr, "Didn't reserve enough space for fields in VFS COL");
                    exit(-1);
                }
            }
        }
        else {
            *(write_ptr++) = *(read_ptr++);
        }
    }

    *(write_ptr++) = '\0';

    return 0;
}

// No Indexes on memory table
int colMem_findIndex(
    __attribute__((unused)) struct DB *db,
    __attribute__((unused)) const char *table_name,
    __attribute__((unused)) struct Node *node,
    __attribute__((unused)) int index_type_flags,
    __attribute__((unused)) char **resolved
) {
    return 0;
}
