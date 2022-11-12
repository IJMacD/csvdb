#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "../structs.h"
#include "../functions/util.h"

static int countFields (const char *ptr);

static int prepareHeaders (struct DB *db);

static int indexLines (struct DB *db);

static void consumeStream (struct DB *db, FILE *stream);

static char * get_end_of_data (struct DB *db);

int csvMem_makeDB (struct DB *db, FILE *f) {
    db->vfs = VFS_CSV_MEM;

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
 * @brief Opens, consumes, and closes file specified by filename
 *
 * @param db
 * @param filename can also be "stdin"
 * @returns int 0 on success; -1 on failure
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
        char buffer[FILENAME_MAX];
        sprintf(buffer, "%s.csv", filename);
        f = fopen(buffer, "r");

        if (!f) {
            return -1;
        }
    }

    int result = csvMem_makeDB(db, f);

    fclose(f);

    return result;
}

void csvMem_closeDB (struct DB *db) {
    if (db->line_indices != NULL) {
        free(db->line_indices);
        db->line_indices = NULL;
    }

    if (db->fields != NULL) {
        // db->data is in the same block as db->fields
        free(db->fields);
        db->fields = NULL;
    }
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

int csvMem_getRecordCount (struct DB *db) {
    if (db->_record_count == -1) {
        indexLines(db);
    }

    return db->_record_count;
}

/**
 * Returns the number of bytes read, or -1 on error
 */
int csvMem_getRecordValue (
    struct DB *db,
    int record_index,
    int field_index,
    char *value,
    size_t value_max_length
) {
    if (db->_record_count == -1) {
        indexLines(db);
    }

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
            // the field was not found.
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

static int prepareHeaders (struct DB *db) {
    db->field_count = 1;

    db->fields = db->data;

    int i = 0;

    if (db->data[0] == '\0') {
        fprintf(stderr, "Empty file\n");
        return -1;
    }

    while (db->data[i] != '\n' && db->data[i] != '\0') {

        if (db->data[i] == ',') {
            db->fields[i] = '\0';
            db->field_count++;
        }

        i++;
    }

    db->fields[i] = '\0';

    db->data += i + 1;

    return 0;
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

            void *ptr = realloc(
                db->line_indices,
                sizeof(*db->line_indices) * max_size
            );

            if (ptr == NULL) {
                fprintf(
                    stderr,
                    "Unable to allocate memory for %d line_indices\n",
                    max_size
                );
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


// No Indexes on memory table
int csvMem_findIndex(
    __attribute__((unused)) struct DB *db,
    __attribute__((unused)) const char *table_name,
    __attribute__((unused)) const char *index_name,
    __attribute__((unused)) int index_type_flags
) {
    return 0;
}

static void consumeStream (struct DB *db, FILE *stream) {
    // 4 KB blocks
    int block_size = 4 * 1024;

    db->data = NULL;

    int read_size = -1;
    int block_count = 0;

    int offset = 0;

    do {
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

        read_size = fread(db->data + offset, 1, block_size, stream);

        offset += read_size;
    } while (read_size > 0);

    // Add null terminator to end of stream.
    // Necessary when more than one query is executed in same process.
    db->data[offset] = '\0';
}

/**
 * @brief Convert from VALUES layout e.g. ('a',1),('b',2) to csv in memory
 *
 * @param db
 * @param input
 * @param length Maximum length to read from input. -1 for no limit
 * @returns char * pointer to end of query (i.e. if it came accross a '\0' or
 * ';')
 */
const char *csvMem_fromValues(struct DB *db, const char *input, int length) {
    const char *in_ptr = input;
    const char *end_ptr = input + length - 1;
    char *out_ptr;

    db->vfs = VFS_CSV_MEM;
    db->file = NULL;

    size_t max_data_size = MAX_VALUE_LENGTH;

    db->data = malloc(max_data_size);
    out_ptr = db->data;

    int max_line_count = 100;

    db->line_indices = malloc(max_line_count * sizeof (long));

    // Headers
    db->line_indices[0] = 0;
    const char headers[]
        = "col1\0col2\0col3\0col4\0col5\0col6\0col7\0col8\0col9\0col10\n";
    memcpy(out_ptr, headers, sizeof(headers));
    out_ptr += sizeof(headers);

    db->fields = db->data;
    db->data += sizeof(headers);

    int line_index = 0;

    while(*in_ptr != '\0' && *in_ptr != ';' && in_ptr != end_ptr) {

        if (*in_ptr != '(') {
            fprintf(
                stderr,
                "VALUES: expected row %d to start with '('. Found: %c\n",
                line_index,
                *in_ptr
            );
            exit(-1);
        }

        db->line_indices[line_index++] = out_ptr - db->data;

        // Check to see if we need to extend the line index allocation
        if (line_index == max_line_count) {
            max_line_count *= 2;
            size_t size = max_line_count * sizeof(long);
            long *ptr = realloc(db->line_indices, size);
            if (ptr == NULL) {
                fprintf(stderr, "Unable to allocate memory: %ld\n", size);
                exit(-1);
            }
            db->line_indices = ptr;
        }

        int line_length = find_matching_parenthesis(in_ptr);

        // Check to see if we need to extend the data allocation
        // NOTE: db->fields is the start of the allocation
        size_t len = out_ptr - db->fields;
        if (len + line_length >= max_data_size) {
            max_data_size *= 2;
            // NOTE: db->fields is the start of the allocation
            void *ptr = realloc(db->fields, max_data_size);
            if (ptr == NULL) {
                fprintf(
                    stderr,
                    "Unable to allocate memory: %ld\n",
                    max_data_size
                );
                exit(-1);
            }
            db->fields = ptr;
            db->data = ptr + sizeof(headers);
            out_ptr = db->data + len;
        }

        strncpy(out_ptr, in_ptr + 1, line_length - 2);

        // Swap quotes '\'' -> '"'
        // Warning: will break quotes in string (converts to space)
        // Warning: can't cope with escaped quotes in string
        for (int i = 0; i < line_length - 2; i++) {
            if (*out_ptr == '\'') *out_ptr = '"';
            else if (*out_ptr == '"') *out_ptr = ' ';
            out_ptr++;
        }

        in_ptr += line_length;

        // End of file
        if (*in_ptr == '\0') {
            break;
        }
        // End of line
        else if (*in_ptr == ',') {
            in_ptr++;
            *(out_ptr++) = '\n';
        }

        // Skip whitespace
        while (isspace(*in_ptr)) {
            in_ptr++;
        }
    }

    *out_ptr = '\0';

    if (*in_ptr == ';') {
        in_ptr++;
    }

    db->field_count = countFields(db->data + db->line_indices[0]);

    db->_record_count = line_index;

    return in_ptr;
}

/**
 * @brief Creates an empty table from a string of headers
 *
 * @param db
 * @param headers "col1,col2" etc.
 */
void csvMem_fromHeaders (struct DB *db, const char *headers) {
    db->vfs = VFS_CSV_MEM;

    db->data = malloc(strlen(headers) + 1);

    strcpy(db->data, headers);

    // prepareHeaders() copies pointer to db->fields and adjusts db->data to be
    // after the headers.
    prepareHeaders(db);

    db->line_indices = malloc(sizeof(*db->line_indices) * 32);

    db->line_indices[0] = 0;

    db->_record_count = 0;
}

static int countFields (const char *ptr) {
    int count = 1;

    // Note: abritrary line limit
    while (*ptr != '\0') {
        if (*ptr == '\n'){
            return count;
        }

        if (*ptr == ','){
            count++;
        }

        ptr++;
    }

    return count;
}

int csvMem_insertRow (struct DB *db, const char *row) {
    int len = strlen(row);
    char *data_end = get_end_of_data(db);
    int data_len = data_end - db->data;
    int header_len = db->data - db->fields;

    int new_size = header_len + data_len + len;

    db->fields = realloc(db->fields, new_size);

    if (db->fields == NULL) {
        fprintf(stderr, "Unable to allocate %d for csv_insertRow\n", new_size);
        exit(-1);
    }

    db->data = db->fields + header_len;

    // After realloc data_end might have changed
    data_end = get_end_of_data(db);

    strcpy(data_end, row);
    data_end[len] = '\n';

    db->_record_count++;

    // IF THERE'S A BUG CHECK HERE!
    // We're not checking if db->line_inidices is big enough
    db->line_indices[db->_record_count] = data_len + len + 1;

    return 0;
}

static char * get_end_of_data (struct DB *db) {
    if (db->_record_count == -1) {
        indexLines(db);
    }

    return db->data + db->line_indices[db->_record_count];
}